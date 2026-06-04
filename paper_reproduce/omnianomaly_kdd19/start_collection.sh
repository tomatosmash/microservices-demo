#!/bin/bash
# ================================================================
# start_collection.sh — 一键启动 Prometheus 端口转发 + K8s 指标采集
#
# 用法:
#   ./start_collection.sh                    # 默认: 持续采集直到 Ctrl+C
#   ./start_collection.sh --hours 24         # 采集 24 小时后自动停止
#   ./start_collection.sh --hours 48 --interval 10  # 每10秒采样，持续48小时
#   ./start_collection.sh --mode backfill --hours 6  # 回填过去6小时
# ================================================================

set -e

# ---- 可配置参数 ----
PROMETHEUS_NS="${PROMETHEUS_NS:-monitoring}"
PROMETHEUS_SVC="${PROMETHEUS_SVC:-prometheus-operated}"
PROMETHEUS_PORT="${PROMETHEUS_PORT:-9090}"
LOCAL_PORT="${LOCAL_PORT:-9090}"
DURATION_HOURS="${DURATION_HOURS:-}"
INTERVAL="${INTERVAL:-15}"
OUTPUT_FILE="${OUTPUT_FILE:-k8s_metrics.csv}"

# ---- 颜色输出 ----
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ---- 获取脚本所在目录 ----
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DATA_DIR="${SCRIPT_DIR}/boutique__data"

# ---- 1. 检查前置条件 ----
info "========== 检查环境 =========="

# 检查 minikube
MINIKUBE=$(which minikube 2>/dev/null || echo "/d/Minikube/minikube")
if ! "$MINIKUBE" status &>/dev/null; then
    error "minikube 未运行，请先启动: minikube start"
    exit 1
fi
info "minikube: 运行中"

# 检查 kubectl
KUBECTL=$(which kubectl 2>/dev/null || echo "/c/Program Files/Docker/Docker/resources/bin/kubectl")
if ! "$KUBECTL" cluster-info &>/dev/null; then
    error "kubectl 无法连接集群"
    exit 1
fi
info "kubectl: 可用"

# 检查 Prometheus Pod
if ! "$KUBECTL" get pod -n "$PROMETHEUS_NS" -l app.kubernetes.io/name=prometheus -o name 2>/dev/null | grep -q .; then
    # 尝试 kube-prometheus-stack 的标签
    if ! "$KUBECTL" get pod -n "$PROMETHEUS_NS" -l app=kube-prometheus-stack-prometheus -o name 2>/dev/null | grep -q .; then
        # 尝试其他常见标签
        if ! "$KUBECTL" get pod -n "$PROMETHEUS_NS" | grep -q prometheus; then
            error "Prometheus Pod 未找到 (namespace: $PROMETHEUS_NS)"
            error "请确认 Prometheus 已部署: helm list -n $PROMETHEUS_NS"
            exit 1
        fi
    fi
fi
info "Prometheus Pod: 存在"

# 检查 Python
PYTHON=$(which python || which python3)
if [ -z "$PYTHON" ]; then
    error "Python 未找到"
    exit 1
fi
info "Python: $PYTHON"

# ---- 2. 检查是否已有端口转发 ----
info ""
info "========== 设置端口转发 =========="

if curl -s http://localhost:${LOCAL_PORT}/api/v1/status/runtimeinfo &>/dev/null; then
    info "Prometheus 端口 ${LOCAL_PORT} 已可访问，跳过转发"
else
    info "启动端口转发: ${PROMETHEUS_SVC}:${LOCAL_PORT} (namespace: ${PROMETHEUS_NS})"

    # 先杀掉可能存在的旧转发
    pkill -f "port-forward.*${PROMETHEUS_SVC}.*${LOCAL_PORT}" 2>/dev/null || true
    sleep 1

    # 启动新转发
    "$KUBECTL" port-forward -n "$PROMETHEUS_NS" "svc/${PROMETHEUS_SVC}" ${LOCAL_PORT}:${PROMETHEUS_PORT} &
    PF_PID=$!

    # 等待转发建立
    for i in $(seq 1 10); do
        sleep 1
        if curl -s http://localhost:${LOCAL_PORT}/api/v1/status/runtimeinfo &>/dev/null; then
            info "端口转发已建立 (PID: $PF_PID)"
            break
        fi
        if [ $i -eq 10 ]; then
            error "端口转发超时"
            exit 1
        fi
    done
fi

# ---- 3. 启动数据采集 ----
info ""
info "========== 启动数据采集 =========="
info "输出文件: ${DATA_DIR}/${OUTPUT_FILE}"

cd "$DATA_DIR"

# 构建采集命令
COLLECT_ARGS="--mode live --interval ${INTERVAL} --output ${OUTPUT_FILE}"
if [ -n "$DURATION_HOURS" ]; then
    DURATION_MIN=$((DURATION_HOURS * 60))
    COLLECT_ARGS="${COLLECT_ARGS} --duration ${DURATION_MIN}"
    info "模式: 实时采集, 持续 ${DURATION_HOURS} 小时, 间隔 ${INTERVAL}s"
else
    info "模式: 实时采集, 间隔 ${INTERVAL}s, 按 Ctrl+C 停止"
fi

info ""
info "=============================================="
info "  采集进行中..."
info "  按 Ctrl+C 安全停止 (数据已增量保存)"
info "=============================================="
echo ""

# 开始采集
$PYTHON collect_metrics.py $COLLECT_ARGS
EXIT_CODE=$?

# ---- 4. 完成 ----
info ""
if [ $EXIT_CODE -eq 0 ] || [ $EXIT_CODE -eq 130 ]; then
    info "========== 采集结束 =========="

    # 显示采集结果
    if [ -f "$OUTPUT_FILE" ]; then
        ROWS=$(wc -l < "$OUTPUT_FILE")
        SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
        info "输出文件: ${DATA_DIR}/${OUTPUT_FILE}"
        info "数据行数: $((ROWS - 1))"
        info "文件大小: ${SIZE}"

        META="${OUTPUT_FILE%.csv}_metadata.json"
        if [ -f "$META" ]; then
            info "元数据:   ${DATA_DIR}/${META}"
        fi
    fi

    info ""
    info "下一步: 运行预处理"
    info "  cd ${SCRIPT_DIR}"
    info "  python boutique_preprocess.py --csv boutique__data/${OUTPUT_FILE}"
else
    error "采集异常退出 (code: $EXIT_CODE)"
fi

# 清理端口转发（仅清理本次启动的）
if [ -n "$PF_PID" ]; then
    info "关闭端口转发 (PID: $PF_PID)..."
    kill $PF_PID 2>/dev/null || true
fi

exit $EXIT_CODE
