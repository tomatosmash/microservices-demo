# -*- coding: utf-8 -*-
"""
Online Boutique K8s 指标采集脚本（改进版）

支持两种模式:
  - backfill: 回填采集过去 N 小时的历史数据
  - live:     持续采集，每隔 N 秒抓取一次，直到手动停止 (Ctrl+C)

用法:
  # 回填过去 24 小时的数据
  python collect_metrics.py --mode backfill --hours 24

  # 持续采集，每 15 秒一次，运行直到 Ctrl+C
  python collect_metrics.py --mode live --interval 15

  # 持续采集 2 小时，每 10 秒一次，指定输出文件
  python collect_metrics.py --mode live --duration 120 --interval 10 --output my_data.csv
"""

import argparse
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# 配置
# ============================================================

PROM_URL = os.environ.get('PROMETHEUS_URL', 'http://localhost:9090')

# 指标定义: {name: (query_string, 是否可选)}
# 注意: 此 Prometheus 中 kubelet/cAdvisor 指标没有 container 标签，
# 用 pod/namespace/id 区分。网络容器级指标不可用，改用节点级。
METRICS = {
    # --- CPU ---
    "cpu_rate": (
        "rate(container_cpu_usage_seconds_total[2m])",
        False
    ),

    # --- 内存 ---
    "memory_working_set": (
        "container_memory_working_set_bytes",
        False
    ),
    "memory_rss": (
        "container_memory_rss",
        True
    ),

    # --- 磁盘 I/O ---
    "fs_reads_bytes_rate": (
        "rate(container_fs_reads_bytes_total[2m])",
        True
    ),
    "fs_writes_bytes_rate": (
        "rate(container_fs_writes_bytes_total[2m])",
        True
    ),
    "fs_reads_total": (
        "container_fs_reads_total",
        True
    ),
    "fs_writes_total": (
        "container_fs_writes_total",
        True
    ),

    # --- Pod 状态 (来自 kube-state-metrics) ---
    "pod_status_phase": (
        "kube_pod_status_phase",
        False
    ),
    "pod_restarts": (
        "kube_pod_container_status_restarts_total",
        False
    ),
}

# 全局停止标志（用于优雅退出）
_shutdown = False


def setup_logging():
    """配置日志格式"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S'
    )
    return logging.getLogger(__name__)


def create_session(retries=3, backoff=1.0):
    """创建带重试的 HTTP 会话"""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def on_shutdown(signum, frame):
    """Ctrl+C 或 SIGTERM 处理器"""
    global _shutdown
    if _shutdown:
        print("\n强制退出...")
        sys.exit(1)
    _shutdown = True
    print("\n收到停止信号，正在安全退出... (再按一次 Ctrl+C 强制退出)")


def query_prometheus_range(session, metric_name, query_str, start_ts, end_ts, step):
    """
    使用 query_range API 回填历史数据。

    Returns:
        list[dict]: [{metric, node, pod, namespace, container, timestamp, value}, ...]
    """
    params = {
        "query": query_str,
        "start": int(start_ts),
        "end": int(end_ts),
        "step": f"{step}s"
    }

    try:
        resp = session.get(
            f"{PROM_URL}/api/v1/query_range",
            params=params,
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "success":
            logging.warning(f"  Prometheus 返回非 success: {metric_name}")
            return []

        results = []
        for series in data["data"]["result"]:
            labels = series.get("metric", {})
            # cAdvisor 用 'id' (cgroup路径), kube-state-metrics 用 'container'
            container = labels.get("container", "") or labels.get("id", "")
            # kube_pod_status_phase: 把 phase 拼到指标名以区分不同状态
            full_metric = metric_name
            if metric_name == "pod_status_phase":
                phase = labels.get("phase", "")
                full_metric = f"pod_status_phase_{phase}"
            for ts, value in series["values"]:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    continue
                results.append({
                    "metric": full_metric,
                    "node": labels.get("node", ""),
                    "pod": labels.get("pod", ""),
                    "namespace": labels.get("namespace", ""),
                    "container": container,
                    "timestamp": int(ts),
                    "value": value
                })
        return results

    except requests.exceptions.RequestException as e:
        logging.error(f"  请求失败 [{metric_name}]: {e}")
        return []


def query_prometheus_instant(session, metric_name, query_str):
    """
    使用 query API 获取当前瞬时值（live 模式）。

    Returns:
        list[dict]: [{metric, node, pod, namespace, container, timestamp, value}, ...]
    """
    params = {"query": query_str}
    ts = int(time.time())

    try:
        resp = session.get(
            f"{PROM_URL}/api/v1/query",
            params=params,
            timeout=15
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "success":
            logging.warning(f"  Prometheus 返回非 success: {metric_name}")
            return []

        results = []
        for series in data["data"]["result"]:
            labels = series.get("metric", {})
            # cAdvisor 用 'id' (cgroup路径), kube-state-metrics 用 'container'
            container = labels.get("container", "") or labels.get("id", "")
            # kube_pod_status_phase: 把 phase 拼到指标名以区分不同状态
            full_metric = metric_name
            if metric_name == "pod_status_phase":
                phase = labels.get("phase", "")
                full_metric = f"pod_status_phase_{phase}"
            value = series["value"][1]  # [timestamp, value]
            try:
                value = float(value)
            except (ValueError, TypeError):
                continue
            results.append({
                "metric": full_metric,
                "node": labels.get("node", ""),
                "pod": labels.get("pod", ""),
                "namespace": labels.get("namespace", ""),
                "container": container,
                "timestamp": ts,
                "value": value
            })
        return results

    except requests.exceptions.RequestException as e:
        logging.error(f"  请求失败 [{metric_name}]: {e}")
        return []


def fetch_all_metrics(session, mode, metric_filter, **kwargs):
    """
    获取所有指标的数据（range 或 instant 模式）。

    Args:
        session: HTTP session
        mode: 'range' 或 'instant'
        metric_filter: None（全部）或指标名列表
        **kwargs: 传给 query 函数的参数 (start_ts, end_ts, step for range)

    Returns:
        list[dict]: 标准化后的数据行
    """
    all_rows = []

    for name, (query_str, is_optional) in METRICS.items():
        if metric_filter and name not in metric_filter:
            continue

        logging.info(f"  采集 [{name}] ...")

        if mode == 'range':
            rows = query_prometheus_range(
                session, name, query_str,
                kwargs['start_ts'], kwargs['end_ts'], kwargs['step']
            )
        else:
            rows = query_prometheus_instant(session, name, query_str)

        if rows:
            all_rows.extend(rows)
            logging.info(f"    → {len(rows)} 条")
        else:
            if is_optional:
                logging.info(f"    → 0 条 (可选指标，跳过)")
            else:
                logging.warning(f"    → 0 条 (必须指标，检查 Prometheus 配置)")

    return all_rows


def save_incrementally(rows, output_path, is_first_batch):
    """增量追加数据到 CSV 文件"""
    df = pd.DataFrame(rows, columns=[
        "metric", "node", "pod", "namespace", "container", "timestamp", "value"
    ])
    df.to_csv(
        output_path,
        mode='w' if is_first_batch else 'a',
        header=is_first_batch,
        index=False
    )
    return len(df)


def save_metadata(output_path, metadata):
    """保存采集元数据到 JSON 文件"""
    import json
    meta_path = output_path.replace('.csv', '_metadata.json')
    with open(meta_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    logging.info(f"元数据已保存: {meta_path}")


def run_backfill(args, logger):
    """回填模式：采集过去 N 小时的历史数据"""
    session = create_session()

    end_ts = int(time.time())
    start_ts = end_ts - args.hours * 3600
    step = args.step

    n_samples = (end_ts - start_ts) // step
    logger.info(f"回填模式: 过去 {args.hours} 小时, 步长 {step}s")
    logger.info(f"时间范围: {datetime.fromtimestamp(start_ts)} → {datetime.fromtimestamp(end_ts)}")
    logger.info(f"预计数据点: ~{n_samples}")

    logger.info("开始采集...")
    rows = fetch_all_metrics(
        session, mode='range',
        metric_filter=args.metrics,
        start_ts=start_ts, end_ts=end_ts, step=step
    )

    if not rows:
        logger.error("未获取到任何数据! 请检查 Prometheus 连接和指标名称")
        return 1

    save_incrementally(rows, args.output, is_first_batch=True)
    logger.info(f"完成! 共 {len(rows)} 条记录 → {args.output}")

    save_metadata(args.output, {
        "mode": "backfill",
        "start_ts": start_ts,
        "end_ts": end_ts,
        "start_time": datetime.fromtimestamp(start_ts).isoformat(),
        "end_time": datetime.fromtimestamp(end_ts).isoformat(),
        "step_seconds": step,
        "prometheus_url": PROM_URL,
        "metrics": list(args.metrics) if args.metrics else list(METRICS.keys()),
        "total_rows": len(rows),
    })

    return 0


def run_live(args, logger):
    """持续采集模式：每 N 秒采集一次，直到停止"""
    global _shutdown
    session = create_session()

    # 注册信号处理器
    signal.signal(signal.SIGINT, on_shutdown)
    signal.signal(signal.SIGTERM, on_shutdown)

    logger.info(f"实时采集模式: 间隔 {args.interval}s"
                + (f", 持续时间 {args.duration}m" if args.duration else ", 直到手动停止"))

    start_time = time.time()
    n_rounds = 0
    total_rows = 0

    # 如果文件已存在，先删除（全新开始）
    if os.path.exists(args.output):
        logger.warning(f"输出文件已存在，将被覆盖: {args.output}")

    is_first = True
    while not _shutdown:
        round_start = time.time()

        rows = fetch_all_metrics(
            session, mode='instant',
            metric_filter=args.metrics,
        )

        if rows:
            save_incrementally(rows, args.output, is_first_batch=is_first)
            total_rows += len(rows)
            n_rounds += 1
            is_first = False

        elapsed = time.time() - start_time
        logger.info(f"第 {n_rounds} 轮完成, 累计 {total_rows} 条, "
                     f"已运行 {elapsed/60:.1f} 分钟")

        # 检查是否达到指定时长
        if args.duration and elapsed >= args.duration * 60:
            logger.info(f"已达到指定时长 {args.duration} 分钟，停止采集")
            break

        # 等待下一轮（扣除本轮采集耗时）
        sleep_time = args.interval - (time.time() - round_start)
        if sleep_time > 0 and not _shutdown:
            # 分段 sleep 以便快速响应 Ctrl+C
            for _ in range(int(sleep_time)):
                if _shutdown:
                    break
                time.sleep(1)

    logger.info(f"采集结束: {n_rounds} 轮, 共 {total_rows} 条")

    save_metadata(args.output, {
        "mode": "live",
        "interval_seconds": args.interval,
        "rounds": n_rounds,
        "total_rows": total_rows,
        "start_time": datetime.fromtimestamp(start_time).isoformat(),
        "end_time": datetime.now(timezone.utc).isoformat(),
        "duration_minutes": (time.time() - start_time) / 60,
        "prometheus_url": PROM_URL,
        "metrics": list(args.metrics) if args.metrics else list(METRICS.keys()),
    })

    return 0


def main():
    global PROM_URL

    parser = argparse.ArgumentParser(
        description='Online Boutique K8s 指标采集脚本 (OmniAnomaly 数据源)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python collect_metrics.py --mode backfill --hours 24
  python collect_metrics.py --mode live --interval 15 --duration 120
  python collect_metrics.py --mode live --interval 10 --metrics cpu_rate,memory_working_set
        """
    )
    parser.add_argument('--mode', choices=['backfill', 'live'], default='live',
                        help='采集模式: backfill=回填历史, live=持续采集 (默认: live)')
    parser.add_argument('--output', default='k8s_metrics.csv',
                        help='输出 CSV 文件路径 (默认: k8s_metrics.csv)')

    # backfill 模式参数
    parser.add_argument('--hours', type=float, default=24.0,
                        help='回填的小时数 (默认: 24)')
    parser.add_argument('--step', type=int, default=15,
                        help='采样步长，秒 (默认: 15)')

    # live 模式参数
    parser.add_argument('--interval', type=int, default=15,
                        help='live 模式采集间隔，秒 (默认: 15)')
    parser.add_argument('--duration', type=float, default=None,
                        help='live 模式持续时间，分钟 (默认: 无限)')

    # 通用参数
    parser.add_argument('--metrics', nargs='*', default=None,
                        help=f'指定采集的指标（默认全部）。可选: {", ".join(METRICS.keys())}')
    parser.add_argument('--prometheus-url', default=None,
                        help=f'Prometheus 地址 (默认: {PROM_URL})')

    args = parser.parse_args()

    # 设置 Prometheus URL
    if args.prometheus_url:
        PROM_URL = args.prometheus_url

    logger = setup_logging()
    logger.info(f"Prometheus: {PROM_URL}")
    logger.info(f"指标数量: {len(args.metrics) if args.metrics else len(METRICS)}")

    # 检查 Prometheus 连通性
    try:
        resp = requests.get(f"{PROM_URL}/api/v1/status/runtimeinfo", timeout=10)
        if resp.status_code != 200:
            logger.error(f"Prometheus 返回异常状态码: {resp.status_code}")
            logger.error("请确认 Prometheus 已启动且地址正确")
            return 1
        logger.info(f"Prometheus 连接正常")
    except requests.exceptions.ConnectionError:
        logger.error(f"无法连接 Prometheus: {PROM_URL}")
        logger.error("请确认: 1) Prometheus 已启动  2) 地址正确  3) 端口转发已配置")
        logger.error("如果是 minikube: kubectl port-forward -n monitoring svc/prometheus 9090:9090")
        return 1
    except requests.exceptions.RequestException as e:
        logger.warning(f"Prometheus 连通性检查异常: {e}")

    if args.mode == 'backfill':
        return run_backfill(args, logger)
    else:
        return run_live(args, logger)


if __name__ == '__main__':
    sys.exit(main())
