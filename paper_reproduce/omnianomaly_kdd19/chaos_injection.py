#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Online Boutique 混沌故障注入与标签记录脚本 (改进版)

每个故障都精确指定目标 Pod，并记录:
  1. 故障起止时间 → 生成时间维度的异常标签
  2. 受影响的 Pod → 可用于根因分析

用法:
  python chaos_injection.py                           # 完整计划
  python chaos_injection.py --types cpu_stress,memory_stress  # 只注入部分
  python chaos_injection.py --dry-run                    # 干跑
  python chaos_injection.py --generate-labels --csv data.csv  # 从事件生成标签
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np
import pandas as pd

# ============================================================
# 配置
# ============================================================
CHAOS_NAMESPACE = "chaos-testing"
BOUTIQUE_NAMESPACE = "boutique"
KUBECTL = "kubectl"

_shutdown = False


def on_shutdown(signum, frame):
    global _shutdown
    if _shutdown:
        print("\n强制退出...")
        sys.exit(1)
    _shutdown = True
    print("\n\n收到停止信号，正在清理所有故障...")


# ============================================================
# 故障定义 — 每种故障精确指定目标 Pod
# ============================================================

FAULT_PLAN = [
    # ---- CPU 压力: 让特定服务 CPU 飙升 ----
    {
        "id": "cpu_cartservice",
        "name": "CPU压力-cartservice",
        "desc": "对 cartservice 注入 1 核 CPU 压力",
        "type": "stresschaos",
        "selector": {"app": "cartservice"},
        "stressors": {"cpu": {"workers": 1, "load": 100}},
        "duration": "60s",
        "affected_pods": ["cartservice"],
        "interval": 300,
    },
    {
        "id": "cpu_currencyservice",
        "name": "CPU压力-currencyservice",
        "desc": "对 currencyservice 注入 1 核 CPU 压力（影响 QPS 最高服务）",
        "type": "stresschaos",
        "selector": {"app": "currencyservice"},
        "stressors": {"cpu": {"workers": 1, "load": 100}},
        "duration": "60s",
        "affected_pods": ["currencyservice"],
        "interval": 300,
    },
    {
        "id": "cpu_adservice",
        "name": "CPU压力-adservice",
        "desc": "对 adservice 注入 1 核 CPU 压力",
        "type": "stresschaos",
        "selector": {"app": "adservice"},
        "stressors": {"cpu": {"workers": 1, "load": 100}},
        "duration": "60s",
        "affected_pods": ["adservice"],
        "interval": 300,
    },

    # ---- 内存压力: 让特定服务内存吃紧 ----
    {
        "id": "mem_recommendationservice",
        "name": "内存压力-recommendationservice",
        "desc": "对 recommendationservice 注入 256MB 内存压力",
        "type": "stresschaos",
        "selector": {"app": "recommendationservice"},
        "stressors": {"memory": {"workers": 1, "size": "256MB"}},
        "duration": "60s",
        "affected_pods": ["recommendationservice"],
        "interval": 300,
    },
    {
        "id": "mem_paymentservice",
        "name": "内存压力-paymentservice",
        "desc": "对 paymentservice 注入 256MB 内存压力",
        "type": "stresschaos",
        "selector": {"app": "paymentservice"},
        "stressors": {"memory": {"workers": 1, "size": "256MB"}},
        "duration": "60s",
        "affected_pods": ["paymentservice"],
        "interval": 300,
    },

    # ---- Pod Kill: 杀死特定服务 Pod（K8s 会自动重建）----
    {
        "id": "kill_checkoutservice",
        "name": "PodKill-checkoutservice",
        "desc": "杀死 checkoutservice Pod（结账流程中断）",
        "type": "podchaos",
        "action": "pod-kill",
        "selector": {"app": "checkoutservice"},
        "duration": "0s",  # pod-kill 是即时动作
        "affected_pods": ["checkoutservice"],
        "interval": 300,
    },
    {
        "id": "kill_frontend",
        "name": "PodKill-frontend",
        "desc": "杀死 frontend Pod（用户无法访问网站）",
        "type": "podchaos",
        "action": "pod-kill",
        "selector": {"app": "frontend"},
        "duration": "0s",
        "affected_pods": ["frontend"],
        "interval": 300,
    },
    {
        "id": "kill_productcatalogservice",
        "name": "PodKill-productcatalogservice",
        "desc": "杀死 productcatalogservice Pod（无法浏览商品）",
        "type": "podchaos",
        "action": "pod-kill",
        "selector": {"app": "productcatalogservice"},
        "duration": "0s",
        "affected_pods": ["productcatalogservice"],
        "interval": 300,
    },

    # ---- Pod Failure: 让 Pod 不可用（不杀死，不重建）----
    {
        "id": "fail_shippingservice",
        "name": "PodFailure-shippingservice",
        "desc": "让 shippingservice Pod 不可用 40 秒（发货功能中断）",
        "type": "podchaos",
        "action": "pod-failure",
        "selector": {"app": "shippingservice"},
        "duration": "40s",
        "affected_pods": ["shippingservice"],
        "interval": 300,
    },
    {
        "id": "fail_emailservice",
        "name": "PodFailure-emailservice",
        "desc": "让 emailservice Pod 不可用 40 秒（邮件通知中断）",
        "type": "podchaos",
        "action": "pod-failure",
        "selector": {"app": "emailservice"},
        "duration": "40s",
        "affected_pods": ["emailservice"],
        "interval": 300,
    },
]


# ============================================================
# 标签记录器 — 记录时间 + 受影响 Pod
# ============================================================

class LabelRecorder:
    """记录故障开始/结束时间和受影响的 Pod"""

    def __init__(self, output_dir="."):
        self.output_dir = output_dir
        self.events = []

    def record(self, fault_id, fault_name, affected_pods, action, start_dt, end_dt=None):
        """记录一条故障事件"""
        self.events.append({
            "fault_id": fault_id,
            "fault_name": fault_name,
            "affected_pods": affected_pods,
            "action": action,
            "start": start_dt.isoformat(),
            "start_unix": start_dt.timestamp(),
            "end": end_dt.isoformat() if end_dt else None,
            "end_unix": end_dt.timestamp() if end_dt else None,
        })

    def save_events(self):
        """保存事件到 JSON"""
        path = os.path.join(self.output_dir, "chaos_events.json")
        with open(path, 'w') as f:
            json.dump(self.events, f, indent=2, ensure_ascii=False)
        print(f"\n故障事件: {path}")

    def generate_labels(self, csv_path, output_path=None):
        """
        从 CSV 数据的时间戳和 chaos_events 生成异常标签。

        支持两种粒度的标签:
          1. 时间级: 整个时间点标记为异常 (anomaly_labels.csv)
          2. Pod 级: 每个 Pod 单独标记 (anomaly_labels_per_pod.csv)
        """
        # 读取所有时间戳
        df = pd.read_csv(csv_path)
        timestamps = sorted(df["timestamp"].unique())

        # 时间级标签: 任一时间点只要有任何故障发生 = 异常
        time_label = np.zeros(len(timestamps), dtype=np.int32)
        for e in self.events:
            if e["end_unix"] is None:
                continue
            mask = (timestamps >= e["start_unix"]) & (timestamps <= e["end_unix"])
            time_label[mask] = 1

        # Pod 级标签: 每个 Pod 单独标注
        all_pods = sorted(df["pod"].unique())
        pod_labels = pd.DataFrame({"timestamp": timestamps})
        for pod in all_pods:
            pod_labels[pod] = 0

        for e in self.events:
            if e["end_unix"] is None:
                continue
            mask = (pod_labels["timestamp"] >= e["start_unix"]) & \
                   (pod_labels["timestamp"] <= e["end_unix"])
            for affected_pod in e["affected_pods"]:
                # 找到完整的 Pod 名称（因为 affected_pods 存的是 app 名称，Pod 全名更长）
                matching = [p for p in all_pods if p.startswith(affected_pod)]
                for m in matching:
                    pod_labels.loc[mask, m] = 1

        # 保存时间级标签
        if output_path is None:
            output_path = os.path.join(self.output_dir, "anomaly_labels.csv")
        time_label_df = pd.DataFrame({
            "timestamp": timestamps,
            "is_anomaly": time_label
        })
        time_label_df.to_csv(output_path, index=False)

        # 保存 Pod 级标签
        pod_path = output_path.replace(".csv", "_per_pod.csv")
        pod_labels.to_csv(pod_path, index=False)

        anomaly_ratio = time_label.sum() / len(time_label) * 100
        print(f"时间级标签: {output_path}")
        print(f"  总时间点: {len(time_label)}, 异常: {time_label.sum()} ({anomaly_ratio:.2f}%)")
        print(f"Pod 级标签: {pod_path}")
        print(f"  列: {len(all_pods)} 个 Pod")

        return time_label_df, pod_labels

    def print_summary(self):
        """打印总结"""
        print(f"\n{'='*65}")
        print(f"故障注入总结")
        print(f"{'='*65}")
        print(f"{'故障':<28s} {'受影响Pod':<22s} {'持续':>8s}")
        print(f"{'-'*28} {'-'*22} {'-'*8}")
        total = 0
        for e in self.events:
            dur = ""
            if e["end_unix"] and e["start_unix"]:
                dur_s = e["end_unix"] - e["start_unix"]
                total += dur_s
                dur = f"{dur_s:.0f}s"
            print(f"{e['fault_name']:<28s} {', '.join(e['affected_pods']):<22s} {dur:>8s}")
        print(f"{'-'*59}")
        print(f"共 {len(self.events)} 次故障, 总异常时长 {total:.0f}s ({total/60:.1f}分钟)")
        print(f"{'='*65}")


# ============================================================
# Kubectl 操作
# ============================================================

def kubectl_apply(yaml_str, name):
    """应用 YAML"""
    result = subprocess.run(
        [KUBECTL, "apply", "-f", "-"],
        input=yaml_str, text=True, capture_output=True, timeout=30
    )
    if result.returncode != 0:
        print(f"  [ERROR] apply {name}: {result.stderr.strip()}")
        return False
    return True


def kubectl_delete(yaml_str, name):
    """删除资源"""
    result = subprocess.run(
        [KUBECTL, "delete", "-f", "-"],
        input=yaml_str, text=True, capture_output=True, timeout=30
    )
    # 删除失败不是致命错误
    return True


def cleanup_all():
    """清理所有故障"""
    print("\n清理所有混沌实验...")
    for kind in ["podchaos", "stresschaos", "networkchaos", "iochaos"]:
        subprocess.run(
            [KUBECTL, "delete", kind, "--all", "-n", CHAOS_NAMESPACE],
            capture_output=True, timeout=10
        )
    print("清理完成")


# ============================================================
# 生成 Chaos Mesh YAML
# ============================================================

def make_yaml(fault_def):
    """根据故障定义生成 Chaos Mesh YAML"""
    ftype = fault_def["type"]
    fid = fault_def["id"]
    selector = fault_def["selector"]
    duration = fault_def["duration"]
    sel_labels = "\n".join(f"      {k}: {v}" for k, v in selector.items())

    if ftype == "stresschaos":
        stressors = fault_def["stressors"]
        parts = []
        for stress_type, config in stressors.items():
            parts.append(f"    {stress_type}:")
            for k, v in config.items():
                parts.append(f"      {k}: {v}")
        stress_yaml = "\n".join(parts)
        return f"""apiVersion: chaos-mesh.org/v1alpha1
kind: StressChaos
metadata:
  name: bout-{fid}
  namespace: {CHAOS_NAMESPACE}
spec:
  mode: one
  selector:
    namespaces:
      - {BOUTIQUE_NAMESPACE}
    labelSelectors:
{sel_labels}
  stressors:
{stress_yaml}
  duration: "{duration}"
"""

    elif ftype == "podchaos":
        action = fault_def["action"]
        dur_line = f'  duration: "{duration}"' if duration != "0s" else ""
        return f"""apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: bout-{fid}
  namespace: {CHAOS_NAMESPACE}
spec:
  action: {action}
  mode: one
  selector:
    namespaces:
      - {BOUTIQUE_NAMESPACE}
    labelSelectors:
{sel_labels}
{dur_line}
"""

    return None


# ============================================================
# 主执行逻辑
# ============================================================

def run_chaos_plan(fault_ids, recorder, dry_run=False):
    global _shutdown

    # 筛选要执行的故障
    plan = [f for f in FAULT_PLAN if not fault_ids or f["id"] in fault_ids]

    print(f"\n{'='*65}")
    print(f"Online Boutique 混沌故障注入计划")
    print(f"{'='*65}")
    for i, f in enumerate(plan, 1):
        print(f"  {i}. [{f['name']}] {f['desc']}")
        print(f"     类型={f['type']}, 目标={f['selector']}, "
              f"持续={f.get('duration','即时')}, 间隔={f['interval']}s")
    print(f"  共 {len(plan)} 种故障")
    if dry_run:
        print(f"  模式: DRY RUN (不实际注入)")
    print(f"{'='*65}")

    if dry_run:
        return

    # 确认
    print("\n即将开始故障注入! 请确保数据采集已在运行:")
    print("  python boutique__data/collect_metrics.py --mode live --interval 15")
    resp = input("\n是否继续? (y/N): ")
    if resp.lower() != 'y':
        print("已取消")
        return

    signal.signal(signal.SIGINT, on_shutdown)
    signal.signal(signal.SIGTERM, on_shutdown)

    print(f"\n实验开始: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("按 Ctrl+C 安全停止\n")

    try:
        round_num = 0
        while not _shutdown:
            round_num += 1
            for fault_def in plan:
                if _shutdown:
                    break

                fid = fault_def["id"]
                fname = fault_def["name"]
                interval = fault_def["interval"]

                # ---- 正常间隔 ----
                print(f"\n[{round_num}] 正常间隔 {interval}s ...", end="", flush=True)
                for i in range(interval):
                    if _shutdown:
                        break
                    if i % 60 == 0 and i > 0:
                        print(f" {i}s", end="", flush=True)
                    time.sleep(1)
                print()

                if _shutdown:
                    break

                # ---- 注入故障 ----
                print(f"[{round_num}] ▶ 注入: {fname}")
                print(f"          目标: {fault_def['selector']}, "
                      f"影响Pod: {fault_def['affected_pods']}")

                yaml_str = make_yaml(fault_def)
                if yaml_str is None:
                    print(f"  [ERROR] 无法生成 YAML")
                    continue

                start_dt = datetime.now(timezone.utc)
                success = kubectl_apply(yaml_str, fid)

                if not success:
                    print(f"  [WARN] 注入失败，跳过")
                    continue

                # 确定等待时间
                dur_str = fault_def.get("duration", "0s")
                if dur_str == "0s":
                    wait_sec = 15  # pod-kill 即时生效，等 Pod 重建
                else:
                    wait_sec = int(dur_str.replace("s", ""))

                # 等待故障持续
                print(f"          持续 {wait_sec}s ...", end="", flush=True)
                for i in range(wait_sec):
                    if _shutdown:
                        break
                    if i % 30 == 0 and i > 0:
                        print(f" {i}s", end="", flush=True)
                    time.sleep(1)
                print()

                # 清理故障
                kubectl_delete(yaml_str, fid)
                end_dt = datetime.now(timezone.utc)

                # 记录事件
                recorder.record(fid, fname, fault_def["affected_pods"],
                                fault_def.get("action", fault_def["type"]),
                                start_dt, end_dt)

                print(f"          ✓ 完成 ({fault_def['affected_pods']})")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n实验终止，清理...")
        cleanup_all()
        recorder.save_events()
        recorder.print_summary()


def generate_labels_from_csv(csv_path, events_path, output_dir):
    """从已有事件生成标签"""
    with open(events_path, 'r') as f:
        events = json.load(f)

    recorder = LabelRecorder(output_dir)
    recorder.events = events
    recorder.generate_labels(csv_path)
    recorder.print_summary()


def main():
    parser = argparse.ArgumentParser(
        description='Online Boutique 混沌故障注入 (精确Pod定位版)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python chaos_injection.py                              # 执行全部 10 种故障
  python chaos_injection.py --types cpu_cartservice,kill_frontend  # 指定故障
  python chaos_injection.py --dry-run                       # 干跑查看计划
  python chaos_injection.py --generate-labels --csv data.csv  # 生成标签
        """
    )
    parser.add_argument('--types', nargs='*', default=None,
                        help='要注入的故障ID (默认: 全部)')
    parser.add_argument('--dry-run', action='store_true', help='干跑模式')
    parser.add_argument('--output-dir', default='.', help='输出目录')
    parser.add_argument('--generate-labels', action='store_true',
                        help='从已有事件生成标签 (不注入新故障)')
    parser.add_argument('--csv', help='CSV 文件路径 (配合 --generate-labels)')
    parser.add_argument('--events', default='chaos_events.json',
                        help='事件文件路径')
    args = parser.parse_args()

    if args.generate_labels:
        csv_path = args.csv or "boutique__data/k8s_metrics.csv"
        if not os.path.exists(csv_path):
            print(f"错误: CSV 文件不存在: {csv_path}")
            return 1
        if not os.path.exists(args.events):
            print(f"错误: 事件文件不存在: {args.events}")
            return 1
        generate_labels_from_csv(csv_path, args.events, args.output_dir)
        return 0

    recorder = LabelRecorder(output_dir=args.output_dir)
    run_chaos_plan(args.types, recorder, dry_run=args.dry_run)
    return 0


if __name__ == '__main__':
    sys.exit(main())
