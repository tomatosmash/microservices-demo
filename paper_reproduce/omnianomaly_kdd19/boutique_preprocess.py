# -*- coding: utf-8 -*-
"""
将 Online Boutique K8s 指标数据 (k8s_metrics.csv) 转换为 OmniAnomaly 可用格式。

输入数据格式 (长表):
    metric,node,pod,namespace,container,timestamp,value
    cpu,minikube,pod-a,boutique,,1780395480,0.00056
    memory,minikube,pod-a,boutique,,1780395480,5132288
    ...

输出格式 (宽表矩阵):
    每行 = 一个时间点, 每列 = 一个 metric|pod|namespace 维度
    保存为 processed/boutique_train.pkl 和 processed/boutique_test.pkl

用法:
    python boutique_preprocess.py
    python boutique_preprocess.py --namespace boutique      # 只保留 boutique 命名空间
    python boutique_preprocess.py --train-ratio 0.8         # 80% 训练 / 20% 测试
"""

import os
import pickle
import sys

import numpy as np
import pandas as pd


def preprocess_boutique(csv_path='boutique__data/k8s_metrics.csv',
                         output_dir='processed',
                         train_ratio=0.7,
                         filter_namespace=None):
    """
    将 K8s 指标 CSV 转换为 OmniAnomaly 训练格式。

    Args:
        csv_path: k8s_metrics.csv 文件路径
        output_dir: 输出目录，默认为 processed/
        train_ratio: 训练集比例，默认 0.7
        filter_namespace: 只保留指定命名空间的数据，None 表示全部保留

    Returns:
        n_dims: 数据集的维度数
    """

    # ============================================================
    # 1. 读取原始 CSV
    # ============================================================
    print(f'[1/5] 读取原始数据: {csv_path}')
    df = pd.read_csv(csv_path)
    print(f'      原始行数: {len(df)}')
    print(f'      指标类型: {sorted(df["metric"].unique())}')
    print(f'      命名空间:   {sorted(df["namespace"].unique())}')
    print(f'      Pod 数量:  {df["pod"].nunique()}')
    print(f'      时间戳数:  {df["timestamp"].nunique()}')

    # ============================================================
    # 2. 可选：过滤只保留某个命名空间
    # ============================================================
    if filter_namespace is not None:
        before = len(df)
        df = df[df['namespace'] == filter_namespace]
        print(f'\n      过滤 namespace="{filter_namespace}": {before} → {len(df)} 行')
        if len(df) == 0:
            raise ValueError(f'命名空间 "{filter_namespace}" 不存在于数据中! '
                             f'可用: {sorted(df["namespace"].unique())}')

    # ============================================================
    # 3. Pivot 转换: 长表 → 宽表
    #    每列 = metric + pod + namespace 的组合
    # ============================================================
    print(f'\n[2/5] Pivot 转换 (长表 → 宽表)...')

    pivot = df.pivot_table(
        index='timestamp',
        columns=['metric', 'pod', 'namespace'],
        values='value',
        aggfunc='first'   # 每个 (timestamp, metric, pod, ns) 组合只有唯一值
    )

    # 按时间戳排序（确保时间顺序）
    pivot = pivot.sort_index()

    # 将多级列名展平: ('cpu', 'pod-a', 'boutique') → 'cpu|pod-a|boutique'
    pivot.columns = [
        '|'.join(str(level) for level in col)
        for col in pivot.columns.values
    ]

    n_timestamps, n_dims = pivot.shape
    time_start = pivot.index[0]
    time_end = pivot.index[-1]
    time_span_min = (time_end - time_start) / 60

    print(f'      Pivot 后形状: {n_timestamps} 个时间点 × {n_dims} 个维度')
    print(f'      时间跨度: {time_span_min:.1f} 分钟 ({time_span_min/60:.1f} 小时)')

    # 打印各类型指标的维度数
    prefix_counts = {}
    for col in pivot.columns:
        prefix = col.split('|')[0]
        prefix_counts[prefix] = prefix_counts.get(prefix, 0) + 1
    for k, v in sorted(prefix_counts.items()):
        print(f'        - {k}: {v} 个维度')

    # ============================================================
    # 4. 处理缺失值
    # ============================================================
    print(f'\n[3/5] 处理缺失值...')
    total_cells = pivot.size
    missing = pivot.isna().sum().sum()
    if missing > 0:
        print(f'      缺失值: {missing}/{total_cells} ({100*missing/total_cells:.2f}%)')
        print(f'      策略: 前向填充 + 剩余填 0')
        pivot = pivot.ffill().fillna(0)
    else:
        print(f'      无缺失值 ✓')

    # 转为 numpy float32 矩阵（与 data_preprocess.py 一致）
    data = pivot.values.astype(np.float32)
    column_names = list(pivot.columns)

    # 检查数据有效性
    if np.any(np.isnan(data)):
        print(f'      警告: 仍存在 NaN，替换为 0')
        data = np.nan_to_num(data)

    print(f'      最终数据形状: {data.shape}')
    print(f'      值范围: [{np.min(data):.4f}, {np.max(data):.4f}]')

    # ============================================================
    # 5. 划分训练集 / 测试集并保存
    # ============================================================
    print(f'\n[4/5] 划分训练集/测试集 (train_ratio={train_ratio})...')

    split_idx = int(n_timestamps * train_ratio)

    # 确保 split_idx > window_length，否则训练无法进行
    min_window = 30  # 略小于默认的 window_length=100
    if split_idx < min_window:
        print(f'      警告: 训练集只有 {split_idx} 个点，小于建议最小值 {min_window}')
        print(f'      建议: 使用 --train-ratio 调大训练比例，或采集更多数据')

    train_data = data[:split_idx]
    test_data = data[split_idx:]

    print(f'      训练集: {train_data.shape}')
    print(f'      测试集: {test_data.shape}')

    # ============================================================
    # 6. 保存为 pkl 文件（格式与 data_preprocess.py 输出一致）
    # ============================================================
    print(f'\n[5/5] 保存预处理结果到 {output_dir}/ ...')

    os.makedirs(output_dir, exist_ok=True)

    dataset_name = 'boutique'

    # 训练数据
    train_path = os.path.join(output_dir, f'{dataset_name}_train.pkl')
    with open(train_path, 'wb') as f:
        pickle.dump(train_data, f)
    print(f'      ✓ {train_path}')

    # 测试数据
    test_path = os.path.join(output_dir, f'{dataset_name}_test.pkl')
    with open(test_path, 'wb') as f:
        pickle.dump(test_data, f)
    print(f'      ✓ {test_path}')

    # 列名参考（调试用，不影响训练）
    col_path = os.path.join(output_dir, f'{dataset_name}_columns.pkl')
    with open(col_path, 'wb') as f:
        pickle.dump(column_names, f)
    print(f'      ✓ {col_path} (列名参考)')

    # 维度数（供 get_data_dim() 读取）
    dim_path = os.path.join(output_dir, f'{dataset_name}_dim.txt')
    with open(dim_path, 'w') as f:
        f.write(str(n_dims))
    print(f'      ✓ {dim_path} (维度数: {n_dims})')

    # ============================================================
    # 完成
    # ============================================================
    print(f'\n{"="*60}')
    print(f'预处理完成!')
    print(f'  数据集名称: {dataset_name}')
    print(f'  维度数:     {n_dims}')
    print(f'  训练集:     {train_data.shape[0]} 个时间点')
    print(f'  测试集:     {test_data.shape[0]} 个时间点')
    print(f'  时间跨度:   {time_span_min:.1f} 分钟')
    print(f'{"="*60}')
    print(f'\n下一步:')
    print(f'  1. 修改 main.py 中 ExpConfig 的 dataset = "{dataset_name}"')
    print(f'  2. 调整 window_length (建议 ≤ {min(split_idx, n_timestamps - split_idx) // 2})')
    print(f'  3. 运行: python main.py')

    return n_dims


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='预处理 Online Boutique K8s 指标数据，输出 OmniAnomaly 兼容格式'
    )
    parser.add_argument('--csv', default='boutique__data/k8s_metrics.csv',
                        help='k8s_metrics.csv 文件路径 (默认: boutique__data/k8s_metrics.csv)')
    parser.add_argument('--output', default='processed',
                        help='输出目录 (默认: processed)')
    parser.add_argument('--train-ratio', type=float, default=0.7,
                        help='训练集比例 (默认: 0.7)')
    parser.add_argument('--namespace', default=None,
                        help='只保留指定命名空间的数据，如 --namespace boutique (默认: 全部保留)')
    args = parser.parse_args()

    preprocess_boutique(
        csv_path=args.csv,
        output_dir=args.output,
        train_ratio=args.train_ratio,
        filter_namespace=args.namespace
    )
