import pandas as pd

# 读取 CSV
df = pd.read_csv("k8s_metrics.csv")

# 1. 查看前几行数据
print("前五行数据:")
print(df.head())

# 2. 每个指标数量统计
metric_counts = df["metric"].value_counts()
print("\n每个指标的数据量:")
print(metric_counts)

# 3. Pod 总数
num_pods = df["pod"].nunique()
print("\nPod 总数:", num_pods)

# 4. Pod 列表
pod_list = df["pod"].unique()
print("\nPod 列表:")
print(pod_list)