import requests
import pandas as pd
import time
from datetime import datetime

PROM_URL = "http://localhost:9090/api/v1/query_range"

METRICS = {
    "cpu": "rate(container_cpu_usage_seconds_total[5m])",
    "memory": "container_memory_usage_bytes",
    "pod_status": "kube_pod_status_phase",
    "restarts": "kube_pod_container_status_restarts_total"
}

end = int(time.time())
start = end - 3600
step = 15

all_rows = []

for name, query in METRICS.items():

    print(f"Collecting {name} ...")

    params = {
        "query": query,
        "start": start,
        "end": end,
        "step": step
    }

    resp = requests.get(PROM_URL, params=params)

    if resp.status_code != 200:
        print(f"HTTP error: {name}")
        continue

    data = resp.json()

    if data.get("status") != "success":
        print(f"Prometheus error: {name}")
        continue

    for series in data["data"]["result"]:
        labels = series.get("metric", {})

        for ts, value in series["values"]:

            try:
                value = float(value)
            except:
                continue

            all_rows.append([
                name,
                labels.get("node", ""),
                labels.get("pod", ""),
                labels.get("namespace", ""),
                labels.get("container", ""),
                int(ts),
                value
            ])

df = pd.DataFrame(all_rows, columns=[
    "metric", "node", "pod", "namespace", "container", "timestamp", "value"
])

df.to_csv("k8s_metrics.csv", index=False)

print("Done!")