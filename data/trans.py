import json
import pandas as pd
import os

files = [f for f in os.listdir() if f.endswith(".json")]

all_data = []

for f in files:
    with open(f) as file:
        data = json.load(file)

    metric_name = f.replace(".json","")

    for result in data["data"]["result"]:
        for ts, value in result["values"]:
            all_data.append([metric_name, ts, float(value)])

df = pd.DataFrame(all_data, columns=["metric","timestamp","value"])
df.to_csv("k8s_metrics.csv", index=False)

print(df.head())