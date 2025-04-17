import pandas as pd
import re

# Input file downloaded from HDFS
input_file = "directory_sizes.csv"

# Define team keywords
teams = ["ib", "eq", "ld", "bm"]
subdirs = ["model", "feed", "alert", "process"]

# Function to convert human-readable size to bytes
def size_to_bytes(size_str):
    try:
        num, unit = size_str.strip().split()
        num = float(num)
        unit = unit.upper()
        factor = {
            "B": 1,
            "KB": 10**3,
            "MB": 10**6,
            "GB": 10**9,
            "TB": 10**12,
        }
        return int(num * factor[unit])
    except:
        return 0

# Read the CSV file
df = pd.read_csv(input_file)

# Add columns for subdir and team
df["base_path"] = df["path"].apply(lambda x: "/".join(x.split("/")[:6]))
df["team"] = df["path"].apply(lambda x: next((t for t in teams if re.search(rf"\b{t}\b", x)), "others"))
df["bytes"] = df["size"].apply(size_to_bytes)

# Pivot table
pivot = df.pivot_table(index="base_path", columns="team", values="bytes", aggfunc="sum", fill_value=0)

# Format bytes into human readable
def human_readable(n):
    if n == 0: return ""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if n < 1000:
            return f"{n:.1f} {unit}"
        n /= 1000
    return f"{n:.1f} EB"

pivot = pivot.applymap(human_readable)

# Add all expected paths
for subdir in subdirs:
    full_path = f"/project/abcd/efgh/ijkl/{subdir}"
    if full_path not in pivot.index:
        pivot.loc[full_path] = [""] * len(pivot.columns)

# Reorder
pivot = pivot.sort_index().reset_index()

# Print output
print("✅ HDFS Usage Summary by Team:\n")
print(pivot.to_string(index=False))
