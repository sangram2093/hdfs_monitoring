import subprocess
import os

# Configurable base path and subdirectories of interest
base_path = "/project/abcd/efgh/ijkl"
subdirs_of_interest = ["model", "alert", "feed", "process"]
output_file = "hdfs_directories.txt"

# Function to run hdfs dfs -ls and return directory names
def list_hdfs_directories(path):
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-ls", path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,  # Use this instead of text=True for Python 3.6
            check=True
        )
        lines = result.stdout.strip().split("\n")[1:]  # Skip header
        dirs = []
        for line in lines:
            parts = line.split()
            if parts and parts[0].startswith("d"):  # Ensure non-empty line and it's a directory
                dirs.append(parts[-1])
        return dirs
    except subprocess.CalledProcessError:
        return []

# Collect all 1-level deep directories under the interested subdirs
all_dirs = []

for subdir in subdirs_of_interest:
    full_path = os.path.join(base_path, subdir)
    dirs = list_hdfs_directories(full_path)
    all_dirs.extend(dirs)

# Write to output file
with open(output_file, "w") as f:
    for d in all_dirs:
        f.write(d + "\n")

print(f"✅ Directory listing saved to {output_file}")
