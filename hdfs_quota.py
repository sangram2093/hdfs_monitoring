import subprocess

def get_hdfs_quota_usage(path="/project/stadgcsv"):
    """
    Retrieves HDFS quota and usage for the specified path and prints results
    similar to the web UI, but using the Hadoop CLI.
    """
    try:
        # Execute the Hadoop command to get quota data
        output = subprocess.check_output(
            ["hdfs", "dfs", "-count", "-q", path],
            universal_newlines=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Error running hdfs dfs -count: {e}")
        return

    # Parse the command output line. Example line:
    # QUOTA REM_QUOTA SPACE_QUOTA REM_SPACE_QUOTA DIR_COUNT FILE_COUNT CONTENT_SIZE PATHNAME
    line = output.strip().split()
    if len(line) < 8:
        print("Unexpected output format. Check that path exists.")
        return

    quota_str, rem_quota_str, space_quota_str, rem_space_quota_str, \
    dir_count_str, file_count_str, content_size_str, pathname_str = line

    # Convert to integer if not 'none'
    def safe_int(x):
        return int(x) if x.isdigit() else None

    quota = safe_int(quota_str)                      # Namespace quota
    space_quota = safe_int(space_quota_str)          # Space quota in bytes
    dir_count = safe_int(dir_count_str)
    file_count = safe_int(file_count_str)
    content_size = safe_int(content_size_str)        # Includes replication

    # Calculate usage
    namespace_used = (dir_count or 0) + (file_count or 0)

    # Namespace Quota: if None, no quota is set
    namespace_quota_str = quota_str if quota else "No Quota"
    # Space Quota in GB
    if space_quota:
        space_quota_gb = space_quota / (1024**3)
    else:
        space_quota_gb = 0  # or None if you want to signal no quota

    # Content size in GB
    space_used_gb = (content_size or 0) / (1024**3)

    # Namespace utilization percentage
    if quota:
        namespace_pct = (namespace_used / quota) * 100
    else:
        namespace_pct = 0

    # Space utilization percentage
    if space_quota:
        space_pct = (content_size / space_quota) * 100
    else:
        space_pct = 0

    # Print the desired output
    print(f"Location: {pathname_str}")
    print(f"Namespace Quota: {namespace_quota_str}")
    print(f"Namespace Utilization: {namespace_used}")
    print(f"Namespace Utilization Percentage: {namespace_pct:.2f}%")
    print(f"Space Quota in GB: {space_quota_gb:.2f}")
    print(f"Space Utilization in GB: {space_used_gb:.2f}")
    print(f"Space Utilization Percentage: {space_pct:.2f}%")

if __name__ == "__main__":
    get_hdfs_quota_usage("/project/stadgcsv")
