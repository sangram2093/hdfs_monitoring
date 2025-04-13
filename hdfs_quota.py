import subprocess

def get_hdfs_quota_usage(path="/project/stadgcsv"):
    """
    Retrieves HDFS quota and usage for the specified path and prints results.
    This script uses one line of output from:
        hdfs dfs -count -q /project/stadgcsv
    Example of command output (single line):
        200000 4726 17598478645673 14499787842345 52697 142577 1560987564123 /project/stadgcsv
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

    # The command returns a single line. Example:
    # 200000 4726 17598478645673 14499787842345 52697 142577 1560987564123 /project/stadgcsv
    line = output.strip().split()
    if len(line) < 8:
        print("Unexpected output format. Check that path exists or command output.")
        return

    # Assign the fields according to your provided example
    ns_quota_str, ns_remain_str, space_quota_str, space_used_str, \
    dir_count_str, file_count_str, unknown_str, location_str = line

    # Convert numeric fields to integers
    ns_quota = int(ns_quota_str)          # 200000
    ns_remaining = int(ns_remain_str)     # 4726
    space_quota = int(space_quota_str)    # 17598478645673
    space_used = int(space_used_str)      # 14499787842345
    # (dir_count_str, file_count_str, unknown_str) not used for these calculations

    # --- Calculate namespace usage ---
    ns_used = ns_quota - ns_remaining
    ns_usage_pct = (ns_used / ns_quota) * 100 if ns_quota else 0

    # --- Calculate space usage in TB ---
    # 1 TB = 1024^4 bytes = 1,099,511,627,776 bytes
    TB_IN_BYTES = 1024 ** 4
    space_quota_tb = space_quota / TB_IN_BYTES
    space_used_tb = space_used / TB_IN_BYTES
    space_usage_pct = (space_used / space_quota) * 100 if space_quota else 0

    # Print the desired output
    print(f"Location: {location_str}")
    print(f"Namespace Quota: {ns_quota}")
    print(f"Namespace Utilization: {ns_used}")
    print(f"Namespace Utilization Percentage: {ns_usage_pct:.2f}%")
    print(f"Space Quota in TB: {space_quota_tb:.2f}")
    print(f"Space Utilization in TB: {space_used_tb:.2f}")
    print(f"Space Utilization Percentage: {space_usage_pct:.2f}%")


if __name__ == "__main__":
    get_hdfs_quota_usage("/project/stadgcsv")
