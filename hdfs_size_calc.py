#!/usr/bin/env python3
import subprocess
import re

# -----------------------------------------------------------------------------
# Define patterns that map directory names to teams.
# Adjust or add more patterns as needed.
# -----------------------------------------------------------------------------
TEAM_PATTERNS = {
    'TeamAlpha': r'(alpha|team_alpha|alpha_team)',
    'TeamBeta':  r'(beta|team_beta|beta_team)',
    # Add more teams as needed.
}

def run_hdfs_command(cmd_args):
    """
    Helper function to run an HDFS command (e.g., hdfs dfs -ls ...) 
    and return the output as a list of lines.
    """
    result = subprocess.run(
        cmd_args, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE, 
        text=True
    )
    
    if result.returncode != 0:
        print(f"Error running command: {' '.join(cmd_args)}")
        print(result.stderr)
        return []
    
    return result.stdout.strip().split('\n')

def get_one_level_subdirectories(base_path):
    """
    Gets the immediate subdirectories (one level deep) under base_path.
    Returns a list of subdirectory paths: [base_path/subdir1, base_path/subdir2, ...]
    """
    cmd_output = run_hdfs_command(["hdfs", "dfs", "-ls", base_path])
    subdirs = []
    for line in cmd_output:
        parts = line.split()
        # We only look for lines that start with 'd' (directory)
        if len(parts) > 0 and parts[0].startswith('d'):
            dir_path = parts[-1]
            subdirs.append(dir_path)
    return subdirs

def get_hdfs_directory_size(directory_path):
    """
    Returns the size (in bytes) of the given HDFS directory using:
    hdfs dfs -du -s directory_path
    """
    cmd_output = run_hdfs_command(["hdfs", "dfs", "-du", "-s", directory_path])
    if len(cmd_output) > 0:
        parts = cmd_output[0].split()
        if len(parts) >= 2:
            try:
                size_in_bytes = int(parts[0])
                return size_in_bytes
            except ValueError:
                pass
    return 0

def identify_team(directory_name):
    """
    Identify which team a directory belongs to by checking directory_name
    against each pattern in TEAM_PATTERNS.
    If none match, return 'Other'.
    """
    directory_name_lower = directory_name.lower()
    for team, pattern in TEAM_PATTERNS.items():
        if re.search(pattern, directory_name_lower):
            return team
    return 'Other'

def format_size(size_in_bytes):
    """
    Return a human-readable string for the given byte size.
    Specifically:
      - >= 1 TB => TB
      - >= 1 GB => GB
      - >= 1 MB => MB
      - >= 1 KB => KB
      - otherwise => B
    """
    # Using 1 TB = 1024^4, 1 GB = 1024^3, etc.
    TB = 1024**4
    GB = 1024**3
    MB = 1024**2
    KB = 1024
    
    if size_in_bytes >= TB:
        return f"{size_in_bytes / TB:.2f} TB"
    elif size_in_bytes >= GB:
        return f"{size_in_bytes / GB:.2f} GB"
    elif size_in_bytes >= MB:
        return f"{size_in_bytes / MB:.2f} MB"
    elif size_in_bytes >= KB:
        return f"{size_in_bytes / KB:.2f} KB"
    else:
        return f"{size_in_bytes} B"

def main():
    base_path = "/project/abcd"

    # 1. Get the one-level subdirectories:
    subdirectories = get_one_level_subdirectories(base_path)

    # 2. Calculate each directory’s size and identify the team:
    dir_info_list = []
    for subdir in subdirectories:
        size_in_bytes = get_hdfs_directory_size(subdir)
        # For display, convert the size to a readable string:
        size_readable = format_size(size_in_bytes)
        
        # Extract just the subdirectory name (e.g. /project/abcd/teamAlpha -> teamAlpha)
        subdir_name = subdir.replace(base_path, '').lstrip('/')
        team = identify_team(subdir_name)
        
        dir_info_list.append((subdir, team, size_in_bytes, size_readable))

    # 3. Print the directory info
    print("Detailed Directory Sizes:")
    for entry in dir_info_list:
        subdir, team, raw_size, readable_size = entry
        print(f"Directory: {subdir:40s}  Team: {team:10s}  Size: {readable_size}")

    # 4. Aggregate sizes per team (in bytes) and then convert to TB/GB as needed:
    team_sizes = {}
    for _, team, raw_size, _ in dir_info_list:
        team_sizes[team] = team_sizes.get(team, 0) + raw_size

    print("\nAggregated Sizes By Team:")
    for team, total_size_bytes in team_sizes.items():
        print(f"Team: {team:10s}  Total Size: {format_size(total_size_bytes)}")

if __name__ == "__main__":
    main()
