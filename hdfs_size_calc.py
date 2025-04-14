#!/usr/bin/env python3
import subprocess
import re

# -----------------------------------------------------------------------------
# Define patterns that map directory names to teams.
# Each key is a "team name", and the value is a regex pattern that identifies
# that team's directories. You can add more teams/keywords as necessary.
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
    # Example: cmd_args = ["hdfs", "dfs", "-ls", "/path"]
    result = subprocess.run(
        cmd_args, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE, 
        text=True
    )
    
    # If there's an error, you might want to handle/log it:
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
    # hdfs dfs -ls base_path
    # Typical output lines look like:
    #   drwxr-xr-x   - user group          0 2025-04-14 12:00 /project/abcd/someDir
    #
    # We'll parse the last column to get the directory paths.
    cmd_output = run_hdfs_command(["hdfs", "dfs", "-ls", base_path])
    subdirs = []
    for line in cmd_output:
        parts = line.split()
        # We only look for lines that start with 'd' in the permission field (drwxr-xr-x).
        # This ensures we only get directories, not files. 
        # Adjust logic if you want to include files or handle differently.
        if len(parts) > 0 and parts[0].startswith('d'):
            dir_path = parts[-1]
            subdirs.append(dir_path)

    return subdirs

def get_hdfs_directory_size(directory_path):
    """
    Returns the size (in bytes) of the given HDFS directory using: hdfs dfs -du -s directory_path.
    Adjust this for '-h' if you prefer human-readable sizes.
    """
    # Example output of 'hdfs dfs -du -s /project/abcd/dirX' is something like:
    #   123456  /project/abcd/dirX
    # We'll parse the first column for size in bytes.
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
    If none match, return 'Other' or None.
    """
    directory_name_lower = directory_name.lower()
    for team, pattern in TEAM_PATTERNS.items():
        if re.search(pattern, directory_name_lower):
            return team
    return 'Other'  # or None, depending on how you want to handle unmatched directories.

def main():
    base_path = "/project/abcd"

    # 1. Get the one-level subdirectories:
    subdirectories = get_one_level_subdirectories(base_path)

    # 2. Calculate each directory’s size and identify the team:
    dir_info_list = []
    for subdir in subdirectories:
        size_in_bytes = get_hdfs_directory_size(subdir)
        # Extract just the subdirectory name (e.g. /project/abcd/teamAlpha -> teamAlpha)
        subdir_name = subdir.replace(base_path, '').lstrip('/')
        team = identify_team(subdir_name)
        dir_info_list.append((subdir, team, size_in_bytes))

    # 3. Print or aggregate results per directory:
    print("Detailed Directory Sizes:")
    for entry in dir_info_list:
        print(f"Directory: {entry[0]:40s}  Team: {entry[1]:10s}  Size(bytes): {entry[2]}")

    # 4. Optionally, aggregate per team:
    team_sizes = {}
    for _, team, size in dir_info_list:
        team_sizes[team] = team_sizes.get(team, 0) + size

    print("\nAggregated Sizes By Team:")
    for team, total_size in team_sizes.items():
        print(f"Team: {team:10s}  Total Size (bytes): {total_size}")

if __name__ == "__main__":
    main()
