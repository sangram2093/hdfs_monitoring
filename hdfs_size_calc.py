#!/usr/bin/env python3
import subprocess
import re

# -----------------------------------------------------------------------------
# Define patterns that map directory names to teams (optional).
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
        # Only look for lines that start with 'd' => directory
        if len(parts) > 0 and parts[0].startswith('d'):
            dir_path = parts[-1]
            subdirs.append(dir_path)
    return subdirs

def get_hdfs_directory_size(directory_path):
    """
    Returns the size (in bytes) of the given HDFS directory using:
    hdfs dfs -du -s directory_path
    This includes everything underneath that directory.
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
    against each pattern in TEAM_PATTERNS. If none match, return 'Other'.
    """
    directory_name_lower = directory_name.lower()
    for team, pattern in TEAM_PATTERNS.items():
        if re.search(pattern, directory_name_lower):
            return team
    return 'Other'

def format_size(size_in_bytes):
    """
    Return a human-readable string for the given byte size.
    Using powers of 1024 (common in storage contexts):
      1 TB = 1024^4
      1 GB = 1024^3
      1 MB = 1024^2
      1 KB = 1024^1
    """
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

    print(f"Base Path: {base_path}")
    print("Retrieving top-level directories...\n")
    
    # 1. Get the one-level subdirectories:
    top_level_dirs = get_one_level_subdirectories(base_path)

    # Store info about top-level directories here:
    # Each element: (directory_path, team, size_in_bytes, size_readable)
    top_level_info = []
    
    # 2. Calculate each top-level directory’s size and identify the team:
    for d in top_level_dirs:
        raw_size = get_hdfs_directory_size(d)
        size_str = format_size(raw_size)
        # Identify team (optional)
        subdir_name = d.replace(base_path, '').lstrip('/')
        team = identify_team(subdir_name)
        top_level_info.append((d, team, raw_size, size_str))

    # 3. Print summary of top-level directories:
    print("Top-Level Directories Under /project/abcd\n")
    for entry in top_level_info:
        dir_path, team, raw_size, readable_size = entry
        print(f"Directory: {dir_path:40s} | Team: {team:10s} | Size: {readable_size}")
    print()

    # 4. For each top-level directory, get its immediate subdirectories (one level deep)
    #    and calculate their sizes as well.
    print("Second-Level Directories (One Level Below Each Top-Level Directory)\n")
    
    for entry in top_level_info:
        dir_path, team, raw_size, readable_size = entry
        
        # Get immediate subdirectories of this top-level directory
        child_subdirs = get_one_level_subdirectories(dir_path)
        if not child_subdirs:
            # If none, skip printing
            continue
        
        print(f"Subdirectories under: {dir_path} (Team: {team}, Size: {readable_size})")
        for child in child_subdirs:
            child_size_bytes = get_hdfs_directory_size(child)
            child_size_str = format_size(child_size_bytes)
            # We can assume child belongs to the same top-level team, or re-identify if needed.
            print(f"   -> {child:40s}  Size: {child_size_str}")
        print()  # Blank line for readability

    # 5. Optionally, aggregate sizes by team for the top-level dirs only
    print("Aggregated Sizes By Team (Top-Level Only)\n")
    team_sizes = {}
    for _, team, raw_size, _ in top_level_info:
        team_sizes[team] = team_sizes.get(team, 0) + raw_size
    
    for t, total_size_bytes in team_sizes.items():
        print(f"Team: {t:10s}  Total Size: {format_size(total_size_bytes)}")

if __name__ == "__main__":
    main()
