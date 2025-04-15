#!/usr/bin/env python3
import sys

# ---------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------
TEAM_KEYWORDS = ["bm", "ib", "fx", "fi", "wm", "pcc", "eq"]
BASE_PREFIX = "/project/abcd/"  # We'll use this to reconstruct base path strings

# ---------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------
def parse_size(size_str):
    """
    Convert a size string like '1 GB' or '500 MB' into bytes (int).
    Recognized units: TB, GB, MB, KB, B.
    """
    parts = size_str.strip().split()
    if len(parts) != 2:
        # If malformed, return 0 or raise an exception as needed
        return 0

    num_str, unit = parts
    try:
        value = float(num_str)
    except ValueError:
        return 0

    unit = unit.upper()
    if unit == "TB":
        return int(value * 1024**4)
    elif unit == "GB":
        return int(value * 1024**3)
    elif unit == "MB":
        return int(value * 1024**2)
    elif unit == "KB":
        return int(value * 1024)
    elif unit == "B":
        return int(value)
    else:
        return 0  # Unrecognized unit

def format_size(size_in_bytes):
    """
    Return a human-readable string for the given byte size.
    E.g. 1.23 TB, 456.78 MB, etc.
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

def find_team_from_path(path_parts):
    """
    Given the split path parts, find which team keyword applies.
    We assume the 'team' directory is path_parts[3] or [4], etc.
    Typically:
      0: '' (if path starts with /)
      1: 'project'
      2: 'abcd'
      3: e.g. 'model' or 'feed'
      4: e.g. 'bm', 'eq', ...
    If the subdirectory name (index 4) contains a known keyword, we return it; otherwise 'other'.
    """
    if len(path_parts) > 4:
        subdir_name = path_parts[4].lower()
        for kw in TEAM_KEYWORDS:
            if kw in subdir_name:
                return kw
    return "other"

def find_base_directory(path_parts):
    """
    Identify the one-level-deep base directory (e.g., 'model', 'feed', 'alert', 'process')
    from a path like /project/abcd/model/...
    Typically path_parts[3].
    """
    if len(path_parts) > 3:
        return path_parts[3]
    return "unknown"

# ---------------------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------------------
def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <old_dirs_file>")
        sys.exit(1)

    input_file = sys.argv[1]

    # aggregator dict: key = (baseDir, team), value = total_size_in_bytes
    aggregator = {}

    # 1) Read each line, parse path & size
    with open(input_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line or "," not in line:
                continue  # skip empty or malformed lines

            path_str, size_str = line.split(",", 1)
            size_in_bytes = parse_size(size_str)

            # 2) Parse the path to get base directory & team
            path_parts = path_str.split("/")
            base_dir = find_base_directory(path_parts)
            team = find_team_from_path(path_parts)

            # 3) Accumulate sizes
            key = (base_dir, team)
            aggregator[key] = aggregator.get(key, 0) + size_in_bytes

    # 4) Print results in desired format
    #    e.g. "bm size in /project/abcd/model = 1 TB"
    # We'll group by the base_dir so that we print teams for each base_dir together.

    # First, collect all base_dir values
    base_dirs = sorted(set([bd for (bd, _) in aggregator.keys()]))

    for bd in base_dirs:
        # find all team groups for this base dir
        # sort them by name if you wish
        teams_for_bd = [(team, aggregator[(bd, team)]) 
                        for team in sorted(t for (b, t) in aggregator.keys() if b == bd)]
        
        # skip if no data
        if not teams_for_bd:
            continue
        
        base_dir_path = BASE_PREFIX + bd  # e.g. /project/abcd/model
        for (team, size_bytes) in teams_for_bd:
            size_hr = format_size(size_bytes)
            print(f"{team} size in {base_dir_path} = {size_hr}")

if __name__ == "__main__":
    main()
