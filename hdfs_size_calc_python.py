#!/usr/bin/env python3
import sys

# ---------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------
TEAM_KEYWORDS = ["bm", "ib", "fx", "fi", "wm", "pcc", "eq"]
# Adjust this if your "base directory" is at a different position.
# For example, if the full path is: /project/abcd/efgh/ijkl/model/...
# and you want "model" to be considered the "one-level-deep" directory,
# that might be at index 5. Adjust as necessary.
BASE_DIR_INDEX = 5

def parse_size(size_str):
    """
    Convert a size string like '1 GB' or '500 MB' into bytes (int).
    Recognized units: TB, GB, MB, KB, B.
    """
    parts = size_str.strip().split()
    if len(parts) != 2:
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
    Convert a byte size into a human-readable string (TB, GB, MB, etc.).
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

def find_base_directory(path_parts, base_dir_index=BASE_DIR_INDEX):
    """
    Identify the one-level-deep "base directory" at path_parts[base_dir_index].
    If not present, return "unknown".
    """
    if len(path_parts) > base_dir_index:
        return path_parts[base_dir_index]
    return "unknown"

def find_team_keyword(path_parts, team_keywords=TEAM_KEYWORDS):
    """
    Search all path segments after BASE_DIR_INDEX for known team keywords.
    If any segment contains a keyword, return that keyword; else 'other'.
    """
    start_index = BASE_DIR_INDEX + 1
    for segment in path_parts[start_index:]:
        seg_lower = segment.lower()
        for kw in team_keywords:
            if kw in seg_lower:
                return kw
    return "other"

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
                # skip empty or malformed lines
                continue

            path_str, size_str = line.split(",", 1)
            size_in_bytes = parse_size(size_str)

            # 2) Parse the path
            path_parts = path_str.split("/")
            base_dir = find_base_directory(path_parts)
            team = find_team_keyword(path_parts)

            # 3) Accumulate sizes
            key = (base_dir, team)
            aggregator[key] = aggregator.get(key, 0) + size_in_bytes

    # 4) Print results in desired format (per baseDir).
    #    e.g. "bm size in /project/abcd/efgh/ijkl/model = 1 TB"
    #    We'll collect all base_dir values, then print all teams for each base_dir.
    base_dirs = sorted(set(bd for (bd, _) in aggregator.keys()))

    # Also keep track of global sums by team
    global_team_sizes = {}

    for bd in base_dirs:
        # find all (team, size) pairs for this baseDir
        pairs_for_bd = [(team, aggregator[(bd, team)])
                        for (b, team) in aggregator.keys() if b == bd]

        if not pairs_for_bd:
            continue

        # Sort by team name for consistent output
        pairs_for_bd.sort(key=lambda x: x[0])

        # Construct the printing path for base_dir. Adjust prefix if needed.
        full_base_path = f"/project/abcd/efgh/ijkl/{bd}"

        for (team, total_bytes) in pairs_for_bd:
            size_hr = format_size(total_bytes)
            print(f"{team} size in {full_base_path} = {size_hr}")

            # Accumulate into global sums
            global_team_sizes[team] = global_team_sizes.get(team, 0) + total_bytes

    print()
    print("=== Global Sums Across All One-Level Directories ===")
    # Sort by team name
    for team in sorted(global_team_sizes.keys()):
        hr_size = format_size(global_team_sizes[team])
        print(f"{team} size for old dirs = {hr_size}")

if __name__ == "__main__":
    main()
