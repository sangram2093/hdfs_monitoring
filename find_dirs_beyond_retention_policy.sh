#!/bin/bash

# Configurable variables
BASE_PATH="/your/hdfs/path"
CUTOFF_DATE="20220401"
OUTPUT_FILE="matching_old_dirs.txt"
> "$OUTPUT_FILE"  # Clear output

echo "Scanning directories under $BASE_PATH..."

# Get all directories recursively
hdfs dfs -ls -R "$BASE_PATH" 2>/dev/null | grep "^d" | awk '{print $8}' | while read dir; do
  # Loop through each part of the path
  IFS='/' read -ra parts <<< "$dir"
  for part in "${parts[@]}"; do
    # Check if part matches YYYYMMDD
    if [[ "$part" =~ ^[0-9]{8}$ ]]; then
      # Compare with cutoff
      if [[ "$part" -lt "$CUTOFF_DATE" ]]; then
        echo ">>> Matched: $dir (with date $part)"
        echo "$dir" >> "$OUTPUT_FILE"
        break  # No need to check other parts in path
      fi
    fi
  done
done

echo "Done."
echo "Matching directories written to: $OUTPUT_FILE"
