#!/bin/bash

BASE_PATH="/your/hdfs/path"
CUTOFF_MONTH="202204"
OUTPUT_HARS="matching_har_files.txt"
> "$OUTPUT_HARS"

echo "Processing HAR files with final logic..."

# Find all .har files
hdfs dfs -ls -R "$BASE_PATH" 2>/dev/null | grep -E "\.har$" | awk '{print $8}' | while read filepath; do
  filename=$(basename "$filepath")
  name_core="${filename%.har}"

  # Extract the first numeric group of 6-8 digits (from left to right)
  if [[ "$name_core" =~ ([0-9]{6,8}) ]]; then
    first_date="${BASH_REMATCH[1]}"
    month_part="${first_date:0:6}"

    echo "Checking file: $filename | First date part: $first_date | Month part: $month_part"

    if [[ "$month_part" -lt "$CUTOFF_MONTH" ]]; then
      echo ">>> Matched: $filepath"
      echo "$filepath" >> "$OUTPUT_HARS"
    else
      echo "Skipped (newer than cutoff): $filepath"
    fi
  else
    echo "Skipped (no date pattern): $filepath"
  fi
done

echo "Done."
echo "Matching HAR files written to: $OUTPUT_HARS"
