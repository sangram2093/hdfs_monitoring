#!/bin/bash

# Define base path to scan
BASE_PATH="/your/hdfs/path"

# Define cutoff date in YYYY-MM-DD format
CUTOFF_DATE="2022-04-01"

# Output file
OUTPUT_FILE="old_files_before_20220401.txt"
> "$OUTPUT_FILE"  # Clear output file

echo "Scanning files in $BASE_PATH older than $CUTOFF_DATE..."

# List all entries and filter only files (first char is '-')
hdfs dfs -ls -R "$BASE_PATH" 2>/dev/null | grep "^-" | while read -r perm rep user group size date time filepath; do
  file_datetime="${date} ${time}"
  
  # Convert file timestamp and cutoff to epoch (for comparison)
  file_epoch=$(date -d "$file_datetime" +%s)
  cutoff_epoch=$(date -d "$CUTOFF_DATE" +%s)

  # Compare and record if file is older
  if [[ "$file_epoch" -lt "$cutoff_epoch" ]]; then
    echo ">>> Matched: $filepath ($file_datetime)"
    echo "$filepath" >> "$OUTPUT_FILE"
  fi
done

echo "Done."
echo "Matching files written to: $OUTPUT_FILE"
