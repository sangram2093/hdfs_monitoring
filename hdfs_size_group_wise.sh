#!/bin/bash

set -euo pipefail

BASE_PATH="/project/abcd/efgh/ijkl"
HDFS_DIR_LIST="hdfs_dirs.txt"
HDFS_DIR_LIST_PATH="/user/$(whoami)/$HDFS_DIR_LIST"
SCALA_APP_JAR="directory_size_app.jar"  # Update this to actual JAR path
FINAL_CSV_LOCAL="directory_sizes.csv"
SPARK_MAIN_CLASS="MainClassName"        # Replace with actual class name
OUTPUT_PATH="/user/$(whoami)/hdfs_size_output"
FINAL_CSV_PATH="$OUTPUT_PATH/$FINAL_CSV_LOCAL"

log() {
    echo -e "\033[1;34m➡️  $1\033[0m"
}

error_exit() {
    echo -e "\033[1;31m❌ Error: $1\033[0m" >&2
    exit 1
}

# Step 1: List HDFS directories using Python
log "Running Python script to list HDFS directories..."
python3 list_hdfs_dirs.py || error_exit "Failed to list HDFS directories."

# Step 2: Copy file to HDFS
log "Copying directory list to HDFS..."
hdfs dfs -rm -f "$HDFS_DIR_LIST_PATH" > /dev/null 2>&1 || true
hdfs dfs -put "$HDFS_DIR_LIST" "$HDFS_DIR_LIST_PATH" || error_exit "Failed to copy file to HDFS."

# Step 3: Run Spark job to calculate sizes
log "Running Spark job to calculate directory sizes..."
spark-submit \
  --conf "spark.cleanup.inputPath=$HDFS_DIR_LIST_PATH" \
  --conf "spark.cleanup.outputPath=$OUTPUT_PATH" \
  --class "$SPARK_MAIN_CLASS" "$SCALA_APP_JAR" || error_exit "Spark job failed."

# Step 4: Fetch results from HDFS
log "Fetching output CSV from HDFS..."
rm -f "$FINAL_CSV_LOCAL"
hdfs dfs -test -e "$FINAL_CSV_PATH" || error_exit "Final CSV not found in HDFS."
hdfs dfs -getmerge "$FINAL_CSV_PATH" "$FINAL_CSV_LOCAL" || error_exit "Failed to fetch CSV from HDFS."

# Step 5: Group and summarize using Python
log "Running Python script to group and summarize sizes by team..."
python3 group_directory_sizes.py || error_exit "Failed to summarize grouped results."

log "✅ All steps completed successfully."
