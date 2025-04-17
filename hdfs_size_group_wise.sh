#!/bin/bash

set -e

BASE_PATH="/project/abcd/efgh/ijkl"
HDFS_DIR_LIST="hdfs_dirs.txt"
HDFS_DIR_LIST_PATH="/user/$(whoami)/$HDFS_DIR_LIST"
SCALA_APP_JAR="directory_size_app.jar"  # compiled Scala JAR
INPUT_FILE_CONF="spark.cleanup.inputPath=$HDFS_DIR_LIST_PATH"
OUTPUT_PATH="/user/$(whoami)/hdfs_size_output"
FINAL_CSV_PATH="$OUTPUT_PATH/directory_sizes.csv"

echo "➡️ Running Python script to list HDFS directories..."
python3 list_hdfs_dirs.py

echo "➡️ Copying directory list to HDFS..."
hdfs dfs -rm -f "$HDFS_DIR_LIST_PATH" || true
hdfs dfs -put "$HDFS_DIR_LIST" "$HDFS_DIR_LIST_PATH"

echo "➡️ Running Spark job to calculate directory sizes..."
spark-submit \
  --conf "$INPUT_FILE_CONF" \
  --conf "spark.cleanup.outputPath=$OUTPUT_PATH" \
  --class MainClassName "$SCALA_APP_JAR"

echo "➡️ Fetching output CSV from HDFS..."
hdfs dfs -getmerge "$OUTPUT_PATH/directory_sizes.csv" directory_sizes.csv

echo "➡️ Running Python script to group and summarize sizes by team..."
python3 group_directory_sizes.py
