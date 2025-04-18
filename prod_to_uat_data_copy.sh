#!/bin/bash

# -------- CONFIGURATION --------
PROD_EDGE_BASE_DIR="/apps/abcd-efg/support/datacopy/data/har"
HAR_FILE_LIST="/export/home/user/har_files_to_copy.txt"

UAT_SSH_USER="user"
UAT_SSH_HOST="abcd.gf.jg.com"
UAT_EDGE_BASE_DIR="/apps/abcd-efg/support/datacopy/data/har"
UAT_HDFS_BASE="/"

# -------- FUNCTION TO COPY HAR FILE TO PROD EDGE NODE --------
copy_from_hdfs_to_edge() {
  local har_hdfs_path="$1"
  local local_path="${PROD_EDGE_BASE_DIR}${har_hdfs_path}"

  echo "Copying from HDFS: $har_hdfs_path to Prod Edge Node: $local_path"
  mkdir -p "$(dirname "$local_path")"
  hdfs dfs -copyToLocal "$har_hdfs_path" "$local_path"
}

# -------- FUNCTION TO COPY FILE TO UAT --------
copy_to_uat_edge() {
  local har_hdfs_path="$1"
  local remote_path="${UAT_EDGE_BASE_DIR}${har_hdfs_path}"
  local local_path="${PROD_EDGE_BASE_DIR}${har_hdfs_path}"
  local remote_dir
  remote_dir=$(dirname "$remote_path")

  echo "Creating directory in UAT Edge Node: $remote_dir"
  ssh "${UAT_SSH_USER}@${UAT_SSH_HOST}" "rm -rf \"$remote_dir\" && mkdir -p \"$remote_dir\""

  echo "SCP from Prod Edge to UAT Edge: $local_path → $remote_path"
  scp "$local_path" "${UAT_SSH_USER}@${UAT_SSH_HOST}:$remote_path"
}

# -------- FUNCTION TO CLEANUP PROD EDGE NODE --------
cleanup_prod_edge_node() {
  local har_hdfs_path="$1"
  local local_dir
  local_dir=$(dirname "${PROD_EDGE_BASE_DIR}${har_hdfs_path}")
  echo "Removing directory from Prod Edge Node: $local_dir"
  rm -rf "$local_dir"
}

# -------- FUNCTION TO COPY HAR TO UAT HDFS --------
copy_to_uat_hdfs() {
  local har_hdfs_path="$1"
  local remote_file_path="${UAT_EDGE_BASE_DIR}${har_hdfs_path}"
  local uat_hdfs_dir
  uat_hdfs_dir=$(dirname "$har_hdfs_path")
  local uat_hdfs_full_path="${UAT_HDFS_BASE}${uat_hdfs_dir}"

  echo "Copying to UAT HDFS: $remote_file_path → $uat_hdfs_full_path"

  ssh "${UAT_SSH_USER}@${UAT_SSH_HOST}" bash <<EOF
    hdfs dfs -mkdir -p "$uat_hdfs_full_path"
    hdfs dfs -copyFromLocal "$remote_file_path" "$uat_hdfs_full_path"
    echo "Removing UAT Edge directory: $(dirname "$remote_file_path")"
    rm -rf "$(dirname "$remote_file_path")"
EOF
}

# -------- MAIN LOOP --------
while IFS= read -r har_file_path; do
  [[ -z "$har_file_path" || "$har_file_path" == \#* ]] && continue  # Skip blank or commented lines

  echo "==== Processing: $har_file_path ===="

  # Step 1: Copy from HDFS to Prod Edge Node
  copy_from_hdfs_to_edge "$har_file_path"

  # Step 2: SCP to UAT Edge Node
  copy_to_uat_edge "$har_file_path"

  # Step 3: Remove from Prod Edge Node
  cleanup_prod_edge_node "$har_file_path"

  # Step 4: SSH to UAT and Copy to HDFS, then Cleanup
  copy_to_uat_hdfs "$har_file_path"

  echo "==== Completed: $har_file_path ===="
  echo ""
done < "$HAR_FILE_LIST"
