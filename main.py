#!/usr/bin/env python3

import os
import re
import sys
import yaml
import argparse
import subprocess
import smtplib
import ssl
import zipfile
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Regex for identifying "scheduled_deletion_YYYYmmdd" directories
DATE_PATTERN = re.compile(r"^scheduled_deletion_(\d{8})$")

def load_config(config_file):
    """Loads YAML config from the specified path."""
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def send_email(subject, body, recipients, cc=None, attachment=None, config=None):
    """
    Sends an email with optional attachment using SMTP server details from config.
    Attaches the file at 'attachment' if provided.
    """
    if config is None:
        raise ValueError("SMTP config required for sending email.")

    msg = MIMEMultipart()
    msg["From"] = config["email"]["from"]
    msg["To"]   = ", ".join(recipients)
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg["Subject"] = subject
    
    # Email Body
    msg.attach(MIMEText(body, "plain"))

    # Attachment (optional)
    if attachment and os.path.exists(attachment):
        with open(attachment, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f'attachment; filename="{os.path.basename(attachment)}"',
        )
        msg.attach(part)
    
    # Connect & send via SMTP
    smtp_server = config["email"]["host"]
    port        = config["email"]["port"]
    username    = config["email"]["username"]
    password    = config["email"]["password"]
    
    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, port) as server:
        if config["email"].get("use_tls", False):
            server.starttls(context=context)
        server.login(username, password)
        
        final_recipients = recipients + (cc if cc else [])
        server.sendmail(
            from_addr=config["email"]["from"],
            to_addrs=final_recipients,
            msg=msg.as_string()
        )

def normalize_path(p: str) -> str:
    """Remove trailing slash except if p == '/'."""
    if p == "/":
        return "/"
    return p.rstrip("/")

def is_path_excluded(path: str, exclusion_list: set) -> bool:
    """
    Returns True if 'path' should be excluded because it exactly matches or
    is under a directory in the exclusion_list.
    """
    norm_path = normalize_path(path)
    for excl in exclusion_list:
        norm_excl = normalize_path(excl)
        if norm_path == norm_excl:
            return True
        if norm_excl != "/" and norm_path.startswith(norm_excl + "/"):
            return True
        if norm_excl == "/" and norm_path.startswith("/"):
            return True
    return False

def read_file_list(file_path):
    """
    Reads file paths from a text file into a set, ignoring any invalid surrogates.
    Returns an empty set if file doesn't exist.
    """
    if not os.path.exists(file_path):
        return set()
    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.read().splitlines()
    return set(line.strip() for line in lines if line.strip())

def write_file_list(file_paths, target_file):
    """Writes file paths to a text file, handling any surrogate errors."""
    with open(target_file, 'w', encoding='utf-8', errors='replace') as f:
        for p in file_paths:
            f.write(p + "\n")

def get_upcoming_cycle(tmp_path):
    """
    Scans tmp_path for directories named 'scheduled_deletion_YYYYmmdd',
    returns the earliest date >= today or None if none found.
    """
    if not os.path.exists(tmp_path):
        return None

    today = datetime.now().date()
    upcoming_date_str = None

    for entry in os.listdir(tmp_path):
        match = DATE_PATTERN.match(entry)
        if match:
            cycle_date_str = match.group(1)
            try:
                cycle_date = datetime.strptime(cycle_date_str, "%Y%m%d").date()
                if cycle_date >= today:
                    if (upcoming_date_str is None) or (cycle_date < datetime.strptime(upcoming_date_str, "%Y%m%d").date()):
                        upcoming_date_str = cycle_date_str
            except ValueError:
                pass
    
    return upcoming_date_str

def zip_file(original_file):
    """
    Creates a ZIP of 'original_file' in the same location
    and returns the path to the .zip
    """
    zip_path = original_file + ".zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(original_file, arcname=os.path.basename(original_file))
    return zip_path

# ============================= HDFS MODE LOGIC =============================

def find_old_files_hdfs(hdfs_base_path, threshold_days, perm_exclusions):
    """
    Use 'hdfs dfs -ls -R' to list all paths older than threshold_days,
    skipping anything that matches 'perm_exclusions' (never scanned).
    """
    threshold_date = datetime.now() - timedelta(days=threshold_days)
    cmd = ["hdfs", "dfs", "-ls", "-R", hdfs_base_path]
    old_files = []

    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8", errors="replace")
    except subprocess.CalledProcessError as e:
        print("Error running HDFS list command:", e.output.decode("utf-8", errors="replace"))
        return []

    for line in output.splitlines():
        parts = line.strip().split()
        if len(parts) < 8:
            continue
        date_str = parts[5]
        time_str = parts[6]
        path_str = parts[7]

        if is_path_excluded(path_str, perm_exclusions):
            continue

        try:
            file_datetime = datetime.strptime(date_str + " " + time_str, "%Y-%m-%d %H:%M")
        except ValueError:
            continue

        if file_datetime < threshold_date:
            old_files.append(path_str)
    
    return old_files

def run_scala_deletion_job(config, hdfs_final_deletion_list):
    """
    Calls spark-submit for the Scala-based HDFS deletion job,
    passing 'hdfs_final_deletion_list' as spark.cleanup.inputPath.
    """
    scala_jar = "/path/to/HdfsDeletion.jar"
    main_class = "HdfsDeletion"
    
    cmd = [
        "spark-submit",
        "--class", main_class,
        "--conf", f"spark.cleanup.inputPath={hdfs_final_deletion_list}",
        scala_jar
    ]
    print(f"Running Scala deletion job with command: {' '.join(cmd)}")
    try:
        subprocess.check_call(cmd)
        print("Scala deletion job completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running Scala job: {e}")

def hdfs_deletion_workflow(config, final_list, local_final_path, hdfs_final_deletion_list):
    """
    1) Write final_list to a local file
    2) Remove existing file in HDFS if it exists
    3) 'put' the local file to HDFS
    4) run spark-submit for actual deletion
    """
    # 1) local
    write_file_list(final_list, local_final_path)

    # 2) remove old file in HDFS
    try:
        subprocess.check_call(["hdfs", "dfs", "-test", "-e", hdfs_final_deletion_list])
        print(f"HDFS file '{hdfs_final_deletion_list}' exists. Removing it first...")
        subprocess.check_call(["hdfs", "dfs", "-rm", "-skipTrash", hdfs_final_deletion_list])
    except subprocess.CalledProcessError:
        print(f"No existing file at '{hdfs_final_deletion_list}' to remove.")

    # 3) copy
    try:
        subprocess.check_call(["hdfs", "dfs", "-put", local_final_path, hdfs_final_deletion_list])
        print(f"Copied final list to HDFS: {hdfs_final_deletion_list}")
    except subprocess.CalledProcessError as e:
        print(f"Error copying file to HDFS: {e}")
        return

    # 4) run spark
    run_scala_deletion_job(config, hdfs_final_deletion_list)

# =========================== LOCAL VERSION-BASED LOGIC ==========================

VERSION_DIR_REGEX = re.compile(r"^\d+(\.\d+)*$")  
# Matches version directories like 1, 1.0, 1.0.1, 0.1, etc.

def parse_version_str(v: str):
    """
    Convert a version string like '1.0.3' or '0.1.0' into a tuple of ints (1,0,3).
    Compare them lexicographically to find the largest.
    """
    # Split on any non-digit, e.g. '.' or '-'
    parts = re.split(r"[^\d]+", v)
    nums = []
    for p in parts:
        if p.isdigit():
            nums.append(int(p))
    return tuple(nums)

def find_versioned_directories(dir_path):
    """
    1. In 'dir_path', find all subdirectories that match the version pattern (VERSION_DIR_REGEX).
    2. Identify the highest version subdir. Return all others as "old" so they can be scheduled for deletion.
    """
    old_versions = []
    highest_ver_dir = None
    highest_ver_tuple = ()

    try:
        with os.scandir(dir_path) as it:
            for entry in it:
                if entry.is_dir():
                    name = entry.name
                    if VERSION_DIR_REGEX.match(name):
                        ver_tuple = parse_version_str(name)
                        if ver_tuple > highest_ver_tuple:
                            highest_ver_tuple = ver_tuple
                            highest_ver_dir = name
    except Exception as e:
        print(f"Error scanning versioned dir {dir_path}: {e}")
        return []

    if highest_ver_dir is None:
        # no recognized version subdirs
        return []
    else:
        to_remove = []
        try:
            with os.scandir(dir_path) as it:
                for entry in it:
                    if entry.is_dir():
                        name = entry.name
                        if VERSION_DIR_REGEX.match(name) and name != highest_ver_dir:
                            full_path = os.path.join(dir_path, name)
                            to_remove.append(full_path)
        except Exception as e:
            print(f"Error scanning old versions in {dir_path}: {e}")
            return []
        
        return to_remove

def find_old_files_local(local_base_path, threshold_days, perm_exclusions, versioned_dir_list=None):
    """
    Local scanning logic:
    1) For directories in versioned_dir_list, find older version dirs (all but the highest).
    2) For everything else, find files older than threshold_days (st_mtime).
    3) Skip anything in permanent_exclusions.
    4) Return combined list of old items.
    """
    if versioned_dir_list is None:
        versioned_dir_list = set()

    versioned_old = []  # older version directories to remove
    normal_old = []     # normal "older than X days" files

    # A) Handle version-based dirs
    for vd in versioned_dir_list:
        if is_path_excluded(vd, perm_exclusions):
            continue
        old_vers = find_versioned_directories(vd)
        versioned_old.extend(old_vers)

    # B) Normal scanning for older files
    cutoff_time = datetime.now().timestamp() - (threshold_days * 86400)
    for root, dirs, files in os.walk(local_base_path):
        # If root is in perm_exclusions, skip entire subtree
        if is_path_excluded(root, perm_exclusions):
            dirs[:] = []
            continue
        
        # If root is specifically in versioned_dir_list, skip normal scanning for it
        if root in versioned_dir_list:
            dirs[:] = []
            continue

        for fname in files:
            full_path = os.path.join(root, fname)
            if is_path_excluded(full_path, perm_exclusions):
                continue

            # check st_mtime
            try:
                st = os.stat(full_path)
                if st.st_mtime < cutoff_time:
                    normal_old.append(full_path)
            except Exception as e:
                print(f"Error accessing {full_path}: {e}")

    return normal_old + versioned_old

def delete_files_local(final_list):
    """Delete the given files/directories from local filesystem."""
    for path in final_list:
        try:
            if os.path.isdir(path):
                subprocess.check_call(["rm", "-rf", path])
                print(f"Directory deleted: {path}")
            else:
                os.remove(path)
                print(f"File deleted: {path}")
        except Exception as e:
            print(f"Error deleting {path}: {e}")

def local_deletion_workflow(final_list, local_final_path):
    """
    1) Write final_list to local file (for record)
    2) Delete them from local filesystem
    """
    write_file_list(final_list, local_final_path)
    delete_files_local(final_list)

# =========================== MAIN SCHEDULING LOGIC ===========================

def main():
    parser = argparse.ArgumentParser(description="Cleanup script for HDFS or Local filesystem (with version-based logic).")
    parser.add_argument("--config", default="/path/to/config.yaml", help="Path to YAML config file.")
    parser.add_argument("--mode", required=True, choices=["hdfs", "local"], help="Run mode: 'hdfs' or 'local'.")
    args = parser.parse_args()

    config = load_config(args.config)

    # Decide which block of config to use
    if args.mode == "hdfs":
        threshold_days = config["threshold_days_hdfs"]
        base_path      = config["hdfs_base_path"]
        tmp_path       = config["temp_storage_path_hdfs"]
        permanent_exclusion_file = config["permanent_exclusion_file_hdfs"]
        temp_exclusion_file      = config["temp_exclusion_file_hdfs"]
        # No version-based logic for HDFS in this example
        versioned_dir_list_file  = None
    else:
        threshold_days = config["threshold_days_local"]
        base_path      = config["local_base_path"]
        tmp_path       = config["temp_storage_path_local"]
        permanent_exclusion_file = config["permanent_exclusion_file_local"]
        temp_exclusion_file      = config["temp_exclusion_file_local"]
        # For local mode, read versioned_dir_list
        versioned_dir_list_file = config.get("versioned_dirs_local")

    # Read permanent exclusions (skip these at scan time)
    permanent_exclusions = read_file_list(permanent_exclusion_file)

    # Check if we already have a future cycle or need a new one
    upcoming_cycle_str = get_upcoming_cycle(tmp_path)

    if not upcoming_cycle_str:
        # ---- Create new cycle (today+4 days) ----
        new_cycle_dt = datetime.now() + timedelta(days=4)
        new_cycle_str = new_cycle_dt.strftime("%Y%m%d")
        deletion_dir  = os.path.join(tmp_path, f"scheduled_deletion_{new_cycle_str}")
        os.makedirs(deletion_dir, exist_ok=True)

        scheduled_file_list = os.path.join(deletion_dir, f"files_scheduled_{new_cycle_str}.txt")

        # Perform scanning
        if args.mode == "hdfs":
            old_files = find_old_files_hdfs(base_path, threshold_days, permanent_exclusions)
        else:
            # local scanning w/ version-based logic
            versioned_dirs_list = read_file_list(versioned_dir_list_file) if versioned_dir_list_file else set()
            old_files = find_old_files_local(base_path, threshold_days, permanent_exclusions, versioned_dir_list=versioned_dirs_list)

        # Write scheduled file
        write_file_list(old_files, scheduled_file_list)
        
        # Optionally zip it before emailing
        zip_path = zip_file(scheduled_file_list)

        # Email
        subject    = config["email"]["subject"]
        body       = config["email"]["body"]
        recipients = config["email"]["to"]
        cc         = config["email"].get("cc")

        send_email(
            subject,
            body,
            recipients,
            cc=cc,
            attachment=zip_path,
            config=config
        )
        print(f"New cycle started for {new_cycle_str} in {args.mode} mode. Emailed scheduled file.")
    
    else:
        # ---- Existing cycle ----
        cycle_dt = datetime.strptime(upcoming_cycle_str, "%Y%m%d")
        days_left = (cycle_dt - datetime.now()).days

        deletion_dir = os.path.join(tmp_path, f"scheduled_deletion_{upcoming_cycle_str}")
        scheduled_file_list = os.path.join(deletion_dir, f"files_scheduled_{upcoming_cycle_str}.txt")
        exclusion_file_list = os.path.join(deletion_dir, f"exclusions_{upcoming_cycle_str}.txt")

        if days_left <= 0:
            # ====== DELETION DAY ======
            scheduled_set = read_file_list(scheduled_file_list)
            dynamic_exclusions = read_file_list(exclusion_file_list)
            temp_exclusions    = read_file_list(temp_exclusion_file)
            # permanent_exclusions never made it to scheduled_set anyway (skipped at scan)

            # Final exclusions = dynamic + temp
            final_exclusions = dynamic_exclusions.union(temp_exclusions)

            # Filter
            final_list = [p for p in scheduled_set if not is_path_excluded(p, final_exclusions)]

            # Perform deletion
            local_final_deletion_list = os.path.join(deletion_dir, f"final_deletion_list_{upcoming_cycle_str}.txt")

            if args.mode == "hdfs":
                # Copy to hdfs, run spark job
                hdfs_final_deletion_list = f"hdfs:///tmp/final_deletion_list_{upcoming_cycle_str}.txt"
                hdfs_deletion_workflow(config, final_list, local_final_deletion_list, hdfs_final_deletion_list)
            else:
                # local
                local_deletion_workflow(final_list, local_final_deletion_list)

            # Completion email
            subject = f"Deletion Completed for {upcoming_cycle_str} ({args.mode})"
            body    = f"Deletion completed for files scheduled on {upcoming_cycle_str}.\n"
            recipients = config["email"]["to"]
            cc         = config["email"].get("cc")
            send_email(subject, body, recipients, cc=cc, config=config)
            print("Deletion complete.")
        
        else:
            # ====== REMINDER ======
            subject = f"Reminder: {days_left} day(s) left for Deletion ({upcoming_cycle_str}, {args.mode})"
            body    = (
                f"There are {days_left} day(s) left until scheduled deletion on {upcoming_cycle_str}.\n"
                "Update the dynamic or temp exclusion if needed.\n"
            )
            recipients = config["email"]["to"]
            cc         = config["email"].get("cc")
            send_email(subject, body, recipients, cc=cc, config=config)
            print(f"Reminder email sent. {days_left} day(s) until deletion.")


if __name__ == "__main__":
    main()
