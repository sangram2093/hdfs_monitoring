#!/usr/bin/env python3

import os
import re
import sys
import yaml
import argparse
import subprocess
import smtplib
import ssl
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Regex to detect directories named "scheduled_deletion_YYYYmmdd"
DATE_PATTERN = re.compile(r"^scheduled_deletion_(\d{8})$")


# ========================================================================
#                           CONFIG & EMAIL UTILS
# ========================================================================

def load_config(config_file):
    """
    Loads YAML config from the specified path.
    """
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)


def send_email(subject, body, recipients, cc=None, attachment=None, config=None):
    """
    Sends an email with optional attachment using SMTP server details from config.
    """
    if config is None:
        raise ValueError("SMTP config required to send email.")

    msg = MIMEMultipart()
    msg["From"] = config["email"]["from"]
    msg["To"]   = ", ".join(recipients)
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg["Subject"] = subject
    
    # Body
    msg.attach(MIMEText(body, "plain"))

    # Attachment
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
    
    # Connect and send
    smtp_server = config["email"]["host"]
    port = config["email"]["port"]
    username = config["email"]["username"]
    password = config["email"]["password"]
    
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


# ========================================================================
#                           EXCLUSION & PATH UTILS
# ========================================================================

def normalize_path(p: str) -> str:
    """
    Removes trailing slash except if p is '/'.
    """
    if p == "/":
        return "/"
    return p.rstrip("/")

def is_path_excluded(path: str, exclusion_list: set) -> bool:
    """
    Returns True if 'path' should be excluded because:
      - path == excl, OR
      - path starts with (excl + "/"), for directory-based exclusions
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
    Reads file paths from a text file into a set.
    """
    if not os.path.exists(file_path):
        return set()
    with open(file_path, 'r') as f:
        lines = f.read().splitlines()
    return set(line.strip() for line in lines if line.strip())

def write_file_list(file_paths, target_file):
    """
    Writes file paths to a text file.
    """
    with open(target_file, 'w') as f:
        for p in file_paths:
            f.write(p + "\n")


def get_upcoming_cycle(tmp_path):
    """
    Scan tmp_path for directories named 'scheduled_deletion_YYYYmmdd'.
    Return the earliest date >= today, or None if none found.
    """
    today = datetime.now().date()
    upcoming_date_str = None
    for entry in os.listdir(tmp_path):
        match = DATE_PATTERN.match(entry)
        if match:
            cycle_date_str = match.group(1)  # e.g. '20250414'
            try:
                cycle_date = datetime.strptime(cycle_date_str, "%Y%m%d").date()
                if cycle_date >= today:
                    if (upcoming_date_str is None) or (cycle_date < datetime.strptime(upcoming_date_str, "%Y%m%d").date()):
                        upcoming_date_str = cycle_date_str
            except ValueError:
                pass
    return upcoming_date_str


# ========================================================================
#                           HDFS MODE LOGIC
# ========================================================================

def find_old_files_hdfs(hdfs_base_path, threshold_days):
    """
    Use the HDFS CLI to list files older than threshold_days.
    We'll parse 'hdfs dfs -ls -R'.
    """
    threshold_date = datetime.now() - timedelta(days=threshold_days)
    cmd = ["hdfs", "dfs", "-ls", "-R", hdfs_base_path]
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8")
    except subprocess.CalledProcessError as e:
        print("Error running HDFS list command:", e.output.decode("utf-8"))
        return []
    
    old_files = []
    for line in output.splitlines():
        parts = line.strip().split()
        if len(parts) < 8:
            continue
        date_str = parts[5]
        time_str = parts[6]
        path_str = parts[7]
        
        try:
            file_datetime = datetime.strptime(date_str + " " + time_str, "%Y-%m-%d %H:%M")
        except ValueError:
            continue
        
        if file_datetime < threshold_date:
            old_files.append(path_str)
    
    return old_files

def run_scala_deletion_job(config, hdfs_final_deletion_list):
    """
    Calls spark-submit for the Scala-based HDFS deletion job.
    e.g.:
    spark-submit --class HdfsDeletion --conf spark.cleanup.inputPath=<hdfs_final_deletion_list> HdfsDeletion.jar
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
    1) Write final_list to local file
    2) Check if HDFS final file exists, remove it
    3) Copy local file to HDFS
    4) run spark-submit
    """
    # 1) Write local
    write_file_list(final_list, local_final_path)

    # 2) If HDFS file exists, remove
    try:
        subprocess.check_call(["hdfs", "dfs", "-test", "-e", hdfs_final_deletion_list])
        print(f"HDFS file '{hdfs_final_deletion_list}' already exists. Removing it first...")
        subprocess.check_call(["hdfs", "dfs", "-rm", "-skipTrash", hdfs_final_deletion_list])
    except subprocess.CalledProcessError:
        print(f"No existing file at '{hdfs_final_deletion_list}' to remove.")

    # 3) Copy to HDFS
    try:
        subprocess.check_call(["hdfs", "dfs", "-put", local_final_path, hdfs_final_deletion_list])
        print(f"Copied final list to HDFS: {hdfs_final_deletion_list}")
    except subprocess.CalledProcessError as e:
        print(f"Error copying file to HDFS: {e}")
        return

    # 4) Spark job
    run_scala_deletion_job(config, hdfs_final_deletion_list)


# ========================================================================
#                           LOCAL MODE LOGIC
# ========================================================================

def find_old_files_local(local_base_path, threshold_days):
    """
    Recursively find files older than threshold_days in the local filesystem.
    Uses last modification time for comparison.
    """
    cutoff_time = datetime.now().timestamp() - (threshold_days * 24 * 3600)
    old_files = []

    for root, dirs, files in os.walk(local_base_path):
        for fname in files:
            full_path = os.path.join(root, fname)
            try:
                st = os.stat(full_path)
                if st.st_mtime < cutoff_time:
                    old_files.append(full_path)
            except Exception as e:
                print(f"Error accessing {full_path}: {e}")
                continue
    
    return old_files

def delete_files_local(final_list):
    """
    Delete the given list of files from local FS.
    For directories, remove recursively if you choose. 
    But we've only identified old files above, so likely no directories in final_list.
    """
    for path in final_list:
        try:
            if os.path.isdir(path):
                # If you want to remove entire directories, do:
                subprocess.check_call(["rm", "-rf", path])
                print(f"Directory deleted: {path}")
            else:
                os.remove(path)
                print(f"File deleted: {path}")
        except Exception as e:
            print(f"Error deleting {path}: {e}")

def local_deletion_workflow(final_list, local_final_path):
    """
    1) Write final_list for record
    2) Delete them from local filesystem
    """
    write_file_list(final_list, local_final_path)
    delete_files_local(final_list)


# ========================================================================
#                           MAIN SCHEDULING LOGIC
# ========================================================================

def main():
    # 1) Parse arguments
    parser = argparse.ArgumentParser(description="Cleanup script for HDFS or Local filesystem.")
    parser.add_argument("--config", default="/path/to/config.yaml", help="Path to YAML config.")
    parser.add_argument("--mode", required=True, choices=["hdfs", "local"],
                        help="Which mode to run: 'hdfs' or 'local'.")
    args = parser.parse_args()

    # 2) Load config
    config = load_config(args.config)

    # 3) Determine threshold_days and base_path from the config, based on mode
    if args.mode == "hdfs":
        threshold_days = config["threshold_days_hdfs"]
        base_path      = config["hdfs_base_path"]
    else:  # local
        threshold_days = config["threshold_days_local"]
        base_path      = config["local_base_path"]

    tmp_path = config["temp_storage_path"]
    permanent_exclusion_file = config.get("permanent_exclusion_file")

    # 4) Check if there's an upcoming cycle
    upcoming_cycle_str = get_upcoming_cycle(tmp_path)
    if not upcoming_cycle_str:
        # No future cycle => create new
        new_cycle_dt = datetime.now() + timedelta(days=4)
        new_cycle_str = new_cycle_dt.strftime("%Y%m%d")
        deletion_dir = os.path.join(tmp_path, f"scheduled_deletion_{new_cycle_str}")
        os.makedirs(deletion_dir, exist_ok=True)

        scheduled_file_list = os.path.join(deletion_dir, f"files_scheduled_{new_cycle_str}.txt")

        # => find old files depending on mode
        if args.mode == "hdfs":
            old_files = find_old_files_hdfs(base_path, threshold_days)
        else:
            old_files = find_old_files_local(base_path, threshold_days)

        # write them
        write_file_list(old_files, scheduled_file_list)

        # send initial email (attachment = scheduled_file_list)
        subject = config["email"]["subject"]
        body = config["email"]["body"]
        recipients = config["email"]["to"]
        cc = config["email"].get("cc")

        send_email(subject, body, recipients, cc=cc, attachment=scheduled_file_list, config=config)
        print(f"New cycle started for {new_cycle_str} in mode: {args.mode}. List emailed.")

    else:
        # We have a future cycle
        cycle_dt = datetime.strptime(upcoming_cycle_str, "%Y%m%d")
        days_left = (cycle_dt - datetime.now()).days

        deletion_dir = os.path.join(tmp_path, f"scheduled_deletion_{upcoming_cycle_str}")
        scheduled_file_list = os.path.join(deletion_dir, f"files_scheduled_{upcoming_cycle_str}.txt")
        exclusion_file_list = os.path.join(deletion_dir, f"exclusions_{upcoming_cycle_str}.txt")

        if days_left <= 0:
            # DELETION DAY
            scheduled_set = read_file_list(scheduled_file_list)
            dynamic_exclusions = read_file_list(exclusion_file_list)
            perm_exclusions = read_file_list(permanent_exclusion_file) if permanent_exclusion_file else set()

            # Combine
            total_exclusion = dynamic_exclusions.union(perm_exclusions)
            # Build final list
            final_list = [p for p in scheduled_set if not is_path_excluded(p, total_exclusion)]

            local_final_deletion_list = os.path.join(deletion_dir, f"final_deletion_list_{upcoming_cycle_str}.txt")

            # HDFS or local deletion
            if args.mode == "hdfs":
                hdfs_final_deletion_list = f"hdfs:///tmp/final_deletion_list_{upcoming_cycle_str}.txt"
                hdfs_deletion_workflow(config, final_list, local_final_deletion_list, hdfs_final_deletion_list)
            else:
                local_deletion_workflow(final_list, local_final_deletion_list)

            # Optionally send completion email
            completion_subject = f"Deletion Completed for {upcoming_cycle_str} ({args.mode})"
            completion_body = (
                f"Deletion completed for files scheduled on {upcoming_cycle_str}.\n"
                f"Excluded files remain.\n"
            )
            recipients = config["email"]["to"]
            cc = config["email"].get("cc")
            send_email(completion_subject, completion_body, recipients, cc=cc, config=config)

            print("Deletion complete.")

        else:
            # REMINDER
            reminder_subject = f"Reminder: {days_left} day(s) left for Deletion ({upcoming_cycle_str}, {args.mode})"
            reminder_body = (
                f"There are {days_left} day(s) left until scheduled deletion on {upcoming_cycle_str}.\n"
                "If you need to exclude any files, please update the exclusion list.\n"
            )
            recipients = config["email"]["to"]
            cc = config["email"].get("cc")
            send_email(reminder_subject, reminder_body, recipients, cc=cc, config=config)
            print(f"Reminder email sent. {days_left} day(s) until deletion.")

if __name__ == "__main__":
    main()
