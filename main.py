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
    Sends an email with optional attachment (as a zip or any file) using SMTP server details from config.
    """
    if config is None:
        raise ValueError("SMTP config required to send email.")

    msg = MIMEMultipart()
    msg["From"] = config["email"]["from"]
    msg["To"]   = ", ".join(recipients)
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg["Subject"] = subject
    
    # Email Body
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
    Removes trailing slash except if p == '/'.
    """
    if p == "/":
        return "/"
    return p.rstrip("/")


def is_path_excluded(path: str, exclusion_list: set) -> bool:
    """
    Returns True if 'path' should be excluded because:
      - exactly matches an exclusion entry,
      - or is under an excluded directory.
    Example:
      If '/user/sangram' is excluded, then
      '/user/sangram/sample.txt' is also excluded.
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
    Uses 'replace' to avoid surrogate encoding errors.
    """
    if not os.path.exists(file_path):
        return set()
    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.read().splitlines()
    return set(line.strip() for line in lines if line.strip())


def write_file_list(file_paths, target_file):
    """
    Writes file paths to a text file, handling surrogates gracefully.
    """
    with open(target_file, 'w', encoding='utf-8', errors='replace') as f:
        for p in file_paths:
            f.write(p + "\n")


def get_upcoming_cycle(tmp_path):
    """
    Scan tmp_path for directories named 'scheduled_deletion_YYYYmmdd'.
    Return the earliest date >= today, or None if none found.
    """
    today = datetime.now().date()
    upcoming_date_str = None

    if not os.path.exists(tmp_path):
        return None

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
    Zips the given file and returns the path to the created .zip.
    e.g. if original_file = '/path/to/my.txt', we create '/path/to/my.txt.zip'
    """
    zip_path = original_file + ".zip"
    import zipfile
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(original_file, arcname=os.path.basename(original_file))
    return zip_path


# ========================================================================
#                           HDFS MODE LOGIC
# ========================================================================

def find_old_files_hdfs(hdfs_base_path, threshold_days, perm_exclusions):
    """
    Use 'hdfs dfs -ls -R' to list all paths. 
    Skip permanent_exclusions so they're never scheduled. 
    Keep only those older than threshold_days.
    """
    threshold_date = datetime.now() - timedelta(days=threshold_days)
    cmd = ["hdfs", "dfs", "-ls", "-R", hdfs_base_path]
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8", errors="replace")
    except subprocess.CalledProcessError as e:
        print("Error running HDFS list command:", e.output.decode("utf-8", errors="replace"))
        return []

    old_files = []
    for line in output.splitlines():
        parts = line.strip().split()
        if len(parts) < 8:
            continue
        date_str = parts[5]
        time_str = parts[6]
        path_str = parts[7]

        # Skip if permanently excluded
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
    Calls spark-submit for the Scala-based HDFS deletion job.
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
    2) If the HDFS file exists, remove it
    3) put local file to HDFS
    4) run spark-submit
    """
    # 1) Write local
    write_file_list(final_list, local_final_path)

    # 2) Remove old version in HDFS if it exists
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

    # 4) spark-submit
    run_scala_deletion_job(config, hdfs_final_deletion_list)


# ========================================================================
#                           LOCAL MODE LOGIC
# ========================================================================

def find_old_files_local(local_base_path, threshold_days, perm_exclusions):
    """
    Recursively find files older than threshold_days on local filesystem,
    skipping anything in perm_exclusions so they're never scheduled.
    """
    cutoff_time = datetime.now().timestamp() - (threshold_days * 86400)
    old_files = []

    for root, dirs, files in os.walk(local_base_path):
        # If root is permanently excluded, skip entire sub-tree
        if is_path_excluded(root, perm_exclusions):
            dirs[:] = []  # no recursion into subfolders
            continue

        for fname in files:
            full_path = os.path.join(root, fname)

            # skip if permanently excluded
            if is_path_excluded(full_path, perm_exclusions):
                continue

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
    Delete the given list of paths from local filesystem.
    """
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
    1) Write final_list for record
    2) Remove them locally
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
    parser.add_argument("--mode", required=True, choices=["hdfs", "local"], help="Mode: 'hdfs' or 'local'.")
    args = parser.parse_args()

    # 2) Load config
    config = load_config(args.config)

    # 3) Depending on mode, pick relevant paths & threshold
    if args.mode == "hdfs":
        threshold_days = config["threshold_days_hdfs"]
        base_path      = config["hdfs_base_path"]
        tmp_path       = config["temp_storage_path_hdfs"]

        permanent_exclusion_file = config["permanent_exclusion_file_hdfs"]
        temp_exclusion_file      = config["temp_exclusion_file_hdfs"]

    else:
        threshold_days = config["threshold_days_local"]
        base_path      = config["local_base_path"]
        tmp_path       = config["temp_storage_path_local"]

        permanent_exclusion_file = config["permanent_exclusion_file_local"]
        temp_exclusion_file      = config["temp_exclusion_file_local"]

    # Read permanent exclusions (admin-managed)
    permanent_exclusions = read_file_list(permanent_exclusion_file)

    # 4) Check if there's an upcoming cycle
    upcoming_cycle_str = get_upcoming_cycle(tmp_path)

    if not upcoming_cycle_str:
        # ---------- Create a new cycle for (today + 4 days) ----------
        new_cycle_dt = datetime.now() + timedelta(days=4)
        new_cycle_str = new_cycle_dt.strftime("%Y%m%d")

        deletion_dir = os.path.join(tmp_path, f"scheduled_deletion_{new_cycle_str}")
        os.makedirs(deletion_dir, exist_ok=True)

        scheduled_file_list = os.path.join(deletion_dir, f"files_scheduled_{new_cycle_str}.txt")

        # => find old files, skipping permanent exclusions at scan time
        if args.mode == "hdfs":
            old_files = find_old_files_hdfs(base_path, threshold_days, permanent_exclusions)
        else:
            old_files = find_old_files_local(base_path, threshold_days, permanent_exclusions)

        # Write them
        write_file_list(old_files, scheduled_file_list)

        # Optionally zip the scheduled file if you want to email a zip
        # Or skip zipping if you prefer raw text
        zipped_path = scheduled_file_list + ".zip"
        with zipfile.ZipFile(zipped_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(scheduled_file_list, arcname=os.path.basename(scheduled_file_list))

        # Send initial mail with the .zip attached
        subject    = config["email"]["subject"]
        body       = config["email"]["body"]
        recipients = config["email"]["to"]
        cc         = config["email"].get("cc")

        send_email(
            subject,
            body,
            recipients,
            cc=cc,
            attachment=zipped_path,
            config=config
        )
        print(f"New cycle started for {new_cycle_str} in mode: {args.mode}. List emailed (zipped).")

    else:
        # ---------- We have a future cycle ----------
        cycle_dt = datetime.strptime(upcoming_cycle_str, "%Y%m%d")
        days_left = (cycle_dt - datetime.now()).days

        deletion_dir = os.path.join(tmp_path, f"scheduled_deletion_{upcoming_cycle_str}")
        scheduled_file_list = os.path.join(deletion_dir, f"files_scheduled_{upcoming_cycle_str}.txt")
        exclusion_file_list = os.path.join(deletion_dir, f"exclusions_{upcoming_cycle_str}.txt")

        if days_left <= 0:
            # ========== DELETION DAY ==========
            scheduled_set = read_file_list(scheduled_file_list)

            # "Dynamic" cycle-based exclusions:
            dynamic_exclusions = read_file_list(exclusion_file_list)

            # "Temp" user exclusions (for next cycle only), also read at deletion time:
            temp_exclusions = read_file_list(temp_exclusion_file)

            # permanent_exclusions were already removed at scanning,
            # so we don't re-check them here. They never made it to scheduled_set.

            # Final exclusion set = dynamic + temp
            final_exclusions = dynamic_exclusions.union(temp_exclusions)

            # Build final list to delete
            final_list = [p for p in scheduled_set if not is_path_excluded(p, final_exclusions)]

            local_final_deletion_list = os.path.join(deletion_dir, f"final_deletion_list_{upcoming_cycle_str}.txt")

            if args.mode == "hdfs":
                hdfs_final_deletion_list = f"hdfs:///tmp/final_deletion_list_{upcoming_cycle_str}.txt"
                hdfs_deletion_workflow(config, final_list, local_final_deletion_list, hdfs_final_deletion_list)
            else:
                local_deletion_workflow(final_list, local_final_deletion_list)

            # Optional final email
            completion_subject = f"Deletion Completed for {upcoming_cycle_str} ({args.mode})"
            completion_body = (
                f"Deletion completed for files scheduled on {upcoming_cycle_str}.\n"
                "Excluded files remain.\n"
            )
            recipients = config["email"]["to"]
            cc         = config["email"].get("cc")

            send_email(
                completion_subject,
                completion_body,
                recipients,
                cc=cc,
                config=config
            )
            print("Deletion complete.")

        else:
            # ========== REMINDER ==========
            reminder_subject = f"Reminder: {days_left} day(s) left for Deletion ({upcoming_cycle_str}, {args.mode})"
            reminder_body = (
                f"There are {days_left} day(s) left until scheduled deletion on {upcoming_cycle_str}.\n"
                "If you need to exclude any files for this cycle, please update the dynamic per-cycle file or "
                "the temp exclusion file.\n"
            )
            recipients = config["email"]["to"]
            cc         = config["email"].get("cc")

            send_email(
                reminder_subject,
                reminder_body,
                recipients,
                cc=cc,
                config=config
            )
            print(f"Reminder email sent. {days_left} day(s) until deletion.")


if __name__ == "__main__":
    main()
