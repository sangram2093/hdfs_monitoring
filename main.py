import os
import re
import yaml
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

CONFIG_FILE = "/path/to/config.yaml"

def load_config(config_file=CONFIG_FILE):
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
            f"attachment; filename={os.path.basename(attachment)}",
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

def find_old_files(hdfs_base_path, threshold_days):
    """
    Use the HDFS CLI to list files older than threshold_days within hdfs_base_path
    and return as a Python list.
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

def write_file_list(file_paths, target_file):
    """
    Write file paths to a text file.
    """
    with open(target_file, 'w') as f:
        for p in file_paths:
            f.write(p + "\n")

def read_file_list(file_path):
    """
    Read file paths from a text file. Return as a set for quick membership checks.
    """
    if not os.path.exists(file_path):
        return set()
    with open(file_path, 'r') as f:
        lines = f.read().splitlines()
    return set(line.strip() for line in lines if line.strip())

def run_scala_deletion_job(config, deletion_list_path):
    """
    Calls the Spark Scala job to delete files using the provided jar/class.
    For a production flow, spark-submit is recommended.
    """
    scala_jar = "/path/to/HdfsDeletion.jar" 
    main_class = "HdfsDeletion"
    
    cmd = [
        "spark-submit",
        "--class", main_class,
        "--conf", f"spark.cleanup.inputPath={deletion_list_path}",
        scala_jar
    ]
    
    print(f"Running Scala deletion job with command: {' '.join(cmd)}")
    try:
        subprocess.check_call(cmd)
        print("Scala deletion job completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running Scala job: {e}")

def get_upcoming_cycle(tmp_path):
    """
    Scan temp_storage_path for directories named 'scheduled_deletion_YYYYmmdd'.
    Return the earliest date string >= today's date, or None if none is found.
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

def normalize_path(p: str) -> str:
    """
    Returns a normalized path string without trailing slash (except if it's root '/').
    Example:
      /user/sangram/ -> /user/sangram
      /user/sangram  -> /user/sangram
      / -> /
    """
    if p == "/":
        return "/"
    return p.rstrip("/")

def is_path_excluded(path: str, exclusion_list: set) -> bool:
    """
    Returns True if the given path is in or *under* any entry from exclusion_list.
    We interpret any entry that is a directory path to exclude all sub-items.
    
    For each exclusion E in the set:
      - If path == E, exclude
      - If path starts with E + "/", exclude
    """
    norm_path = normalize_path(path)

    for excl in exclusion_list:
        norm_excl = normalize_path(excl)
        # If exactly matches
        if norm_path == norm_excl:
            return True
        # If path starts with 'excl + /' ... exclude
        # e.g. path "/user/sangram/sample.txt" starts with "/user/sangram/" 
        # if norm_excl = "/user/sangram"
        if norm_excl != "/" and norm_path.startswith(norm_excl + "/"):
            return True
        
        # Special case: if the exclusion is "/" then everything is excluded
        # but presumably you never want that unless you want to exclude the entire cluster.
        if norm_excl == "/" and norm_path.startswith("/"):
            return True
    
    return False

def main():
    config = load_config()
    threshold_days = config["threshold_days"]
    base_path      = config["hdfs_base_path"]
    tmp_path       = config["temp_storage_path"]

    # A permanent exclusion file that developers cannot edit
    permanent_exclusion_file = config.get("permanent_exclusion_file")

    # 1) Check if there's any existing scheduled_deletion_YYYYmmdd directory
    #    that is >= today's date. If yes, we use that cycle. If not, create new cycle.
    upcoming_cycle_str = get_upcoming_cycle(tmp_path)

    if upcoming_cycle_str:
        # There's an existing future cycle
        cycle_dt = datetime.strptime(upcoming_cycle_str, "%Y%m%d")
        days_left = (cycle_dt - datetime.now()).days
        
        # Directories and files for that cycle
        deletion_dir = os.path.join(tmp_path, f"scheduled_deletion_{upcoming_cycle_str}")
        scheduled_file_list = os.path.join(deletion_dir, f"files_scheduled_{upcoming_cycle_str}.txt")
        exclusion_file_list = os.path.join(deletion_dir, f"exclusions_{upcoming_cycle_str}.txt")
        
        if days_left <= 0:
            # Deletion Day
            print(f"Deletion day arrived for cycle {upcoming_cycle_str}. Running Scala-based deletion job.")
            
            scheduled_set = read_file_list(scheduled_file_list)
            # 1) read dynamic per-cycle exclusions
            exclusion_set = read_file_list(exclusion_file_list)
            
            # 2) read permanent exclusions
            permanent_exclusion_set = read_file_list(permanent_exclusion_file) if permanent_exclusion_file else set()

            # 3) union them
            total_exclusion_set = exclusion_set.union(permanent_exclusion_set)

            # 4) final list = scheduled - total_exclusion
            final_list = [f for f in scheduled_set if not is_path_excluded(f, total_exclusions)]

            final_deletion_list_path = os.path.join(deletion_dir, f"final_deletion_list_{upcoming_cycle_str}.txt")
            write_file_list(final_list, final_deletion_list_path)

            hdfs_final_deletion_list = f"hdfs:///tmp/final_deletion_list_{upcoming_cycle_str}.txt"
            
            # Use -f to overwrite if it exists
            try:
                subprocess.check_call([
                    "hdfs", "dfs", "-put", "-f", final_deletion_list_path, hdfs_final_deletion_list
                ])
                print(f"Copied final deletion list to: {hdfs_final_deletion_list}")
            except subprocess.CalledProcessError as e:
                print(f"Error copying file to HDFS: {e}")
                return 

            # Run Scala job
            run_scala_deletion_job(config, final_deletion_list_path)

            # Optional: completion email
            completion_subject = f"HDFS Deletion Completed for {upcoming_cycle_str}"
            completion_body = (
                f"Deletion completed for files scheduled on {upcoming_cycle_str}.\n"
                f"Excluded files remain.\n"
            )
            send_email(
                completion_subject,
                completion_body,
                config["email"]["to"],
                cc=config["email"].get("cc"),
                config=config
            )
            print("Deletion complete.")
        
        else:
            # Not yet Deletion Day => send reminder
            print(f"There are {days_left} day(s) left until the scheduled deletion on {upcoming_cycle_str}.")
            reminder_subject = f"Reminder: {days_left} day(s) left for HDFS Deletion ({upcoming_cycle_str})"
            reminder_body = (
                f"There are {days_left} day(s) left until the scheduled HDFS deletion ({upcoming_cycle_str}).\n"
                "If you need to exclude any files, please update the exclusion list.\n"
            )
            send_email(
                reminder_subject,
                reminder_body,
                config["email"]["to"],
                cc=config["email"].get("cc"),
                config=config
            )
            print("Reminder email sent.")
    
    else:
        # No future cycle => create a new one for (today + 4 days)
        new_cycle_dt = datetime.now() + timedelta(days=4)
        new_cycle_str = new_cycle_dt.strftime("%Y%m%d")
        deletion_dir = os.path.join(tmp_path, f"scheduled_deletion_{new_cycle_str}")
        os.makedirs(deletion_dir, exist_ok=True)

        scheduled_file_list = os.path.join(deletion_dir, f"files_scheduled_{new_cycle_str}.txt")

        old_files = find_old_files(base_path, threshold_days)
        write_file_list(old_files, scheduled_file_list)

        # Send initial mail
        subject = config["email"]["subject"]
        body = config["email"]["body"]
        send_email(
            subject,
            body,
            config["email"]["to"],
            cc=config["email"].get("cc"),
            attachment=scheduled_file_list,
            config=config
        )
        print(f"New cycle started for {new_cycle_str}. List emailed.")


if __name__ == "__main__":
    main()
