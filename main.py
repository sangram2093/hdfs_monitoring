import os
import yaml
import subprocess
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta

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
        
        # Merge 'to' and 'cc' for final recipients
        final_recipients = recipients + (cc if cc else [])
        server.sendmail(
            from_addr=config["email"]["from"],
            to_addrs=final_recipients,
            msg=msg.as_string()
        )

def find_old_files(hdfs_base_path, threshold_days):
    """
    Use the HDFS CLI to list files older than threshold_days within hdfs_base_path
    and return as a Python list. This is a simple approach using `hdfs dfs -ls -R`.
    """
    from datetime import datetime, timedelta
    
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
        
        # parse date/time from parts[5], parts[6], e.g. "2022-10-01" "12:00"
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
    Write a list of file paths to a text file.
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
    Calls the Spark Scala job to delete files using the provided `HdfsDeletion.scala` logic.
    Provide the path to the text file containing final list of files.
    Example spark-submit command:
        spark-submit --class HdfsDeletion \
                     --conf spark.cleanup.inputPath=<deletion_list_path> \
                     /path/to/CompiledJar.jar
    """
    # Adjust these as per your environment:
    scala_jar = "/path/to/HdfsDeletion.jar"   # The jar after you compile HdfsDeletion.scala
    main_class = "HdfsDeletion"               # The object with main()
    
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

def main():
    config = load_config()
    
    threshold_days = config["threshold_days"]
    base_path      = config["hdfs_base_path"]
    tmp_path       = config["temp_storage_path"]

    # Determine the "deletion date" (4 days from now)
    deletion_date_str = (datetime.now() + timedelta(days=4)).strftime("%Y%m%d")

    # Directory & file naming
    deletion_dir = os.path.join(tmp_path, f"scheduled_deletion_{deletion_date_str}")
    scheduled_file_list = os.path.join(deletion_dir, f"files_scheduled_{deletion_date_str}.txt")
    exclusion_file_list = os.path.join(deletion_dir, f"exclusions_{deletion_date_str}.txt")

    # Create the directory if not present
    os.makedirs(deletion_dir, exist_ok=True)

    # Check if we have an existing schedule file for the upcoming deletion date
    if not os.path.exists(scheduled_file_list):
        # No existing file => New cycle
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
        print("New cycle started. List emailed.")
    else:
        # We are mid-cycle; check how many days remain
        deletion_date_dt = datetime.strptime(deletion_date_str, "%Y%m%d")
        days_left = (deletion_date_dt - datetime.now()).days

        if days_left <= 0:
            # Deletion Day => combine scheduled list & exclude list; run the Scala job
            print("Deletion day arrived. Running Scala-based deletion job.")
            
            scheduled_set = read_file_list(scheduled_file_list)
            exclusion_set = read_file_list(exclusion_file_list)
            
            # Final list to delete
            final_list = [f for f in scheduled_set if f not in exclusion_set]
            final_deletion_list_path = os.path.join(deletion_dir, f"final_deletion_list_{deletion_date_str}.txt")
            write_file_list(final_list, final_deletion_list_path)
            
            # Run the Scala job to delete them
            run_scala_deletion_job(config, final_deletion_list_path)

            # Optional: send completion email
            completion_subject = f"HDFS Deletion Completed for {deletion_date_str}"
            completion_body = (
                f"Deletion completed for files scheduled on {deletion_date_str}.\n"
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
            # Not deletion day yet => send reminder
            reminder_subject = f"Reminder: {days_left} day(s) left for HDFS Deletion"
            reminder_body = (
                f"There are {days_left} day(s) left until scheduled HDFS deletion.\n"
                f"If you need to exclude any files, please update the exclusion list.\n"
            )
            send_email(
                reminder_subject,
                reminder_body,
                config["email"]["to"],
                cc=config["email"].get("cc"),
                config=config
            )
            print(f"Reminder email sent. {days_left} day(s) until deletion.")

if __name__ == "__main__":
    main()
