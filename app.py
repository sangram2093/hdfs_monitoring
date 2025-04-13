import os
import io
import re
import yaml
import zipfile
import subprocess
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from flask_cors import CORS

app = Flask(__name__)
app.secret_key = "some-random-secret-key"
CORS(app)

CONFIG_PATH = "config.yaml"

DATE_PATTERN = re.compile(r"^scheduled_deletion_(\d{8})$")

def load_config():
    if not os.path.exists(CONFIG_PATH):
        return {}
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def save_config(cfg):
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False)

config = load_config()

# ------------------ SCHEDULING LOGIC: FIND UPCOMING CYCLE ------------------
def get_upcoming_cycle(tmp_path):
    """
    Looks for directories named 'scheduled_deletion_YYYYmmdd' in tmp_path,
    returns the earliest date >= today, or None if none found.
    """
    if not os.path.exists(tmp_path):
        return None

    today = datetime.now().date()
    upcoming_date_str = None

    for entry in os.listdir(tmp_path):
        match = DATE_PATTERN.match(entry)
        if match:
            cycle_date_str = match.group(1)  # e.g. "20250415"
            try:
                cycle_date = datetime.strptime(cycle_date_str, "%Y%m%d").date()
                if cycle_date >= today:
                    if (upcoming_date_str is None) or (cycle_date < datetime.strptime(upcoming_date_str, "%Y%m%d").date()):
                        upcoming_date_str = cycle_date_str
            except ValueError:
                pass
    return upcoming_date_str

# ------------------ HDFS QUOTA LOGIC (hdfs dfs -count -q) ------------------

def get_hdfs_quota_usage(path):
    """
    Executes: hdfs dfs -count -q <path> to get:
        ns_quota ns_remaining space_quota space_used dir_count file_count unknown location
    Example:
      200000 4726 17598478645673 14499787842345 52697 142577 1560987564123 /project/stadgcsv
    We'll parse numeric fields, compute usage, and return a dict.
    """
    try:
        output = subprocess.check_output(["hdfs", "dfs", "-count", "-q", path],
                                         universal_newlines=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running 'hdfs dfs -count -q': {e}")
        return None

    line = output.strip().split()
    if len(line) < 8:
        print(f"Unexpected output from hdfs dfs -count for {path}: {line}")
        return None

    ns_quota_str, ns_remain_str, space_quota_str, space_used_str, \
    dir_count_str, file_count_str, unknown_str, location_str = line

    try:
        ns_quota = int(ns_quota_str)
        ns_remaining = int(ns_remain_str)
        space_quota = int(space_quota_str)
        space_used = int(space_used_str)
    except ValueError as e:
        print(f"Error parsing numeric fields for {path}: {e}")
        return None

    ns_used = ns_quota - ns_remaining
    ns_usage_pct = (ns_used / ns_quota) * 100 if ns_quota else 0

    # Convert bytes to TB
    TB_IN_BYTES = 1024 ** 4
    space_quota_tb = space_quota / TB_IN_BYTES
    space_used_tb = space_used / TB_IN_BYTES
    space_usage_pct = (space_used / space_quota) * 100 if space_quota else 0

    return {
        "location": location_str,
        "ns_quota": ns_quota,
        "ns_used": ns_used,
        "ns_pct": round(ns_usage_pct, 2),
        "space_quota_tb": round(space_quota_tb, 2),
        "space_used_tb": round(space_used_tb, 2),
        "space_pct": round(space_usage_pct, 2),
    }

def get_all_hdfs_quotas(paths):
    """
    For each path in 'paths', call get_hdfs_quota_usage and collect results in a list.
    Return a list of dicts (or empty if none).
    Each dict has keys: location, ns_quota, ns_used, ns_pct, space_quota_tb, space_used_tb, space_pct
    or None if error.
    """
    results = []
    for p in paths:
        data = get_hdfs_quota_usage(p)
        if data:
            results.append(data)
        else:
            results.append({
                "location": p,
                "ns_quota": 0,
                "ns_used": 0,
                "ns_pct": 0,
                "space_quota_tb": 0,
                "space_used_tb": 0,
                "space_pct": 0,
                "error": True
            })
    return results

# ------------------ LOCAL FS USAGE (df -h) ------------------

def get_local_fs_usage(mount_points):
    """
    Return usage info for each mount as a list of tuples:
      (mount_point, size, used, avail, use%)
    from 'df -h <mount>'.
    """
    usage = []
    for mp in mount_points:
        if not os.path.exists(mp):
            usage.append((mp, "Not found", "", "", ""))
            continue
        try:
            cmd = ["df", "-h", mp]
            output = subprocess.check_output(cmd).decode("utf-8", errors="replace").splitlines()
            if len(output) >= 2:
                # second line has usage details
                values = output[1].split()
                # Typically: Filesystem, Size, Used, Avail, Use%, Mount
                usage.append((mp, values[1], values[2], values[3], values[4]))
            else:
                usage.append((mp, "N/A", "N/A", "N/A", "N/A"))
        except Exception as e:
            usage.append((mp, f"Error: {e}", "", "", ""))
    return usage

# ------------------ FLASK ROUTES ------------------

@app.route("/")
def index():
    """
    Home page:
      1) Display next upcoming deletion date for HDFS and local.
      2) Display HDFS usage in tabular form (for multiple paths from config).
      3) Display local FS usage in table.
      4) Provide links to download scheduled file if it exists.
      5) Provide nav tabs to other functionalities (temp, versioned, search, etc.).
    """
    # Next upcoming HDFS cycle
    tmp_path_hdfs = config.get("temp_storage_path_hdfs")
    upcoming_date_hdfs = get_upcoming_cycle(tmp_path_hdfs) if tmp_path_hdfs else None

    # Next upcoming Local cycle
    tmp_path_local = config.get("temp_storage_path_local")
    upcoming_date_local = get_upcoming_cycle(tmp_path_local) if tmp_path_local else None

    # 1) HDFS usage for multiple paths
    hdfs_paths = config.get("hdfs_quota_paths", [])
    hdfs_usage_list = get_all_hdfs_quotas(hdfs_paths)

    # 2) Local FS usage
    mount_points = config.get("local_mount_points", [])
    local_fs_usage = get_local_fs_usage(mount_points)

    return render_template(
        "index.html",
        upcoming_date_hdfs=upcoming_date_hdfs,
        upcoming_date_local=upcoming_date_local,
        hdfs_usage_list=hdfs_usage_list,
        local_fs_usage=local_fs_usage
    )

@app.route("/download_scheduled_file", methods=["GET"])
def download_scheduled_file():
    """
    Download the "actual deletion" file (files_scheduled_<date>.txt) for next upcoming cycle,
    as a zipped file. We accept ?mode=hdfs or ?mode=local
    """
    mode = request.args.get("mode")
    if not mode:
        flash("Missing mode for scheduled file download.", "error")
        return redirect(url_for("index"))

    if mode == "hdfs":
        tmp_path = config.get("temp_storage_path_hdfs")
    else:
        tmp_path = config.get("temp_storage_path_local")

    if not tmp_path:
        flash(f"No tmp path found in config for {mode}.", "error")
        return redirect(url_for("index"))

    # Find the upcoming date
    cycle_date_str = get_upcoming_cycle(tmp_path)
    if not cycle_date_str:
        flash(f"No upcoming cycle found for {mode}.", "error")
        return redirect(url_for("index"))

    scheduled_dir = os.path.join(tmp_path, f"scheduled_deletion_{cycle_date_str}")
    scheduled_file = os.path.join(scheduled_dir, f"files_scheduled_{cycle_date_str}.txt")

    if not os.path.exists(scheduled_file):
        flash(f"Scheduled file does not exist: {scheduled_file}", "error")
        return redirect(url_for("index"))

    # Zip in-memory
    import io
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(scheduled_file, arcname=os.path.basename(scheduled_file))
    zip_buffer.seek(0)

    zip_filename = f"files_scheduled_{cycle_date_str}_{mode}.zip"
    return send_file(
        zip_buffer,
        mimetype='application/zip',
        as_attachment=True,
        download_name=zip_filename
    )

# ----- Existing routes for Tab functionalities -----

@app.route("/add_temp_exclusions", methods=["POST"])
def add_temp_exclusions():
    """Append lines to temp_exclusion_file_(hdfs/local)."""
    lines = request.form.get("exclusionLines", "").strip().splitlines()
    mode = request.form.get("mode", "")

    if not lines or not mode:
        flash("Missing lines or mode for temp exclusions.", "error")
        return redirect(url_for("index"))

    if mode == "hdfs":
        temp_file = config.get("temp_exclusion_file_hdfs")
    else:
        temp_file = config.get("temp_exclusion_file_local")

    if not temp_file:
        flash(f"temp_exclusion_file_{mode} not set in config.", "error")
        return redirect(url_for("index"))
    
    try:
        with open(temp_file, "a", encoding="utf-8") as f:
            for line in lines:
                l = line.strip()
                if l:
                    f.write(l + "\n")
        flash(f"Appended lines to {temp_file}.", "success")
    except Exception as e:
        flash(f"Error appending lines: {e}", "error")

    return redirect(url_for("index"))

@app.route("/add_versioned_directories", methods=["POST"])
def add_versioned_directories():
    """Append lines to versioned_dirs_(hdfs/local)."""
    lines = request.form.get("versionedLines", "").strip().splitlines()
    mode = request.form.get("mode", "")

    if not lines or not mode:
        flash("Missing lines or mode for versioned dirs.", "error")
        return redirect(url_for("index"))

    if mode == "hdfs":
        versioned_file = config.get("versioned_dirs_hdfs")
    else:
        versioned_file = config.get("versioned_dirs_local")

    if not versioned_file:
        flash(f"versioned_dirs_{mode} not set in config.", "error")
        return redirect(url_for("index"))
    
    try:
        with open(versioned_file, "a", encoding="utf-8") as f:
            for line in lines:
                l = line.strip()
                if l:
                    f.write(l + "\n")
        flash(f"Appended versioned dirs to {versioned_file}.", "success")
    except Exception as e:
        flash(f"Error appending versioned directories: {e}", "error")

    return redirect(url_for("index"))

@app.route("/search_path", methods=["POST"])
def search_path():
    """Search for exact match line in either temp or versioned file for hdfs/local."""
    search_term = request.form.get("searchTerm", "").strip()
    file_type   = request.form.get("fileType")   # "temp" or "versioned"
    mode        = request.form.get("mode")       # "hdfs" or "local"

    if not search_term or not file_type or not mode:
        flash("Missing searchTerm, fileType, or mode.", "error")
        return redirect(url_for("index"))

    if file_type == "temp":
        fp = config["temp_exclusion_file_hdfs"] if mode=="hdfs" else config["temp_exclusion_file_local"]
    else:
        fp = config["versioned_dirs_hdfs"] if mode=="hdfs" else config["versioned_dirs_local"]

    if not os.path.exists(fp):
        flash(f"File not found: {fp}", "error")
        return redirect(url_for("index"))

    found = False
    with open(fp, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip() == search_term:
                found = True
                break
    
    if found:
        flash(f"'{search_term}' found in {fp}.", "success")
    else:
        flash(f"'{search_term}' NOT found in {fp}.", "info")
    
    return redirect(url_for("index"))

@app.route("/download_file", methods=["GET"])
def download_file():
    """
    Zip the chosen file (temp or versioned, hdfs or local) in-memory
    and prompt user to download it.
    e.g. /download_file?fileType=temp&mode=local
    """
    file_type = request.args.get("fileType")
    mode      = request.args.get("mode")

    if not file_type or not mode:
        flash("Missing fileType or mode for file download.", "error")
        return redirect(url_for("index"))
    
    if file_type == "temp":
        fp = config["temp_exclusion_file_hdfs"] if mode=="hdfs" else config["temp_exclusion_file_local"]
    else:
        # versioned
        fp = config["versioned_dirs_hdfs"] if mode=="hdfs" else config["versioned_dirs_local"]

    if not os.path.exists(fp):
        flash(f"File does not exist: {fp}", "error")
        return redirect(url_for("index"))

    # Zip in-memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(fp, arcname=os.path.basename(fp))
    zip_buffer.seek(0)

    zip_filename = os.path.basename(fp) + ".zip"
    return send_file(
        zip_buffer,
        mimetype='application/zip',
        as_attachment=True,
        download_name=zip_filename
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
