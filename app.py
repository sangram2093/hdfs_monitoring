from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from flask_cors import CORS
import yaml
import os
import subprocess
import zipfile
import io
import requests
from bs4 import BeautifulSoup

app = Flask(__name__)
app.secret_key = "some-random-secret-key"
CORS(app)

CONFIG_PATH = "config.yaml"

def load_config():
    if not os.path.exists(CONFIG_PATH):
        return {}
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def save_config(cfg):
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False)

# We'll load config once at startup. 
# If you expect real-time changes, you might re-load for certain operations.
config = load_config()

@app.route("/")
def index():
    """
    Render a page with multiple tabs for:
      1. Temp Exclusions
      2. Versioned Directories
      3. Path Search
      4. File Download
      5. Local FS Quota
      6. HDFS Quota
    """
    # We'll gather local usage data for /apps/trade-srv, /tmp, /opt
    local_fs_usage = get_local_fs_usage(config.get("local_mount_points", []))
    
    # We'll gather HDFS usage data by scraping (if we can)
    hdfs_usage_data = get_hdfs_usage_data(config.get("hdfs_usage_url"), config.get("hdfs_quota_location"))

    return render_template("index.html",
                           config=config,
                           local_fs_usage=local_fs_usage,
                           hdfs_usage_data=hdfs_usage_data)

@app.route("/add_temp_exclusions", methods=["POST"])
def add_temp_exclusions():
    """
    Append lines to the relevant temp exclusion file: hdfs or local
    depending on user's selection in the form.
    """
    lines = request.form.get("exclusionLines", "").strip().splitlines()
    mode = request.form.get("mode")  # "hdfs" or "local"
    
    if not lines or not mode:
        flash("Missing lines or mode.", "error")
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
    """
    Append lines to the relevant versioned_dirs file (hdfs or local).
    """
    lines = request.form.get("versionedLines", "").strip().splitlines()
    mode = request.form.get("mode")  # "hdfs" or "local"

    if not lines or not mode:
        flash("Missing lines or mode.", "error")
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
    """
    Search for a path in the chosen file (temp or versioned, hdfs or local).
    """
    search_term = request.form.get("searchTerm", "").strip()
    file_type   = request.form.get("fileType")    # "temp" or "versioned"
    mode        = request.form.get("mode")        # "hdfs" or "local"
    
    if not search_term or not file_type or not mode:
        flash("Missing searchTerm, fileType, or mode.", "error")
        return redirect(url_for("index"))

    # Decide which file to search
    if file_type == "temp":
        if mode == "hdfs":
            file_path = config.get("temp_exclusion_file_hdfs")
        else:
            file_path = config.get("temp_exclusion_file_local")
    else:
        # versioned
        if mode == "hdfs":
            file_path = config.get("versioned_dirs_hdfs")
        else:
            file_path = config.get("versioned_dirs_local")

    if not file_path or not os.path.exists(file_path):
        flash(f"File not found: {file_path}", "error")
        return redirect(url_for("index"))

    found = False
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip() == search_term:
                found = True
                break
    
    if found:
        flash(f"'{search_term}' found in {file_path}.", "success")
    else:
        flash(f"'{search_term}' NOT found in {file_path}.", "info")
    
    return redirect(url_for("index"))

@app.route("/download_file", methods=["GET"])
def download_file():
    """
    Provide a download link that, given some query parameters, 
    zips the target file and returns it.
    e.g. /download_file?fileType=temp&mode=local
    """
    file_type = request.args.get("fileType")  # "temp" or "versioned"
    mode      = request.args.get("mode")      # "hdfs" or "local"

    if not file_type or not mode:
        flash("Missing fileType or mode.", "error")
        return redirect(url_for("index"))
    
    # Decide which actual file
    if file_type == "temp":
        fp = config["temp_exclusion_file_hdfs"] if mode=="hdfs" else config["temp_exclusion_file_local"]
    else:
        # versioned
        fp = config["versioned_dirs_hdfs"] if mode=="hdfs" else config["versioned_dirs_local"]
    
    if not os.path.exists(fp):
        flash(f"File does not exist: {fp}", "error")
        return redirect(url_for("index"))

    # We'll zip it in-memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(fp, arcname=os.path.basename(fp))
    zip_buffer.seek(0)

    # Return as file download
    zip_filename = os.path.basename(fp) + ".zip"
    return send_file(
        zip_buffer,
        mimetype='application/zip',
        as_attachment=True,
        download_name=zip_filename
    )

# ------------------ Utility Functions for Disk & HDFS Usage ------------------

def get_local_fs_usage(mount_points):
    """
    Return usage info for each mount in a dict.
    We'll call `df -h <mount>` and parse the output.
    """
    usage = []
    for mp in mount_points:
        if not os.path.exists(mp):
            usage.append((mp, "Not found", "", "", ""))
            continue
        try:
            # We'll run `df -h` for the mount
            cmd = ["df", "-h", mp]
            output = subprocess.check_output(cmd).decode("utf-8", errors="replace").splitlines()
            # output might look like:
            # Filesystem Size Used Avail Use% Mounted on
            # /dev/sda1  30G  25G  5G   83% /apps/trade-srv
            if len(output) >= 2:
                headers = output[0].split()
                values  = output[1].split()
                # Typically something like: [Filesystem, Size, Used, Avail, Use%, Mounted on]
                # We'll store (Size, Used, Avail, Use%)
                usage.append((mp, values[1], values[2], values[3], values[4]))
            else:
                usage.append((mp, "N/A", "N/A", "N/A", "N/A"))
        except Exception as e:
            usage.append((mp, f"Error: {e}", "", "", ""))
    return usage

def get_hdfs_usage_data(url, target_location):
    """
    Scrape the webpage (like the user’s example) to find 
    usage row for 'target_location'. Return 
    (location, ns_quota, ns_used, ns_pct, space_quota, space_used, space_pct)
    or None if not found.
    """
    if not url or not target_location:
        return None
    
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.content, 'html.parser')
        table = soup.find(name='table', attrs={'align': 'CENTER', 'width': '99%'})
        if not table:
            return None
        
        target_row = None
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if cells and cells[0].text.strip() == target_location:
                target_row = [c.text.strip() for c in cells]
                break
        
        if not target_row:
            return None
        
        # Suppose the row is:
        # [ /project/stadgcsv,
        #   200000, 100000, 97.63,
        #   16384.00, 2880.57, 17.58 ]
        # We'll just return it as a tuple or dict
        return {
            "location": target_row[0],
            "ns_quota": target_row[1],
            "ns_used": target_row[2],
            "ns_pct": target_row[3],
            "space_quota": target_row[4],
            "space_used": target_row[5],
            "space_pct": target_row[6],
        }
    except Exception as e:
        print(f"Error fetching HDFS usage: {e}")
        return None

if __name__ == "__main__":
    # For dev only
    app.run(host="0.0.0.0", port=5000, debug=True)
