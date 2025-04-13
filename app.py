from flask import Flask, request, render_template, redirect, url_for, flash
from flask_cors import CORS
import yaml
import os

app = Flask(__name__)
app.secret_key = "some-secret-key"  # needed for flash messages
CORS(app)  # enable CORS

# Load config on startup
CONFIG_PATH = "config.yaml"

def load_config():
    if not os.path.exists(CONFIG_PATH):
        return {}
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def save_config(cfg):
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False)

config = load_config()

@app.route("/")
def index():
    """
    Render a main page with forms to:
    1) Append paths to temp exclusion file
    2) Subscribe email
    3) Unsubscribe email
    4) Add versioned directory
    """
    # We'll read the 'to' list from config
    # for demonstration in the UI
    email_list = config.get("email", {}).get("to", [])
    return render_template("index.html", email_list=email_list)

@app.route("/add_temp_exclusions", methods=["POST"])
def add_temp_exclusions():
    """
    Append user-provided lines (paths) to temp_exclusion_file_local.
    Can handle multiple lines, one per line.
    """
    if not request.form.get("exclusionLines"):
        flash("No exclusion lines provided.", "error")
        return redirect(url_for("index"))
    
    lines = request.form.get("exclusionLines").strip().splitlines()
    
    # Identify the file from config
    temp_exclusion_file_local = config.get("temp_exclusion_file_local")
    if not temp_exclusion_file_local:
        flash("temp_exclusion_file_local not set in config.yaml.", "error")
        return redirect(url_for("index"))
    
    # Append to the file
    try:
        with open(temp_exclusion_file_local, "a", encoding="utf-8") as f:
            for line in lines:
                line = line.strip()
                if line:
                    f.write(line + "\n")
        flash("Temp exclusions added successfully.", "success")
    except Exception as e:
        flash(f"Error writing to temp exclusion file: {e}", "error")
    
    return redirect(url_for("index"))

@app.route("/subscribe_email", methods=["POST"])
def subscribe_email():
    """
    Add an email ID to the "to" list in config.yaml, if not already present.
    Then save config.
    """
    new_email = request.form.get("emailAddress", "").strip()
    if not new_email:
        flash("No email address provided.", "error")
        return redirect(url_for("index"))
    
    email_list = config.get("email", {}).get("to", [])
    if new_email in email_list:
        flash("Email already in the subscription list.", "info")
    else:
        email_list.append(new_email)
        # update in-memory config
        config["email"]["to"] = email_list
        # save to file
        save_config(config)
        flash(f"Email {new_email} subscribed successfully.", "success")
    
    return redirect(url_for("index"))

@app.route("/unsubscribe_email", methods=["POST"])
def unsubscribe_email():
    """
    Remove an email from the "to" list in config.yaml, if present.
    """
    remove_email = request.form.get("emailAddress", "").strip()
    if not remove_email:
        flash("No email address provided.", "error")
        return redirect(url_for("index"))
    
    email_list = config.get("email", {}).get("to", [])
    if remove_email not in email_list:
        flash("Email not found in subscription list.", "info")
    else:
        email_list.remove(remove_email)
        config["email"]["to"] = email_list
        save_config(config)
        flash(f"Email {remove_email} unsubscribed successfully.", "success")
    
    return redirect(url_for("index"))

@app.route("/add_versioned_directory", methods=["POST"])
def add_versioned_directory():
    """
    Append user-provided lines (paths) to versioned_dirs_local
    so the next local scan can remove old versions except the latest.
    """
    if not request.form.get("versionedLines"):
        flash("No versioned directory lines provided.", "error")
        return redirect(url_for("index"))
    
    lines = request.form.get("versionedLines").strip().splitlines()
    
    versioned_dirs_local = config.get("versioned_dirs_local")
    if not versioned_dirs_local:
        flash("versioned_dirs_local not set in config.yaml.", "error")
        return redirect(url_for("index"))
    
    try:
        with open(versioned_dirs_local, "a", encoding="utf-8") as f:
            for line in lines:
                line = line.strip()
                if line:
                    f.write(line + "\n")
        flash("Versioned directories added successfully.", "success")
    except Exception as e:
        flash(f"Error writing to versioned_dirs_local file: {e}", "error")
    
    return redirect(url_for("index"))

if __name__ == "__main__":
    # For demonstration
    # In production: run with a production server, e.g. gunicorn
    app.run(host="0.0.0.0", port=5000, debug=True)
