from flask import Flask, request, jsonify
import os
import yaml
from datetime import datetime, timedelta

app = Flask(__name__)
CONFIG_FILE = "/path/to/config.yaml"

def load_config(config_file=CONFIG_FILE):
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

@app.route("/exclude", methods=["POST"])
def exclude_files():
    """
    Expects a JSON body: { "files": ["/path/to/file1", "/path/to/dir2"] }
    Appends them to the relevant exclusion_<date>.txt for the next scheduled deletion date.
    """
    data = request.json
    if not data or "files" not in data:
        return jsonify({"error": "Missing 'files' in JSON body"}), 400

    config = load_config()
    tmp_path = config["temp_storage_path"]

    # Next deletion date is 4 days ahead
    deletion_date_str = (datetime.now() + timedelta(days=4)).strftime("%Y%m%d")
    deletion_dir = os.path.join(tmp_path, f"scheduled_deletion_{deletion_date_str}")
    os.makedirs(deletion_dir, exist_ok=True)
    
    exclusion_file_list = os.path.join(deletion_dir, f"exclusions_{deletion_date_str}.txt")

    with open(exclusion_file_list, "a") as f:
        for file_path in data["files"]:
            f.write(file_path.strip() + "\n")

    return jsonify({"message": "Files successfully added to exclusion list"}), 200

if __name__ == "__main__":
    # Run on a port accessible internally
    app.run(host="0.0.0.0", port=5000, debug=True)
