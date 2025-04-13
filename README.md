# HDFS & Local Cleanup Framework

## Overview
This repository contains a framework to automate the cleanup of old files both in HDFS and on the local filesystem. It includes:
- A Python scheduling script (cron-based or manually run) that discovers, notifies, and deletes old files.
- A Flask web UI for managing temporary and versioned-directory exclusions.
- Integrated reporting of disk usage (local mount points) and HDFS quotas.

## Key Components
1. **`config.yaml`**  
   - Stores thresholds, paths, email settings, mount points, and other configurations.  
2. **Cleanup Scripts**  
   - A Python script (scheduling logic) that runs daily (or as needed) to:
     - Identify old files
     - Send notifications / reminders
     - Perform final deletions after a grace period
3. **Flask Web UI**  
   - A user-friendly interface to:
     - Add temporary exclusions (paths to skip)
     - Manage versioned directories (only keep the latest version, remove older ones)
     - Download or search specific files
     - View local and HDFS usage
4. **HDFS Quota Usage**  
   - Uses `hdfs dfs -count -q` to gather quota data for multiple HDFS paths, displayed in a tabular UI (with highlight if usage > 80%).

## Prerequisites
- **Python 3.7+**  
- **Flask**, **Flask-CORS**, **PyYAML**, **requests**, **bs4** (depending on usage)  
- **Hadoop CLI** tools installed (so `hdfs dfs` commands work).  
- **Local OS** should support `df -h` for disk usage.  


# 2. User/Developer Guide

Below is a **step-by-step** guide for end-users and developers on how to work with the Cleanup UI, handle the scheduled files, and manage exclusions.

---

## Introduction
The Cleanup Framework is designed to help remove old files and manage versioned directories both on HDFS and local servers. It automatically emails you about upcoming deletions and gives you a 4-day grace period to exclude specific items.

### Accessing the UI
- Go to [http://hostname:5000](http://hostname:5000) (replace `hostname` with your server name).
- You’ll land on the **Home** page, which shows:
  - Upcoming Deletion Dates for HDFS & Local
  - HDFS Quota usage (in a table)
  - Local Filesystem usage (in a table)
  - Links to download the next scheduled file if available

### What Happens on Deletion File Generation
1. **Initial Email**: You’ll receive an email with a list of old files scheduled for deletion in 4 days. This list is also accessible on the UI.  
2. **Grace Period**: You have 4 days to exclude any files you want to retain.

### Adding Temp Exclusions
1. On the home page, scroll down to the **Tabs**.  
2. Click on **“Temp Exclusions”** tab.  
3. Choose **“HDFS”** or **“LOCAL”** from the dropdown.  
4. Paste the file paths (one per line) you want to exclude.  
5. Click **“Add to Temp Exclusions”**.  
6. The paths are appended to a text file (either `temp_exclusion_file_hdfs` or `temp_exclusion_file_local`), which will be honored on Deletion Day.

### Adding Versioned Directories
1. On the home page, in the **“Versioned Directories”** tab, choose mode (HDFS or Local).  
2. Paste each directory path (one per line) that contains your versioned subdirectories.  
3. Click **“Add to Versioned Dirs”**.  
4. The system will only keep the **latest** subdirectory version (by comparing numeric version strings) and schedule older versions for deletion.

### Searching a Path
1. In the **“Path Search”** tab, select the mode (HDFS/Local) and file type (Temp or Versioned).  
2. Enter the **exact** path as your search term.  
3. If found, a confirmation message appears.

### Downloading a File (Temp/Versioned)
1. Go to the **“Download (Temp/Versioned)”** tab.  
2. Select the mode and the file type you want to download.  
3. It downloads a **.zip** containing that text file for reference or record.

### Viewing & Handling HDFS Quota
- The home page’s **HDFS Quota Usage** table shows each path from `hdfs_quota_paths`, plus the namespace & space usage.
- Cells are **highlighted in red** if usage > 80%. Consider archiving or cleaning data if usage is high.

### Final Deletion Day
- On Day 4, the script checks the final scheduled file and excludes anything found in the permanent or temp exclusions. 
- A completion email is sent once the deletion is done.

---

## FAQ
**Q: What if a path is missing from the UI?**  
A: Confirm your `config.yaml` is updated with the correct paths. Restart the app if needed.

**Q: Can I add multiple lines for versioned directories at once?**  
A: Yes, just put them on separate lines in the text area.

**Q: I see a usage column in red—what do I do?**  
A: That means you’re over 80% usage. Consider cleaning up or archiving data.

---

## Support
- For questions or issues, contact **YourTeam@company.com** or open a ticket on the internal help desk.

---

# 3. White Paper

Below is a **conceptual, business-oriented** explanation of your HDFS & Local Cleanup Framework, suitable for a management or broad technical audience:

---

## Title: “Unified HDFS & Local Filesystem Cleanup Framework”

### Abstract
As data accumulates across Hadoop Distributed File System (HDFS) and local server mounts, manual cleanups can become error-prone and time-consuming. This white paper introduces a **unified, automated approach** to manage old files and versioned directories, backed by a user-friendly UI, robust scheduling, and timely notifications.

### Table of Contents
1. **Introduction**  
2. **Challenges**  
3. **Solution Overview**  
4. **Key Benefits**  
5. **Architecture**  
6. **Conclusion**

---

## 1. Introduction
In a modern data platform, large volumes of historical and versioned files often accumulate. Over time, these can degrade performance, consume excessive storage, and complicate system management.

## 2. Challenges
1. **Data Sprawl**: Both HDFS clusters and local Linux filesystems face constant growth.  
2. **Version Proliferation**: Many teams keep multiple versions of binaries or data sets.  
3. **Inconsistent Cleanup**: Without automation, cleanups are sporadic, risking accidental deletions or wasted space.

## 3. Solution Overview
Our Cleanup Framework automates discovery and deletion of old files across HDFS and local mounts. A **Flask**-based UI allows easy user participation in the cleanup process. Key features include:

- **Scheduled Deletion** with a 4-day grace period  
- **Email Notifications** summarizing pending deletions and sending reminders  
- **Temp & Permanent Exclusion** for ensuring critical paths are never removed  
- **Versioned Directory Logic** that retains only the latest version  
- **HDFS Quota** & **Local Disk** usage reporting for proactive capacity monitoring

## 4. Key Benefits
- **Improved Efficiency**: Automatic identification and removal of stale files ensures consistent housekeeping.  
- **Reduced Risk**: The 4-day window and UI-based exclusions prevent unintentional data loss.  
- **Enhanced Visibility**: The user interface displays disk usage, upcoming deletions, and versioned directories.  
- **Scalable**: Manages both small departmental servers and large HDFS clusters in the same workflow.

## 5. Architecture

- **Web App**: Built in Python/Flask for interactive path exclusions and version directory management.  
- **Scheduling**: A Python script or cron job finds and deletes old files.  
- **Config**: YAML file that centralizes thresholds, email details, and usage parameters.

## 6. Conclusion
By consolidating HDFS and local filesystem cleanup in a single automated framework with a user-facing UI, organizations can maintain clean storage, reduce costs, and confidently manage file lifecycles with minimal overhead.

---

# 4. Introductory Email to Users

Below is a **professional announcement** email you can send to your user base, explaining the framework and your request for them to input their versioned directories.

---

**Subject**: Introducing Our New HDFS & Local Cleanup Framework—Action Required

**Hello Team,**

I hope you’re doing well. I’m pleased to announce the rollout of our **new HDFS & Local Cleanup Framework**, designed to keep our storage tidy and organized with minimal effort. Here’s what you need to know:

1. **What Is It?**  
   - A unified system that identifies and deletes older files, both in Hadoop (HDFS) and on our local Linux mounts, after a 4-day notification period.  
   - A simple web UI at [http://yourserver:5000](http://yourserver:5000) for adding path exclusions and designating versioned directories.

2. **Key Features**  
   - **Automated Deletions**: Files older than the configured threshold are scheduled for removal.  
   - **Version Management**: Only the latest version is retained in specified “versioned directories.”  
   - **UI for Exclusions**: You can easily exclude critical files from the next deletion cycle.  
   - **Usage Monitoring**: Check local disk usage and HDFS quotas in real time.

3. **What You Need To Do**  
   - **Visit the UI**: Access [http://yourserver:5000](http://yourserver:5000).  
   - **Enter Versioned Directories**: If you have directories containing multiple version folders (e.g. `1.0.1`, `1.0.2`), please add them under “Versioned Directories” in the UI.  
   - **Verify Critical Files**: If there are any paths you want to permanently or temporarily exclude, do so under “Temp Exclusions.”  

4. **Next Steps**  
   - We will **start the framework** via cron after everyone has had a chance to input their directories and examine the UI.  
   - You will begin receiving daily or weekly emails with a list of upcoming deletions.  
   - You have **4 days** from receipt of each email to add any new exclusions.

5. **Support**  
   - For questions or issues, contact **DataOps@yourcompany.com**.  
   - Detailed documentation is available in our internal wiki and at the UI “Help” tab.

We appreciate your cooperation in populating the versioned directories and ensuring all your critical files are accounted for. This new framework will help us maintain a more organized and efficient environment.

Thank you for your help in making this initiative a success.
