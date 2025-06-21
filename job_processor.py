
import os
import uuid
import shutil
from fastapi import UploadFile
from utils import save_uploaded_file, process_data_file
import requests

# Job storage paths
JOB_DIR = "jobs"
RESULTS_DIR = "results"

os.makedirs(JOB_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

def notify_webhook(job_id: str, result_url: str):
    webhook_url = "https://webhook.site/b8666cc9-b382-4786-b629-6c61f35fea48"
    payload = {
        "job_id": job_id,
        "status": "complete",
        "result_url": result_url
    }
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"[ERROR] Webhook notification failed: {e}")

def process_job(upload_file: UploadFile):
    job_id = str(uuid.uuid4())
    job_path = os.path.join(JOB_DIR, job_id)
    os.makedirs(job_path, exist_ok=True)

    file_path = os.path.join(job_path, upload_file.filename)
    save_uploaded_file(upload_file, file_path)

    # Process data and generate result
    result = process_data_file(file_path)

    result_file = os.path.join(RESULTS_DIR, f"{job_id}.txt")
    with open(result_file, "w") as f:
        f.write(result)

    # Notify user via webhook
    public_host = os.environ.get("PUBLIC_HOSTNAME", "yourdomain.com")
    download_url = f"https://{public_host}/v1/job/result/{job_id}"
    notify_webhook(job_id, download_url)

    return job_id
