import time
import requests
from utils import update_job_status, save_input_file, save_result_file

def process_job(job_id: str, file_url: str):
    try:
        update_job_status(job_id, "processing")
        response = requests.get(file_url)
        input_path = save_input_file(job_id, response.content)
        time.sleep(2)  # Simulate processing
        result_data = b"Processed: " + response.content  # Dummy output
        save_result_file(job_id, result_data)
        update_job_status(job_id, "completed")
    except Exception as e:
        update_job_status(job_id, f"failed: {str(e)}")