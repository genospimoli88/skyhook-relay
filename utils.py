import os
import redis

redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
r = redis.Redis.from_url(redis_url)

BASE_DIR = "/mnt/data/jobs"
os.makedirs(BASE_DIR, exist_ok=True)

def update_job_status(job_id, status):
    r.set(f"job_status:{job_id}", status)

def get_job_status(job_id):
    status = r.get(f"job_status:{job_id}")
    return status.decode() if status else "unknown"

def save_input_file(job_id, data):
    input_path = os.path.join(BASE_DIR, f"{job_id}_input")
    with open(input_path, "wb") as f:
        f.write(data)
    return input_path

def save_result_file(job_id, data):
    result_path = os.path.join(BASE_DIR, f"{job_id}_result")
    with open(result_path, "wb") as f:
        f.write(data)
    return result_path

def get_result_file_path(job_id):
    return os.path.join(BASE_DIR, f"{job_id}_result")