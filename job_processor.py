import json, time, redis, os, logging, requests
import utils

r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))
logger = logging.getLogger("skyhook")

def process_job(job_json):
    try:
        job = json.loads(job_json)
        job_id = job.get("id")
        job_type = job.get("type")
        data_url = job.get("data_url")
        callback_url = job.get("callback_url")
        logger.info(f"Processing job {job_id} of type {job_type}")
        time.sleep(2)
        result = {"job_id": job_id, "status": "complete", "result_url": f"https://example.com/fake/{job_id}"}
        content = json.dumps(result).encode("utf-8")
        s3_url = utils.upload_to_s3(content, f"{job_id}.json")
        requests.post(callback_url, json={"job_id": job_id, "status": "complete", "result_url": s3_url})
    except Exception as e:
        utils.send_alert(f"Job processing failed: {e}")
