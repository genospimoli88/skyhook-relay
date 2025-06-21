
import json
import time
import redis
import traceback
from utils import process_weather_job, notify_webhook

r = redis.Redis(
    host='redis-15078.c278.us-east-1-4.ec2.redns.redis-cloud.com',
    port=15078,
    password='your_redis_password_here',
    decode_responses=True
)

print("Worker started and listening for jobs...")

while True:
    try:
        job_data = r.lpop("job_queue")
        if job_data is None:
            time.sleep(1)
            continue

        try:
            job = json.loads(job_data)
        except json.JSONDecodeError:
            print("❌ Failed to decode job data:", job_data)
            continue

        if not isinstance(job, dict) or "job_type" not in job:
            print("❌ Invalid job format or missing 'job_type':", job)
            continue

        job_type = job["job_type"]
        webhook_url = job.get("webhook_url")

        print(f"🚀 Processing job: {job_type}")

        if job_type == "weather":
            result = process_weather_job(job)
        else:
            print(f"❌ Unknown job type: {job_type}")
            continue

        if webhook_url:
            notify_webhook(webhook_url, result)
        else:
            print("ℹ️ No webhook URL provided. Skipping notification.")

        print(f"✅ Job processed successfully: {job_type}")

    except Exception as e:
        print("❌ Unexpected error during job processing:")
        traceback.print_exc()
        time.sleep(1)
