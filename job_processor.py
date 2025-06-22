import json
import time
import redis
import requests
import os
import logging
import traceback
import stripe
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("/mnt/data/jobs/worker.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

stripe.api_key = os.getenv("STRIPE_API_KEY")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
RUNPOD_ENDPOINT = os.getenv("RUNPOD_ENDPOINT", "https://skyhook.yourdomain.com")
try:
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5, socket_connect_timeout=5)
    r.ping()
    logger.info("Connected to Redis")
except redis.exceptions.RedisError as e:
    logger.error(f"Failed to connect to Redis: {e}")
    exit(1)

RESULTS_DIR = "/mnt/data/jobs"
try:
    os.makedirs(RESULTS_DIR, exist_ok=True)
    logger.info(f"Created results directory: {RESULTS_DIR}")
except OSError as e:
    logger.error(f"Failed to create results directory: {e}")
    exit(1)

def process_weather_job(job_data):
    """Process a weather job (mocked)."""
    job_id = job_data["job_id"]
    city = job_data.get("city", "Unknown")
    try:
        weather_data = {"city": city, "temperature": 20, "condition": "Sunny"}
        result_data = json.dumps(weather_data).encode()
        result_path = os.path.join(RESULTS_DIR, f"weather_{job_id}.json")
        with open(result_path, "wb") as f:
            f.write(result_data)
        result_url = f"{RUNPOD_ENDPOINT}/v1/job/result/{job_id}"
        logger.info(f"Saved weather result for job {job_id} to {result_path}")
        return {"result_url": result_url, "status": "completed"}
    except Exception as e:
        logger.error(f"Failed to process weather job {job_id}: {e}")
        raise

def process_transcription_job(job_data):
    """Process a transcription job (mocked)."""
    job_id = job_data["job_id"]
    audio_url = job_data.get("audio_url")
    try:
        transcription = {"text": "Mocked transcription from audio"}
        result_data = json.dumps(transcription).encode()
        result_path = os.path.join(RESULTS_DIR, f"transcription_{job_id}.json")
        with open(result_path, "wb") as f:
            f.write(result_data)
        result_url = f"{RUNPOD_ENDPOINT}/v1/job/result/{job_id}"
        logger.info(f"Saved transcription result for job {job_id} to {result_path}")
        return {"result_url": result_url, "status": "completed"}
    except Exception as e:
        logger.error(f"Failed to process transcription job {job_id}: {e}")
        raise

def process_ocr_job(job_data):
    """Process an OCR job (mocked)."""
    job_id = job_data["job_id"]
    image_url = job_data.get("image_url")
    try:
        ocr_result = {"text": "Mocked text from image"}
        result_data = json.dumps(ocr_result).encode()
        result_path = os.path.join(RESULTS_DIR, f"ocr_{job_id}.json")
        with open(result_path, "wb") as f:
            f.write(result_data)
        result_url = f"{RUNPOD_ENDPOINT}/v1/job/result/{job_id}"
        logger.info(f"Saved OCR result for job {job_id} to {result_path}")
        return {"result_url": result_url, "status": "completed"}
    except Exception as e:
        logger.error(f"Failed to process OCR job {job_id}: {e}")
        raise

def process_llm_job(job_data):
    """Process an LLM job (mocked)."""
    job_id = job_data["job_id"]
    prompt = job_data.get("prompt", "Default prompt")
    try:
        llm_result = {"response": f"Mocked LLM response to: {prompt}"}
        result_data = json.dumps(llm_result).encode()
        result_path = os.path.join(RESULTS_DIR, f"llm_{job_id}.json")
        with open(result_path, "wb") as f:
            f.write(result_data)
        result_url = f"{RUNPOD_ENDPOINT}/v1/job/result/{job_id}"
        logger.info(f"Saved LLM result for job {job_id} to {result_path}")
        return {"result_url": result_url, "status": "completed"}
    except Exception as e:
        logger.error(f"Failed to process LLM job {job_id}: {e}")
        raise

def update_job_status(job_id: str, status: str, result_url: str = None):
    """Update job status and result URL in Redis."""
    try:
        updates = {
            "status": status,
            "updated_at": datetime.utcnow().isoformat()
        }
        if result_url:
            updates["result_url"] = result_url
        r.hset(f"job:{job_id}", mapping=updates)
        logger.info(f"Updated job {job_id} status to {status}")
    except redis.exceptions.RedisError as e:
        logger.error(f"Failed to update job status for {job_id}: {e}")

def notify_webhook(webhook_url: str, payload: dict):
    """Send webhook notification."""
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info(f"Sent webhook to {webhook_url}: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Webhook failed for {webhook_url}: {e}")

def batch_charge_stripe(jobs):
    """Charge Stripe for a batch of jobs."""
    try:
        total_amount = sum(float(job.get("price", 0.10)) for job in jobs) * 100
        payment_tokens = list(set(job.get("payment_token") for job in jobs))
        if len(payment_tokens) > 1:
            logger.warning("Multiple payment tokens in batch; using first")
        customer = stripe.Customer.create(source=payment_tokens[0])
        charge = stripe.Charge.create(
            amount=int(total_amount),
            currency="usd",
            customer=customer.id,
            description=f"Batch charge for {len(jobs)} jobs"
        )
        logger.info(f"Batched charge for {len(jobs)} jobs: ${total_amount/100:.2f}, charge ID: {charge.id}")
    except stripe.error.StripeError as e:
        logger.error(f"Stripe batch charge failed: {e}")

logger.info("Worker started and listening for jobs...")

BATCH_INTERVAL = 5
while True:
    try:
        immediate_jobs = []
        while True:
            job_id = r.lpop("queue:immediate")
            if job_id is None:
                break
            job_data = r.hgetall(f"job:{job_id}")
            if job_data:
                immediate_jobs.append(job_data)
            else:
                logger.warning(f"Job data missing for immediate job {job_id}")
        for job_data in immediate_jobs:
            job_id = job_data["job_id"]
            job_type = job_data["job_type"]
            webhook_url = job_data["webhook_url"]
            price = float(job_data["price"])
            logger.info(f"Processing immediate job {job_id}: {job_type}")
            update_job_status(job_id, "processing")
            try:
                if job_type == "weather":
                    result = process_weather_job(job_data)
                elif job_type == "transcription":
                    result = process_transcription_job(job_data)
                elif job_type == "ocr":
                    result = process_ocr_job(job_data)
                elif job_type == "llm":
                    result = process_llm_job(job_data)
                else:
                    logger.error(f"Unknown job type: {job_type}")
                    update_job_status(job_id, "failed")
                    continue
                update_job_status(job_id, result["status"], result.get("result_url"))
                notify_webhook(webhook_url, {
                    "job_id": job_id,
                    "status": result["status"],
                    "result_url": result.get("result_url"),
                    "price": price
                })
                logger.info(f"Immediate job {job_id} processed successfully")
            except Exception as e:
                logger.error(f"Failed to process immediate job {job_id}: {e}")
                traceback.print_exc()
                update_job_status(job_id, "failed")
                notify_webhook(webhook_url, {"job_id": job_id, "status": "failed", "price": price})

        start_time = time.time()
        queued_jobs = []
        while time.time() - start_time < BATCH_INTERVAL:
            job_id = r.lpop("queue:queued")
            if job_id is None:
                time.sleep(0.1)
                continue
            job_data = r.hgetall(f"job:{job_id}")
            if job_data:
                queued_jobs.append(job_data)
            else:
                logger.warning(f"Job data missing for queued job {job_id}")
        if queued_jobs:
            logger.info(f"Processing batch of {len(queued_jobs)} queued jobs")
            batch_charge_stripe(queued_jobs)
            for job_data in queued_jobs:
                job_id = job_data["job_id"]
                job_type = job_data["job_type"]
                webhook_url = job_data["webhook_url"]
                price = float(job_data["price"])
                logger.info(f"Processing queued job {job_id}: {job_type}")
                update_job_status(job_id, "processing")
                try:
                    if job_type == "weather":
                        result = process_weather_job(job_data)
                    elif job_type == "transcription":
                        result = process_transcription_job(job_data)
                    elif job_type == "ocr":
                        result = process_ocr_job(job_data)
                    elif job_type == "llm":
                        result = process_llm_job(job_data)
                    else:
                        logger.error(f"Unknown job type: {job_type}")
                        update_job_status(job_id, "failed")
                        continue
                    update_job_status(job_id, result["status"], result.get("result_url"))
                    notify_webhook(webhook_url, {
                        "job_id": job_id,
                        "status": result["status"],
                        "result_url": result.get("result_url"),
                        "price": price
                    })
                    logger.info(f"Queued job {job_id} processed successfully")
                except Exception as e:
                    logger.error(f"Failed to process queued job {job_id}: {e}")
                    traceback.print_exc()
                    update_job_status(job_id, "failed")
                    notify_webhook(webhook_url, {"job_id": job_id, "status": "failed", "price": price})
        else:
            time.sleep(BATCH_INTERVAL - (time.time() - start_time))
    except redis.exceptions.RedisError as e:
        logger.error(f"Redis error during job processing: {e}")
        time.sleep(5)
    except Exception as e:
        logger.error(f"Unexpected error during job processing: {e}")
        traceback.print_exc()
        time.sleep(5)
