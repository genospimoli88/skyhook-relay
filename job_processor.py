import os
import json
import requests
import logging
from datetime import datetime
import redis
import stripe
import time
import random
from math import exp
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from hashlib import sha256
from hmac import compare_digest
import aiohttp
from functools import wraps
import asyncio
import multiprocessing
from collections import deque

# Setup logging
LOG_DIR = os.getenv("LOG_DIR", "/mnt/data/jobs")
LOG_FILE = os.path.join(LOG_DIR, "worker.log")
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Setup FastAPI
app = FastAPI()

# Environment vars from RunPod secrets or environment
stripe.api_key = os.getenv("STRIPE_API_KEY")  # Secret from RunPod
REDIS_URL = os.getenv("REDIS_URL", "redis://your-redis-host:6379")
CLOUD_STORAGE_URL = os.getenv("CLOUD_STORAGE_URL")  # Required
ALT_CLOUD_STORAGE_URL = os.getenv("ALT_CLOUD_STORAGE_URL")  # Fallback cloud
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")  # For alerts
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")  # Secret from RunPod
PROVIDER_API_KEYS = {
    "runpod": os.getenv("RUNPOD_API_KEY"),
    "openrouter": os.getenv("OPENROUTER_API_KEY"),
    "together": os.getenv("TOGETHER_API_KEY")  # Secret from RunPod
}
INSTANT_MIN_FEE = float(os.getenv("INSTANT_MIN_FEE", 0.30))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))
BATCH_TIMEOUT = int(os.getenv("BATCH_TIMEOUT", 20))
JOB_TTL = int(os.getenv("JOB_TTL", 86400))
REQUESTS_PER_SECOND = float(os.getenv("REQUESTS_PER_SECOND", 10.0))
MIN_WORKERS = int(os.getenv("MIN_WORKERS", 1))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 10))
TARGET_JOBS_PER_WORKER = int(os.getenv("TARGET_JOBS_PER_WORKER", 1000))
DESIRED_MARGIN = float(os.getenv("DESIRED_MARGIN", 0.05))
DAILY_SPEND_CAP = float(os.getenv("DAILY_SPEND_CAP", 100.0))
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
PROVIDER_COSTS = {
    "openrouter": float(os.getenv("OPENROUTER_COST", 0.08)),
    "together": float(os.getenv("TOGETHER_COST", 0.08)),
    "runpod": float(os.getenv("RUNPOD_COST", 0.10))
}
RUNPOD_DISABLE_KEY = "runpod:disabled"
RUNPOD_USAGE_COUNT = "runpod:usage_count"
JOB_FAILURE_PATTERN = "job_failure:{reason}"

# Redis connection
r = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5)

# Whitelisted job types
ALLOWED_JOB_TYPES = {"weather", "text_classification", "instant"}

# Global profit metrics
PROFIT_METRICS = {"total_charged": 0.0, "total_spent": 0.0, "job_count": 0, "last_100_profit": deque(maxlen=100)}

# Throttle decorator
def throttle(func):
    last_call = {}
    @wraps(func)
    async def wrapper(*args, **kwargs):
        provider = kwargs.get('provider')
        if not provider:
            return await func(*args, **kwargs)
        last_call[provider] = last_call.get(provider, 0)
        min_interval = 1.0 / REQUESTS_PER_SECOND
        time_since_last = time.time() - last_call[provider]
        if time_since_last < min_interval:
            await asyncio.sleep(min_interval - time_since_last)
        result = await func(*args, **kwargs)
        last_call[provider] = time.time()
        return result
    return wrapper

# Rate limiting decorator
def rate_limit(max_requests=100, window_seconds=3600):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            client_ip = kwargs.get('client_ip', 'unknown')
            key = f"rate_limit:{client_ip}"
            count = r.get(key)
            if count is None:
                r.setex(key, window_seconds, 1)
            elif int(count) >= max_requests:
                raise HTTPException(status_code=429, detail="Too Many Requests")
            else:
                r.incr(key)
            return await func(*args, **kwargs)
        return wrapper
    return decorator

async def fetch_result_data(result_url):
    async with aiohttp.ClientSession() as session:
        async with session.get(result_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
            response.raise_for_status()
            return await response.read()

@throttle
async def dispatch_to_provider(provider, job_id, input_url):
    api_key = PROVIDER_API_KEYS.get(provider)
    if not api_key:
        logger.error(f"No API key for provider {provider}")
        return None, 0
    url = f"https://{provider}.com/api/v1/job"
    payload = {"job_id": job_id, "input_url": input_url}
    start_time = time.time()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers={"Authorization": f"Bearer {api_key}"}, timeout=10) as response:
                if response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", 1))
                    logger.warning(f"429 from {provider} for job {job_id}, retrying after {retry_after}s")
                    await asyncio.sleep(retry_after)
                    return await dispatch_to_provider(provider, job_id, input_url)
                response.raise_for_status()
                result_url = (await response.json()).get("result_url")
                latency = (time.time() - start_time) * 1000
                logger.info(f"Dispatched job {job_id} to {provider}, result URL: {result_url}")
                return result_url, latency
    except aiohttp.ClientError as e:
        logger.error(f"Dispatch failed for job {job_id} to {provider}: {e}")
        return None, 0

def upload_to_cloud(result_data, job_id):
    if not CLOUD_STORAGE_URL:
        logger.error(f"Missing CLOUD_STORAGE_URL for job {job_id}")
        return None
    try:
        if not result_data or len(result_data) == 0:
            raise ValueError("Invalid result data")
        for attempt in range(2):
            current_url = CLOUD_STORAGE_URL if attempt == 0 else ALT_CLOUD_STORAGE_URL
            if not current_url:
                continue
            response = requests.post(current_url, data=result_data, timeout=10)
            if response.status_code == 200:
                break
            if attempt == 0 and ALT_CLOUD_STORAGE_URL:
                logger.warning(f"Switching to alt cloud for job {job_id}")
            else:
                raise requests.exceptions.RequestException("Upload failed")
        response.raise_for_status()
        upload_url = response.json().get("url")
        if not upload_url or not requests.head(upload_url).ok:
            raise ValueError("Invalid or inaccessible upload URL")
        logger.info(f"Uploaded job {job_id} to {upload_url}")
        return upload_url
    except Exception as e:
        logger.error(f"Cloud upload failed for job {job_id} (attempt {attempt + 1}/2): {e}")
        r.incr(JOB_FAILURE_PATTERN.format(reason="upload_failed"))
        return None

def create_payment_intent(job_id, amount, batch=False, instant=False):
    try:
        intent = stripe.PaymentIntent.create(
            amount=int(amount * 100),
            currency="usd",
            customer=stripe.Customer.create(source="tok_visa").id,
            description=f"{'Instant ' if instant else 'Batch ' if batch else ''}intent for job {job_id}",
            metadata={"job_id": job_id},
            capture_method="manual"
        )
        logger.info(f"Created PaymentIntent for job {job_id}, intent ID: {intent.id}")
        return intent.id
    except stripe.error.StripeError as e:
        logger.error(f"PaymentIntent creation failed for job {job_id}: {e}")
        return None

def capture_payment_intent(payment_intent_id, amount):
    try:
        stripe.PaymentIntent.capture(payment_intent_id)
        PROFIT_METRICS["total_charged"] += amount
        r.hset("profit:metrics", mapping=PROFIT_METRICS)
        logger.info(f"Captured ${amount} for PaymentIntent {payment_intent_id}")
        return True
    except stripe.error.StripeError as e:
        logger.error(f"Payment capture failed for PaymentIntent {payment_intent_id}: {e}")
        return False

def cancel_payment_intent(payment_intent_id, job_id=None):
    try:
        stripe.PaymentIntent.cancel(payment_intent_id)
        if job_id:
            price = float(r.hget(f"job:{job_id}", "price_per_job") or 0)
            PROFIT_METRICS["total_charged"] -= price
            r.hset("profit:metrics", mapping=PROFIT_METRICS)
        logger.info(f"Cancelled PaymentIntent {payment_intent_id}")
        return True
    except stripe.error.StripeError as e:
        logger.error(f"PaymentIntent cancellation failed for {payment_intent_id}: {e}")
        return False

async def process_job(job_data, max_retries=1):
    job_id = job_data["job_id"]
    webhook_url = job_data["webhook_url"]
    price = float(job_data.get("price", 0.12))
    job_type = job_data["job_type"]
    client_ip = job_data.get("client_ip", "unknown")
    queue_key = f"queue:{'instant' if job_type == 'instant' else 'batch_' + job_type}"

    # Daily spend cap check
    daily_spent = float(r.get(f"spend:{client_ip}") or 0)
    if not TEST_MODE and daily_spent >= DAILY_SPEND_CAP:
        r.hset(f"job:{job_id}", mapping={"status": "rejected", "updated_at": datetime.utcnow().isoformat(), "reason": "daily_spend_cap_exceeded"})
        notify_webhook(webhook_url, {"job_id": job_id, "status": "rejected", "reason": "daily_spend_cap_exceeded"})
        r.incr(JOB_FAILURE_PATTERN.format(reason="daily_spend_cap_exceeded"))
        return

    # Input validation (basic hash check)
    async with aiohttp.ClientSession() as session:
        async with session.head(job_data["input_url"], timeout=10) as response:
            content_hash = response.headers.get("ETag", "")
            if r.exists(f"input_hash:{content_hash}"):
                r.hset(f"job:{job_id}", mapping={"status": "rejected", "updated_at": datetime.utcnow().isoformat(), "reason": "duplicate_input"})
                notify_webhook(webhook_url, {"job_id": job_id, "status": "rejected", "reason": "duplicate_input"})
                r.incr(JOB_FAILURE_PATTERN.format(reason="duplicate_input"))
                return

    # Profit margin check
    provider = {"weather": "openrouter", "text_classification": "openrouter", "summarization": "together", "instant": "openrouter"}[job_type]
    provider_cost = PROVIDER_COSTS.get(provider, 0.08)
    total_cost = provider_cost + (price * 0.029) + 0.30
    if not TEST_MODE and price <= total_cost + DESIRED_MARGIN:
        r.hset(f"job:{job_id}", mapping={"status": "rejected", "updated_at": datetime.utcnow().isoformat(), "reason": "insufficient_margin"})
        notify_webhook(webhook_url, {"job_id": job_id, "status": "rejected", "reason": "insufficient_margin"})
        r.incr(JOB_FAILURE_PATTERN.format(reason="insufficient_margin"))
        return

    if provider == "runpod" and r.get(RUNPOD_DISABLE_KEY):
        r.hset(f"job:{job_id}", mapping={"status": "rejected", "updated_at": datetime.utcnow().isoformat(), "reason": "runpod_disabled"})
        notify_webhook(webhook_url, {"job_id": job_id, "status": "rejected", "reason": "runpod_disabled"})
        r.incr(JOB_FAILURE_PATTERN.format(reason="runpod_disabled"))
        return

    payment_intent_id = create_payment_intent(job_id, price if job_type != "instant" else INSTANT_MIN_FEE, instant=(job_type == "instant"))
    if not payment_intent_id:
        r.hset(f"job:{job_id}", mapping={"status": "failed", "updated_at": datetime.utcnow().isoformat(), "reason": "payment_intent_failed"})
        notify_webhook(webhook_url, {"job_id": job_id, "status": "failed", "reason": "payment_intent_failed"})
        r.incr(JOB_FAILURE_PATTERN.format(reason="payment_intent_failed"))
        return

    start_time = time.time()
    for attempt in range(max_retries + 1):
        result_url, latency = await dispatch_to_provider(provider, job_id, job_data.get("input_url"))
        if result_url:
            result_data = await fetch_result_data(result_url)
            cloud_url = upload_to_cloud(result_data, job_id)
            if cloud_url:
                if not TEST_MODE:
                    if capture_payment_intent(payment_intent_id, price if job_type != "instant" else INSTANT_MIN_FEE):
                        r.hincrbyfloat(f"spend:{client_ip}", price)
                        PROFIT_METRICS["total_spent"] += provider_cost
                        PROFIT_METRICS["job_count"] += 1
                        profit = price - provider_cost
                        PROFIT_METRICS["last_100_profit"].append(profit)
                        if len(PROFIT_METRICS["last_100_profit"]) == 100 and sum(PROFIT_METRICS["last_100_profit"]) < 5.0 and ALERT_WEBHOOK_URL:
                            requests.post(ALERT_WEBHOOK_URL, json={"content": f"⚠️ Profits falling: ${sum(PROFIT_METRICS['last_100_profit']):.2f} over last 100 jobs"})
                    else:
                        cancel_payment_intent(payment_intent_id, job_id)
                        r.hset(f"job:{job_id}", mapping={"status": "failed", "updated_at": datetime.utcnow().isoformat(), "reason": "payment_capture_failed"})
                        notify_webhook(webhook_url, {"job_id": job_id, "status": "failed", "reason": "payment_capture_failed"})
                        r.incr(JOB_FAILURE_PATTERN.format(reason="payment_capture_failed"))
                        return
                break
            else:
                cancel_payment_intent(payment_intent_id, job_id)
                r.hset(f"job:{job_id}", mapping={"status": "failed", "updated_at": datetime.utcnow().isoformat(), "reason": "upload_failed", "refunded": "true"})
                notify_webhook(webhook_url, {"job_id": job_id, "status": "failed", "reason": "upload_failed", "refunded": "true"})
                r.incr(JOB_FAILURE_PATTERN.format(reason="upload_failed"))
                return
        if attempt < max_retries:
            wait_time = exp(attempt)
            logger.info(f"Retrying job {job_id}, attempt {attempt + 1}/{max_retries + 1}, waiting {wait_time:.2f}s")
            await asyncio.sleep(wait_time)
    else:
        cancel_payment_intent(payment_intent_id, job_id)
        r.hset(f"job:{job_id}", mapping={"status": "failed", "updated_at": datetime.utcnow().isoformat(), "reason": "dispatch_failed", "refunded": "true"})
        notify_webhook(webhook_url, {"job_id": job_id, "status": "failed", "reason": "dispatch_failed", "refunded": "true"})
        r.incr(JOB_FAILURE_PATTERN.format(reason="dispatch_failed"))
        return

    r.hset(f"job:{job_id}", mapping={
        "status": "completed",
        "result_url": cloud_url,
        "updated_at": datetime.utcnow().isoformat(),
        "payment_intent_id": payment_intent_id,
        "cost_per_job": provider_cost,
        "price_per_job": price,
        "profit_per_job": price - provider_cost,
        "latency_ms": int(latency)
    })
    r.expire(f"job:{job_id}", JOB_TTL)
    r.lrem(queue_key, 0, job_id)
    r.set(f"input_hash:{content_hash}", job_id)

    notify_webhook(webhook_url, {
        "job_id": job_id,
        "status": "completed",
        "download_url": cloud_url
    })
    logger.info(f"Job {job_id} processed, profit: ${price - provider_cost:.2f}, latency: {latency:.2f}ms")

async def process_batch(jobs):
    if not jobs or len(jobs) < BATCH_SIZE:
        return
    total_amount = sum(float(job.get("price", 0.12)) for job in jobs)
    client_ip = jobs[0].get("client_ip", "unknown")
    daily_spent = float(r.get(f"spend:{client_ip}") or 0)
    if not TEST_MODE and daily_spent + total_amount > DAILY_SPEND_CAP:
        for job in jobs:
            job_id = job["job_id"]
            r.hset(f"job:{job_id}", mapping={"status": "rejected", "updated_at": datetime.utcnow().isoformat(), "reason": "daily_spend_cap_exceeded"})
        return

    payment_intent_id = create_payment_intent(jobs[0]["job_id"], total_amount, batch=True)
    if not payment_intent_id:
        for job in jobs:
            job_id = job["job_id"]
            r.hset(f"job:{job_id}", mapping={"status": "failed", "updated_at": datetime.utcnow().isoformat(), "reason": "payment_intent_failed"})
        return

    total_cost = 0
    success_jobs = []
    start_time = time.time()
    for job in jobs:
        job_id = job["job_id"]
        job_type = job["job_type"]
        price = float(job.get("price", 0.12))
        provider = {"weather": "openrouter", "text_classification": "openrouter", "summarization": "together"}[job_type]
        provider_cost = PROVIDER_COSTS.get(provider, 0.08)
        total_cost_per_job = provider_cost + (price * 0.029) + 0.30
        if not TEST_MODE and price <= total_cost_per_job + DESIRED_MARGIN:
            r.hset(f"job:{job_id}", mapping={"status": "rejected", "updated_at": datetime.utcnow().isoformat(), "reason": "insufficient_margin"})
            continue
        if provider == "runpod" and r.get(RUNPOD_DISABLE_KEY):
            r.hset(f"job:{job_id}", mapping={"status": "rejected", "updated_at": datetime.utcnow().isoformat(), "reason": "runpod_disabled"})
            continue
        result_url, latency = await dispatch_to_provider(provider, job_id, job.get("input_url"))
        if result_url:
            result_data = await fetch_result_data(result_url)
            cloud_url = upload_to_cloud(result_data, job_id)
            if cloud_url:
                total_cost += provider_cost
                profit = price - provider_cost
                r.hset(f"job:{job_id}", mapping={
                    "status": "completed",
                    "result_url": cloud_url,
                    "updated_at": datetime.utcnow().isoformat(),
                    "cost_per_job": provider_cost,
                    "price_per_job": price,
                    "profit_per_job": profit,
                    "latency_ms": int(latency)
                })
                r.lpush("runpod:margins", profit if provider == "runpod" else 0)
                if r.llen("runpod:margins") > 10:
                    r.lpop("runpod:margins")
                success_jobs.append(job)
            else:
                r.hset(f"job:{job_id}", mapping={"status": "failed", "updated_at": datetime.utcnow().isoformat(), "reason": "upload_failed"})
        else:
            r.hset(f"job:{job_id}", mapping={"status": "failed", "updated_at": datetime.utcnow().isoformat(), "reason": "dispatch_failed"})

    total_profit = total_amount - total_cost
    if len(success_jobs) < len(jobs):
        refund_amount = sum(float(job.get("price", 0.12)) for job in jobs if job not in success_jobs)
        cancel_payment_intent(payment_intent_id)
        total_profit -= refund_amount
        r.incr(JOB_FAILURE_PATTERN.format(reason="batch_partial_fail"))

    if not TEST_MODE and len(success_jobs) > 0:
        capture_amount = total_amount - refund_amount
        if capture_payment_intent(payment_intent_id, capture_amount):
            r.hincrbyfloat(f"spend:{client_ip}", capture_amount)
            PROFIT_METRICS["total_spent"] += total_cost
            PROFIT_METRICS["job_count"] += len(jobs)
            for job in success_jobs:
                profit = float(job.get("price", 0.12)) - PROVIDER_COSTS.get({"weather": "openrouter", "text_classification": "openrouter", "summarization": "together"}[job["job_type"]], 0.08)
                PROFIT_METRICS["last_100_profit"].append(profit)
            if len(PROFIT_METRICS["last_100_profit"]) == 100 and sum(PROFIT_METRICS["last_100_profit"]) < 5.0 and ALERT_WEBHOOK_URL:
                requests.post(ALERT_WEBHOOK_URL, json={"content": f"⚠️ Profits falling: ${sum(PROFIT_METRICS['last_100_profit']):.2f} over last 100 jobs"})
        else:
            cancel_payment_intent(payment_intent_id)
            for job in jobs:
                job_id = job["job_id"]
                r.hset(f"job:{job_id}", mapping={"status": "failed", "updated_at": datetime.utcnow().isoformat(), "reason": "payment_capture_failed"})
            return

    r.hset("batch:stats", mapping={
        "total_batch_profit": total_profit,
        "cost_per_job": total_cost / len(jobs) if jobs else 0,
        "price_per_job": total_amount / len(jobs) if jobs else 0,
        "margin_per_job": total_profit / len(jobs) if jobs else 0,
        "job_count": len(jobs),
        "success_count": len(success_jobs)
    })
    r.expire("batch:stats", JOB_TTL)

    for job in jobs:
        job_id = job["job_id"]
        webhook_url = job["webhook_url"]
        status = r.hget(f"job:{job_id}", "status")
        result_url = r.hget(f"job:{job_id}", "result_url")
        notify_webhook(webhook_url, {
            "job_id": job_id,
            "status": status,
            "download_url": result_url if status == "completed" else None,
            "reason": r.hget(f"job:{job_id}", "reason") if status in ["failed", "rejected"] else None
        })

    logger.info(f"Processed batch of {len(jobs)} jobs, profit: ${total_profit:.2f}")

def notify_webhook(webhook_url, payload):
    if not WEBHOOK_SECRET:
        logger.error("WEBHOOK_SECRET not set, skipping HMAC")
        return
    payload_str = json.dumps(payload, sort_keys=True).encode()
    expected_signature = sha256(WEBHOOK_SECRET.encode() + payload_str).hexdigest()
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(webhook_url, json=payload, headers={"X-Webhook-Signature": expected_signature}, timeout=10)
            response.raise_for_status()
            signature = response.headers.get("X-Webhook-Signature")
            if signature and not compare_digest(signature, expected_signature):
                logger.error(f"Invalid HMAC signature for webhook {payload['job_id']}")
                r.hset(f"job:{payload['job_id']}", "retry_webhook", "true")
                return
            logger.info(f"Webhook notified for {payload['job_id']}: {response.status_code}")
            return
        except requests.exceptions.RequestException as e:
            logger.error(f"Webhook failed for {payload['job_id']}, attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                wait_time = exp(attempt) * (0.5 + 0.5 * random.random())
                time.sleep(wait_time)
            else:
                r.hset(f"job:{payload['job_id']}", "status", "pending_webhook")
                r.hset(f"job:{payload['job_id']}", "retry_webhook", "true")

def retry_webhooks():
    while True:
        for key in r.keys("job:*"):
            job_data = r.hgetall(key)
            if job_data.get("retry_webhook") == "true" and job_data.get("status") == "pending_webhook":
                webhook_url = job_data.get("webhook_url")
                payload = {
                    "job_id": job_data.get("job_id"),
                    "status": job_data.get("status", "completed"),
                    "download_url": job_data.get("result_url") if job_data.get("status") == "completed" else None,
                    "reason": job_data.get("reason") if job_data.get("status") in ["failed", "rejected"] else None
                }
                notify_webhook(webhook_url, payload)
                if response.status_code == 200:  # Assuming success
                    r.hdel(key, "retry_webhook")
                    r.hset(key, "status", job_data.get("status", "completed"))
        # RunPod loss prevention
        runpod_count = int(r.get(RUNPOD_USAGE_COUNT) or 0)
        last_10_margins = [float(x) for x in r.lrange("runpod:margins", 0, 9)]
        if len(last_10_margins) >= 10 and sum(1 for m in last_10_margins if m < 0) > 2:
            r.set(RUNPOD_DISABLE_KEY, "true")
            if ALERT_WEBHOOK_URL:
                requests.post(ALERT_WEBHOOK_URL, json={"content": "⚠️ RunPod disabled due to 3+ losses in 10 jobs"})
        time.sleep(60)

def run_worker():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        job_id = r.blpop("queue:instant", 1)
        if job_id:
            job_data = r.hgetall(f"job:{job_id[1]}")
            if job_data:
                loop.run_until_complete(process_job(job_data))
        for job_type in ["weather", "text_classification", "summarization"]:
            queued_jobs = [r.hgetall(f"job:{job_id[1]}") for job_id in r.lrange(f"queue:batch_{job_type}", 0, BATCH_SIZE - 1)]
            if queued_jobs and (len(queued_jobs) >= BATCH_SIZE or time.time() % BATCH_TIMEOUT < 1):
                r.delete(f"queue:batch_{job_type}")
                loop.run_until_complete(process_batch(queued_jobs))
        time.sleep(1)

def adjust_workers():
    total_jobs = sum(len(r.lrange(f"queue:{key}", 0, -1)) for key in ["instant"] + [f"batch_{t}" for t in ["weather", "text_classification", "summarization"]])
    current_workers = len(multiprocessing.active_children())
    target_workers = min(MAX_WORKERS, max(MIN_WORKERS, (total_jobs + TARGET_JOBS_PER_WORKER - 1) // TARGET_JOBS_PER_WORKER))
    if os.getenv("AUTO_SCALE", "true").lower() == "true":
        if current_workers < target_workers:
            for _ in range(target_workers - current_workers):
                p = multiprocessing.Process(target=run_worker)
                p.start()
                logger.info(f"Started new worker, total workers: {target_workers}")
        elif current_workers > target_workers:
            for _ in range(current_workers - target_workers):
                if multiprocessing.active_children():
                    multiprocessing.active_children()[-1].terminate()
                logger.info(f"Stopped worker, total workers: {target_workers}")

def cleanup_jobs():
    while True:
        for key in r.keys("job:*"):
            job_data = r.hgetall(key)
            if job_data.get("status") in ["completed", "failed", "rejected", "refunded", "disputed_refunded"]:
                r.delete(key)
        time.sleep(3600)

if __name__ == "__main__":
    logger.info("Worker started and listening for jobs...")
    for _ in range(MIN_WORKERS):
        p = multiprocessing.Process(target=run_worker)
        p.start()
    autoscaler_process = multiprocessing.Process(target=adjust_workers)
    autoscaler_process.start()
    cleanup_process = multiprocessing.Process(target=cleanup_jobs, daemon=True)
    cleanup_process.start()
    retry_webhook_process = multiprocessing.Process(target=retry_webhooks)
    retry_webhook_process.start()
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
