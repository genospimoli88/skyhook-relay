import os
import uuid
import logging
import json
from enum import Enum
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, HttpUrl
import redis
import uvicorn

# --- CONFIG (Editable) ---
SUPPORTED_JOB_TYPES = ["weather", "text_classification", "summarization"]

JOB_COSTS = {
    "weather": {"openweather": 0.04},
    "text_classification": {"openai": 0.10, "huggingface": 0.08},
    "summarization": {"openai": 0.15}
}

USER_PRICE = {
    "weather": 0.20,
    "text_classification": 0.35,
    "summarization": 0.45
}

MIN_MARGIN = 0.10  # Minimum allowed margin per job

BATCH_SIZE = 10
BATCH_MAX_WAIT = 5        # minutes
MIN_BATCH_MARGIN = 1.00   # Minimum allowed batch profit

INSTANT_FEE = 0.25
MIN_INSTANT_MARGIN = 0.10

WEBHOOK_FIELDS = ["job_id", "status", "result", "error"]
STRIPE_TEST_MODE = True

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
PORT = int(os.getenv("PORT", "8000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_DIR = os.getenv("LOG_DIR", os.path.join(os.path.expanduser("~"), "skyhook_logs"))
os.makedirs(LOG_DIR, exist_ok=True)
log_path = os.path.join(LOG_DIR, "api.log")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(log_path), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- REDIS ---
try:
    redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5)
    redis_client.ping()
    logger.info(f"✅ Connected to Redis at {REDIS_URL}")
except Exception as e:
    logger.error(f"❌ Redis connection failed: {e}")
    raise RuntimeError("Redis connection failed. Shutting down.")

# --- ENUMS & MODELS ---
class JobType(str, Enum):
    weather = "weather"
    text_classification = "text_classification"
    summarization = "summarization"

class JobSubmission(BaseModel):
    job_id: str | None = None
    job_type: JobType
    input_url: HttpUrl
    webhook_url: HttpUrl
    price: float = Field(..., gt=0)
    payment_intent_id: str = Field(..., description="Stripe PaymentIntent ID")

class RefundRequest(BaseModel):
    job_id: str

class JobResponse(BaseModel):
    job_id: str
    status: str

class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    download_url: str | None = None
    reason: str | None = None

# --- FASTAPI ---
app = FastAPI(title="Skyhook Relay API", version="1.0.0")

def get_provider_and_cost(job_type):
    # Choose the default provider for each job type for now
    if job_type not in JOB_COSTS:
        raise ValueError("Unsupported job type")
    provider, cost = list(JOB_COSTS[job_type].items())[0]
    return provider, cost

def verify_margin(job_type, user_price, provider_cost, instant=False):
    # Margin logic for instant and batch jobs
    margin = user_price - provider_cost
    if instant:
        margin -= INSTANT_FEE
        return margin >= MIN_INSTANT_MARGIN, margin
    else:
        return margin >= MIN_MARGIN, margin

# Placeholder for Stripe logic (should be in payments.py, mocked here)
def verify_payment_intent(payment_intent_id):
    if STRIPE_TEST_MODE:
        return True  # Assume always valid in test mode
    # Real implementation would query Stripe API
    return False

@app.post("/job", response_model=JobResponse, status_code=202)
def submit_job(job: JobSubmission):
    job_id = job.job_id.strip() if job.job_id else str(uuid.uuid4())
    if redis_client.exists(f"job:{job_id}"):
        logger.warning(f"Duplicate job_id {job_id}")
        raise HTTPException(status_code=409, detail="job_id already exists")
    if not verify_payment_intent(job.payment_intent_id):
        logger.error(f"Payment for job {job_id} not completed: {job.payment_intent_id}")
        raise HTTPException(status_code=402, detail="Payment not confirmed.")

    # Margin/profit logic
    try:
        provider, provider_cost = get_provider_and_cost(job.job_type.value)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    is_instant = (job.job_type.value == "instant")
    user_price = job.price
    if is_instant:
        margin_ok, margin = verify_margin(job.job_type.value, user_price, provider_cost, instant=True)
        queue_name = "queue:instant"
    else:
        margin_ok, margin = verify_margin(job.job_type.value, user_price, provider_cost)
        queue_name = f"queue:batch_{job.job_type.value}"

    if not margin_ok:
        raise HTTPException(status_code=400, detail=f"Insufficient margin for this job. Margin={margin:.2f}")

    job_data = {
        "job_id": job_id,
        "job_type": job.job_type.value,
        "provider": provider,
        "input_url": str(job.input_url),
        "webhook_url": str(job.webhook_url),
        "price": str(job.price),
        "provider_cost": str(provider_cost),
        "status": "submitted",
        "created_at": datetime.utcnow().isoformat(),
        "payment_intent_id": job.payment_intent_id,
    }
    redis_client.hset(f"job:{job_id}", mapping=job_data)
    redis_client.rpush(queue_name, job_id)
    logger.info(f"Enqueued job {job_id} to {queue_name} (margin: {margin:.2f})")
    return {"job_id": job_id, "status": "submitted"}

# Add alias endpoint /jobs for bot compatibility
@app.post("/jobs", response_model=JobResponse, status_code=202)
def submit_job_alias(job: JobSubmission):
    return submit_job(job)

@app.get("/job/status/{job_id}", response_model=JobStatusResponse)
def get_status(job_id: str):
    key = f"job:{job_id}"
    if not redis_client.exists(key):
        logger.warning(f"Job {job_id} not found.")
        raise HTTPException(status_code=404, detail="Job not found")
    job_data = redis_client.hgetall(key)
    return {
        "job_id": job_id,
        "status": job_data.get("status", "unknown"),
        "download_url": job_data.get("result_url"),
        "reason": job_data.get("reason")
    }

@app.post("/job/refund", response_model=JobResponse)
def refund_job(refund: RefundRequest):
    job_id = refund.job_id
    key = f"job:{job_id}"
    if not redis_client.exists(key):
        raise HTTPException(status_code=404, detail="Job not found")
    job_data = redis_client.hgetall(key)
    current_status = job_data.get("status", "")
    if current_status in ("refunded", "disputed_refunded"):
        return {"job_id": job_id, "status": current_status}
    job_type = job_data.get("job_type", "")
    queue_name = "queue:instant" if job_type == "instant" else f"queue:batch_{job_type}"
    redis_client.lrem(queue_name, 0, job_id)
    redis_client.hset(key, mapping={
        "status": "refunded",
        "updated_at": datetime.utcnow().isoformat()
    })
    logger.info(f"Refunded job {job_id}")
    return {"job_id": job_id, "status": "refunded"}

@app.get("/healthz")
def healthz():
    try:
        redis_client.ping()
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Redis ping failed during health check: {e}")
        raise HTTPException(status_code=503, detail="Redis unavailable")

if __name__ == "__main__":
    logger.info(f"Starting FastAPI on port {PORT}")
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info")
