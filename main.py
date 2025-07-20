import os
import uuid
import logging
from enum import Enum
from datetime import datetime

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, HttpUrl
import redis
import uvicorn

# Stripe payments import
from payments import verify_payment_intent

import os
print(">>> REDIS_URL in Python:", os.environ.get("REDIS_URL"))


# Config
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
LOG_DIR = os.getenv("LOG_DIR", os.path.join(os.path.expanduser("~"), "skyhook_logs"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
PORT = int(os.getenv("PORT", "8000"))

os.makedirs(LOG_DIR, exist_ok=True)
log_path = os.path.join(LOG_DIR, "api.log")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(log_path), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Redis connection
try:
    redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5)
    redis_client.ping()
    logger.info(f"✅ Connected to Redis at {REDIS_URL}")
except Exception as e:
    logger.error(f"❌ Redis connection failed: {e}")
    raise RuntimeError("Redis connection failed. Shutting down.")

# Enums and Models
class JobType(str, Enum):
    weather = "weather"
    text_classification = "text_classification"
    summarization = "summarization"
    instant = "instant"

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

# FastAPI setup
app = FastAPI(title="Skyhook Relay API", version="1.0.0")

@app.post("/job", response_model=JobResponse, status_code=202)
def submit_job(job: JobSubmission):
    job_id = job.job_id.strip() if job.job_id else str(uuid.uuid4())
    if redis_client.exists(f"job:{job_id}"):
        logger.warning(f"Duplicate job_id {job_id}")
        raise HTTPException(status_code=409, detail="job_id already exists")

    if not verify_payment_intent(job.payment_intent_id):
        logger.error(f"Payment for job {job_id} not completed: {job.payment_intent_id}")
        raise HTTPException(status_code=402, detail="Payment not confirmed.")

    job_data = {
        "job_id": job_id,
        "job_type": job.job_type,
        "input_url": str(job.input_url),
        "webhook_url": str(job.webhook_url),
        "price": str(job.price),
        "status": "submitted",
        "created_at": datetime.utcnow().isoformat(),
        "payment_intent_id": job.payment_intent_id
    }

    redis_client.hset(f"job:{job_id}", mapping=job_data)
    queue_name = "queue:instant" if job.job_type == "instant" else f"queue:batch_{job.job_type}"
    redis_client.rpush(queue_name, job_id)

    logger.info(f"Enqueued job {job_id} to {queue_name}")
    return {"job_id": job_id, "status": "submitted"}

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
    if not job_type:
        logger.error(f"Refund error: job_type missing for job {job_id}")
        raise HTTPException(status_code=400, detail="Invalid job data (job_type missing)")

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
