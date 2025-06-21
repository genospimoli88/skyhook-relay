import os
import uuid
import redis
import stripe
import aiohttp
import logging
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel, HttpUrl
from typing import Literal
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("/mnt/data/jobs/app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Skyhook Relay API",
    description="A fast, reliable API for bot-driven tasks like weather queries, transcriptions, OCR, and LLM tasks with webhook notifications and result storage.",
    version="1.0.0"
)
stripe.api_key = os.getenv("STRIPE_API_KEY")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
try:
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5)
    r.ping()
    logger.info("Connected to Redis")
except redis.exceptions.RedisError as e:
    logger.error(f"Failed to connect to Redis: {e}")
    r = None

class JobRequest(BaseModel):
    job_type: Literal["weather", "transcription", "ocr", "llm"]
    city: str | None = None
    audio_url: HttpUrl | None = None
    image_url: HttpUrl | None = None
    prompt: str | None = None
    payment_token: str
    webhook_url: HttpUrl
    priority: Literal["immediate", "queued"] = "queued"

class JobStatus(BaseModel):
    job_id: str
    status: Literal["pending", "processing", "completed", "failed"]
    created_at: str
    updated_at: str
    result_url: str | None = None
    price: float

def get_pricing(job_type: str, priority: str) -> float:
    """Retrieve pricing from Redis or use defaults."""
    pricing = {
        "weather": {"immediate": 0.10, "queued": 0.09},
        "transcription": {"immediate": 0.60, "queued": 0.54},
        "ocr": {"immediate": 0.10, "queued": 0.09},
        "llm": {"immediate": 1.00, "queued": 0.90}
    }
    key = f"pricing:{job_type}:{priority}"
    try:
        price = r.hget("pricing", key)
        return float(price) if price else pricing[job_type][priority]
    except redis.exceptions.RedisError as e:
        logger.error(f"Failed to fetch price for {key}: {e}")
        return pricing[job_type][priority]

async def send_webhook(webhook_url: str, payload: dict):
    """Send job status/result to webhook URL."""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(webhook_url, json=payload, timeout=10) as resp:
                if resp.status != 200:
                    logger.warning(f"Webhook failed: {webhook_url}, status: {resp.status}")
        except Exception as e:
            logger.error(f"Webhook error: {webhook_url}, {str(e)}")

@app.on_event("startup")
async def startup_event():
    """Initialize Redis connection and pricing."""
    if r is None:
        logger.error("Redis unavailable at startup")
        raise HTTPException(status_code=500, detail="Redis connection failed")
    try:
        r.ping()
        logger.info("Redis connection confirmed")
        default_pricing = {
            "weather:immediate": 0.10, "weather:queued": 0.09,
            "transcription:immediate": 0.60, "transcription:queued": 0.54,
            "ocr:immediate": 0.10, "ocr:queued": 0.09,
            "llm:immediate": 1.00, "llm:queued": 0.90
        }
        for key, value in default_pricing.items():
            if not r.hget("pricing", key):
                r.hset("pricing", key, value)
        logger.info("Pricing initialized")
    except redis.exceptions.RedisError as e:
        logger.error(f"Startup error: {e}")
        raise HTTPException(status_code=500, detail="Redis connection failed")

@app.post("/run", response_model=JobStatus)
async def submit_job(job: JobRequest, background_tasks: BackgroundTasks):
    """Submit a job with Stripe payment validation."""
    job_id = str(uuid.uuid4())
    price = get_pricing(job.job_type, job.priority)
    
    try:
        charge = stripe.Charge.create(
            amount=int(price * 100),
            currency="usd",
            source=job.payment_token,
            description=f"Skyhook Relay Job {job_id} ({job.job_type})"
        )
        if charge.status != "succeeded":
            logger.error(f"Payment failed for job {job_id}: {charge.status}")
            raise HTTPException(status_code=400, detail="Payment failed")
        logger.info(f"Charged ${price:.2f} for job {job_id}: {charge.id}")
    except stripe.error.StripeError as e:
        logger.error(f"Payment error for job {job_id}: {e}")
        raise HTTPException(status_code=400, detail=f"Payment error: {str(e)}")

    created_at = datetime.utcnow().isoformat()
    job_data = {
        "job_id": job_id,
        "job_type": job.job_type,
        "city": job.city,
        "audio_url": str(job.audio_url) if job.audio_url else None,
        "image_url": str(job.image_url) if job.image_url else None,
        "prompt": job.prompt,
        "webhook_url": str(job.webhook_url),
        "priority": job.priority,
        "status": "pending",
        "created_at": created_at,
        "updated_at": created_at,
        "price": str(price)
    }
    try:
        r.hset(f"job:{job_id}", mapping=job_data)
        r.lpush(f"queue:{job.priority}", job_id)
        logger.info(f"Queued job {job_id} to queue:{job.priority}")
    except redis.exceptions.RedisError as e:
        logger.error(f"Failed to queue job {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to queue job")

    background_tasks.add_task(
        send_webhook,
        job.webhook_url,
        {"job_id": job_id, "status": "pending", "created_at": created_at, "price": price}
    )

    return JobStatus(
        job_id=job_id,
        status="pending",
        created_at=created_at,
        updated_at=created_at,
        price=price
    )

@app.get("/v1/job/status/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    """Retrieve job status."""
    try:
        job_data = r.hgetall(f"job:{job_id}")
        if not job_data:
            logger.warning(f"Job not found: {job_id}")
            raise HTTPException(status_code=404, detail="Job not found")
        return JobStatus(
            job_id=job_id,
            status=job_data["status"],
            created_at=job_data["created_at"],
            updated_at=job_data["updated_at"],
            result_url=job_data.get("result_url"),
            price=float(job_data["price"])
        )
    except redis.exceptions.RedisError as e:
        logger.error(f"Failed to get status for job {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve status")

@app.get("/v1/job/result/{job_id}")
async def get_job_result(job_id: str):
    """Retrieve job result."""
    try:
        job_data = r.hgetall(f"job:{job_id}")
        if not job_data or job_data["status"] != "completed":
            logger.warning(f"Result not available for job {job_id}")
            raise HTTPException(status_code=404, detail="Result not available")
        result_url = job_data.get("result_url")
        if not result_url:
            logger.error(f"Result URL missing for job {job_id}")
            raise HTTPException(status_code=404, detail="Result URL missing")
        return {"job_id": job_id, "result_url": result_url}
    except redis.exceptions.RedisError as e:
        logger.error(f"Failed to get result for job {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve result")

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        if r:
            r.ping()
        return {"status": "healthy", "redis": "connected"}
    except redis.exceptions.RedisError as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "unhealthy", "redis": f"error: {str(e)}"}

def custom_openapi():
    """Generate OpenAPI schema for bot-friendly documentation."""
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes
    )
    openapi_schema["info"]["contact"] = {
        "name": "Skyhook Relay Support",
        "email": "support@skyhookrelay.com"
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi