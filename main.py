from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
import os, uuid, logging
import httpx
import stripe
import redis
from rq import Queue
# Import worker for access to task function, but ensure it doesn't run worker loop on import
import worker

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# Load environment variables
STRIPE_API_KEY = os.getenv("STRIPE_API_KEY")
if not STRIPE_API_KEY:
    logger.error("Stripe API key not configured")
stripe.api_key = STRIPE_API_KEY if STRIPE_API_KEY else None
# Determine Stripe mode (test vs live)
STRIPE_TEST_MODE = False
if STRIPE_API_KEY:
    if STRIPE_API_KEY.startswith("sk_test") or "test" in STRIPE_API_KEY:
        STRIPE_TEST_MODE = True

# Redis connection (for RQ queue and caching if needed)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
try:
    redis_conn = redis.Redis.from_url(REDIS_URL)
except Exception as e:
    logger.error(f"Failed to connect to Redis: {e}")
    redis_conn = None

# Pricing structure for fees (Batch vs Instant)
PRICING = {
    "batch": {"weather": 0.15, "text_classification": 0.35, "summarization": 0.45},
    "instant": {"weather": 0.15, "text_classification": 0.25, "summarization": 0.25}
}

# Job cost structure (cost per task by provider)
JOB_COSTS = {
    "weather": {"openweather": 0.04},
    "text_classification": {"openai": 0.10},
    "summarization": {"openai": 0.15}
}

# FastAPI app initialization
app = FastAPI()

# Pydantic model for job submission
class JobRequest(BaseModel):
    job_type: str
    input_url: HttpUrl
    webhook_url: HttpUrl | None = None
    price: float
    payment_intent_id: str

@app.post("/job")
async def submit_job(request: JobRequest):
    job_type = request.job_type
    input_url = str(request.input_url)
    webhook_url = str(request.webhook_url) if request.webhook_url else None
    price = request.price
    payment_intent_id = request.payment_intent_id

    # Validate job_type
    if job_type not in JOB_COSTS:
        raise HTTPException(status_code=422, detail=f"Unsupported job type: {job_type}")

    # Generate a unique job ID
    job_id = str(uuid.uuid4())

    # Payment verification via Stripe
    stripe_url = f"https://api.stripe.com/v1/payment_intents/{payment_intent_id}"
    logger.info(f'message="Request to Stripe API" method=GET url="{stripe_url}"')
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            stripe_resp = await client.get(stripe_url, auth=(STRIPE_API_KEY or "", ''))
    except Exception as e:
        # Stripe API call failed (network issue or similar)
        logger.error(f'Stripe API request failed for job {job_id}: {e}')
        raise HTTPException(status_code=502, detail="Failed to verify payment.")
    # Log Stripe API response
    if stripe_resp.status_code != 200:
        # Retrieve error details if available
        err_msg = ""
        try:
            err_data = stripe_resp.json()
            if "error" in err_data and "message" in err_data["error"]:
                err_msg = err_data["error"]["message"]
        except Exception:
            err_msg = stripe_resp.text or ""
        logger.error(f'Payment for job {job_id} not completed: {payment_intent_id} (Stripe response {stripe_resp.status_code}: {err_msg})')
        raise HTTPException(status_code=402, detail="Payment not confirmed.")
    # If 200 OK, parse the payment intent
    intent_data = stripe_resp.json()
    status = intent_data.get("status")
    amount = intent_data.get("amount")  # amount in cents
    # Check that payment is completed or captured
    if status not in ("succeeded", "requires_capture"):
        logger.error(f'Payment for job {job_id} not completed: status={status}')
        raise HTTPException(status_code=402, detail="Payment not confirmed.")
    # Check amount vs provided price
    expected_cents = int(round(price * 100))
    if amount is not None and expected_cents != amount:
        logger.error(f'Payment amount mismatch for job {job_id}: expected ${price:.2f}, Stripe has ${(amount/100):.2f}')
        raise HTTPException(status_code=422, detail="Payment amount mismatch.")

    # Determine mode: batch vs instant (based on presence of webhook_url)
    mode = "batch" if webhook_url else "instant"
    base_price = PRICING[mode].get(job_type)
    if base_price is None:
        logger.error(f'No pricing defined for mode {mode} and job type {job_type}')
        raise HTTPException(status_code=422, detail="Pricing not defined for this job type.")

    # Apply logic constraints based on mode
    tasks_count = 1  # default number of tasks
    if mode == "batch":
        # Attempt to count number of tasks in input for weather or classification (to enforce price per task)
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                input_resp = await client.get(input_url)
                if input_resp.status_code == 200:
                    content_text = input_resp.text.strip()
                    if job_type in ("weather", "text_classification"):
                        lines = [line for line in content_text.splitlines() if line.strip()]
                        tasks_count = len(lines) if lines else 1
                    else:
                        tasks_count = 1
                else:
                    tasks_count = 1
        except Exception as e:
            logger.warning(f'Could not retrieve input to count tasks for job {job_id}: {e}')
            tasks_count = 1
        # Ensure offered price covers at least base price per task for all tasks
        min_required_price = base_price * tasks_count
        if price < min_required_price - 1e-6:
            logger.error(f'Offered price ${price:.2f} too low for {tasks_count} tasks (min ${min_required_price:.2f}) for job {job_id}')
            raise HTTPException(status_code=422, detail="Price below minimum required for tasks.")
        # Calculate total margin (price - total_cost)
        cost_per_task = list(JOB_COSTS[job_type].values())[0]
        total_cost = cost_per_task * tasks_count
        margin = price - total_cost
        if margin < 1.00 - 1e-6:
            logger.error(f'Total margin ${margin:.2f} below $1.00 minimum for batch job {job_id}')
            raise HTTPException(status_code=422, detail="Total margin below required minimum $1.00 for batch job.")
    else:
        # Instant mode: ensure only one task is provided (no multiple inputs combined)
        if job_type in ("weather", "text_classification"):
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    input_resp = await client.get(input_url)
                    content_text = input_resp.text.strip() if input_resp.status_code == 200 else ""
                lines = [line for line in content_text.splitlines() if line.strip()]
                if len(lines) > 1:
                    logger.error(f'Multiple tasks provided in instant mode for job {job_id}, not allowed.')
                    raise HTTPException(status_code=422, detail="Multiple inputs provided for instant job; use batch mode for multiple tasks.")
            except HTTPException:
                raise  # propagate the 422 error
            except Exception as e:
                logger.warning(f'Could not retrieve input content for job {job_id} (instant mode): {e}')
        # Ensure price is not below base price for one task
        if price < base_price - 1e-6:
            logger.error(f'Offered price ${price:.2f} below minimum ${base_price:.2f} for instant job {job_id}')
            raise HTTPException(status_code=422, detail="Price below minimum for instant job.")
        cost_per_task = list(JOB_COSTS[job_type].values())[0]
        margin = price - cost_per_task
        if margin < 0.10 - 1e-6:
            logger.error(f'Margin ${margin:.2f} below $0.10 minimum for instant job {job_id}')
            raise HTTPException(status_code=422, detail="Margin below required minimum $0.10 for instant job.")

    # Process job based on mode
    if mode == "batch":
        # Enqueue the job for asynchronous processing
        if not redis_conn:
            logger.error(f'Cannot enqueue job {job_id}: Redis connection not available')
            raise HTTPException(status_code=500, detail="Server error (queue unavailable).")
        queue_name = f"batch_jobType.{job_type}"
        q = Queue(name=queue_name, connection=redis_conn)
        try:
            q.enqueue(worker.process_job, job_id, job_type, input_url, webhook_url, float(price), job_id=job_id)
        except Exception as e:
            logger.error(f'Failed to enqueue job {job_id}: {e}')
            raise HTTPException(status_code=500, detail="Failed to enqueue job.")
        logger.info(f'Enqueued job {job_id} to queue:batch_jobType.{job_type}')
        return {"job_id": job_id, "status": "submitted"}
    else:
        # Instant mode: process immediately and return result
        try:
            result = await worker.process_job_direct(job_id, job_type, input_url, price)
            # Stripe payout logic for instant job
            provider = list(JOB_COSTS[job_type].keys())[0]
            cost_per_task = list(JOB_COSTS[job_type].values())[0]
            total_cost = cost_per_task  # tasks_count is 1 in instant mode
            profit = price - total_cost
            acct_env = f"{provider.upper()}_ACCOUNT_ID"
            acct_id = os.getenv(acct_env)
            if acct_id:
                try:
                    if STRIPE_TEST_MODE:
                        logger.info(f'(Test mode) Would transfer ${total_cost:.2f} to account {acct_id} for provider {provider}')
                    else:
                        transfer = stripe.Transfer.create(amount=int(total_cost*100), currency="usd", destination=acct_id)
                        logger.info(f'Stripe transfer executed for job {job_id}: ${total_cost:.2f} to {provider} (Transfer ID: {transfer.get("id", "")})')
                except Exception as e:
                    logger.error(f'Stripe payout failed for job {job_id}: {e}')
            else:
                logger.info(f'No Stripe payout account for provider {provider}. Cost ${total_cost:.2f} will not be paid out via Stripe.')
            logger.info(f'Job {job_id} completed instantly: price ${price:.2f}, cost ${total_cost:.2f}, profit ${profit:.2f}')
            return {"job_id": job_id, "status": "completed", "result": result}
        except HTTPException as e:
            # Propagate any HTTPException (e.g., input-related errors) to client
            raise e
        except Exception as e:
            logger.error(f'Instant job {job_id} failed during processing: {e}')
            raise HTTPException(status_code=500, detail="Job processing failed.")
