
import stripe
import logging
import time
import os
import json
import uuid

# Set Stripe API key from environment with fallback warning
stripe.api_key = os.environ.get("STRIPE_API_KEY", "")
if not stripe.api_key:
    raise RuntimeError("Missing STRIPE_API_KEY in environment variables.")

logger = logging.getLogger("skyhook")

# Directory paths
UPLOAD_DIR = "uploads"
PROCESSED_DIR = "processed"
STATUS_DIR = "status"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(STATUS_DIR, exist_ok=True)

class PaymentError(Exception):
    pass

def get_job_price(job_type):
    pricing = {
        "weather": 0.25,
        "text": 0.10,
        "image": 0.50
    }
    if job_type not in pricing:
        raise ValueError(f"Unsupported job type: {job_type}")
    return pricing[job_type]

def charge_payment(amount, payment_token):
    try:
        charge = stripe.Charge.create(
            amount=int(amount * 100),
            currency="usd",
            source=payment_token,
            description=f"Skyhook job relay: ${amount:.2f}"
        )
        return charge.id
    except Exception as e:
        raise PaymentError(str(e))

def send_alert(message):
    logger.error(f"⚠️ ALERT: {message}")

def get_next_job():
    for fname in os.listdir(UPLOAD_DIR):
        if fname.endswith(".json"):
            path = os.path.join(UPLOAD_DIR, fname)
            try:
                with open(path, "r") as f:
                    job_data = json.load(f)
                os.remove(path)
                return job_data
            except Exception as e:
                send_alert(f"Error reading job file {fname}: {e}")
    return None
