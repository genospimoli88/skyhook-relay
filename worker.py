import os
import time
import json
import logging
import requests
from datetime import datetime
import redis

# --- CONFIG ---
REDIS_URL = os.getenv("REDIS_URL")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "1"))

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

MIN_MARGIN = 0.10
BATCH_SIZE = 10
BATCH_MAX_WAIT = 5      # minutes
MIN_BATCH_MARGIN = 1.00
INSTANT_FEE = 0.25
MIN_INSTANT_MARGIN = 0.10

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")
HF_API_KEY = os.getenv("HF_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("skyhook-worker")

# --- REDIS CLIENT (FIXED) ---
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5)


def send_webhook(webhook_url, payload):
    try:
        r = requests.post(webhook_url, json=payload, timeout=10)
        r.raise_for_status()
        logger.info(f"✅ Webhook sent to {webhook_url}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to send webhook: {e}")
        return False

def fetch_text_from_url(url):
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.error(f"❌ Could not fetch text from {url}: {e}")
        return None

def handle_weather(job_id, job_data):
    location = job_data.get("input_url", "")
    webhook_url = job_data.get("webhook_url")
    payload = {"job_id": job_id, "status": "completed"}
    try:
        if "lat=" in location and "lon=" in location:
            lat = location.split("lat=")[1].split("&")[0]
            lon = location.split("lon=")[1].split("&")[0]
            api_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=imperial"
        else:
            api_url = f"https://api.openweathermap.org/data/2.5/weather?q={location}&appid={OPENWEATHER_API_KEY}&units=imperial"
        resp = requests.get(api_url, timeout=10)
        resp.raise_for_status()
        weather = resp.json()
        payload["result"] = {
            "location": location,
            "weather": weather.get("weather", [{}])[0].get("description"),
            "temperature_F": weather.get("main", {}).get("temp"),
            "humidity": weather.get("main", {}).get("humidity"),
            "wind_speed": weather.get("wind", {}).get("speed"),
            "raw": weather
        }
    except Exception as e:
        payload["status"] = "error"
        payload["error"] = f"Weather API failed: {e}"

    send_webhook(webhook_url, payload)
    redis_client.hset(f"job:{job_id}", mapping={
        "status": payload["status"],
        "updated_at": datetime.utcnow().isoformat(),
        "result": json.dumps(payload.get("result", "")),
        "error": payload.get("error", "")
    })

def handle_text_classification(job_id, job_data):
    webhook_url = job_data.get("webhook_url")
    input_url = job_data.get("input_url")
    text = fetch_text_from_url(input_url) if input_url.startswith("http") else input_url
    payload = {"job_id": job_id, "status": "completed"}
    try:
        if not text:
            raise Exception("No text to classify.")
        headers = {"Authorization": f"Bearer {HF_API_KEY}"}
        data = {"inputs": text}
        api_url = "https://api-inference.huggingface.co/models/distilbert-base-uncased-finetuned-sst-2-english"
        resp = requests.post(api_url, headers=headers, json=data, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        payload["result"] = result
    except Exception as e:
        payload["status"] = "error"
        payload["error"] = f"Text classification failed: {e}"

    send_webhook(webhook_url, payload)
    redis_client.hset(f"job:{job_id}", mapping={
        "status": payload["status"],
        "updated_at": datetime.utcnow().isoformat(),
        "result": json.dumps(payload.get("result", "")),
        "error": payload.get("error", "")
    })

def handle_summarization(job_id, job_data):
    webhook_url = job_data.get("webhook_url")
    input_url = job_data.get("input_url")
    text = fetch_text_from_url(input_url) if input_url.startswith("http") else input_url
    payload = {"job_id": job_id, "status": "completed"}
    try:
        if not text:
            raise Exception("No text to summarize.")
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
        api_url = "https://api.openai.com/v1/chat/completions"
        prompt = f"Summarize this:\n{text[:2000]}"
        data = {
            "model": "gpt-3.5-turbo",
            "messages": [
                {"role": "system", "content": "You are a helpful summarization bot."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 256,
        }
        resp = requests.post(api_url, headers=headers, json=data, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        summary = result["choices"][0]["message"]["content"].strip()
        payload["result"] = {"summary": summary}
    except Exception as e:
        payload["status"] = "error"
        payload["error"] = f"Summarization failed: {e}"

    send_webhook(webhook_url, payload)
    redis_client.hset(f"job:{job_id}", mapping={
        "status": payload["status"],
        "updated_at": datetime.utcnow().isoformat(),
        "result": json.dumps(payload.get("result", "")),
        "error": payload.get("error", "")
    })

def handle_instant(job_id, job_data):
    job_type = job_data.get("job_type")
    logger.info(f"🚀 Handling instant job: {job_id} ({job_type})")
    if job_type == "weather":
        handle_weather(job_id, job_data)
    elif job_type == "text_classification":
        handle_text_classification(job_id, job_data)
    elif job_type == "summarization":
        handle_summarization(job_id, job_data)
    else:
        payload = {
            "job_id": job_id,
            "status": "error",
            "error": f"Unknown instant job type: {job_type}"
        }
        send_webhook(job_data.get("webhook_url"), payload)
        redis_client.hset(f"job:{job_id}", mapping={
            "status": "error",
            "updated_at": datetime.utcnow().isoformat(),
            "error": payload["error"]
        })

def worker_loop():
    logger.info("🔁 Skyhook worker started")
    queues = [
        ("queue:instant", handle_instant),
        ("queue:batch_weather", handle_weather),
        ("queue:batch_text_classification", handle_text_classification),
        ("queue:batch_summarization", handle_summarization),
    ]
    while True:
        job_found = False
        for queue_name, handler in queues:
            job_id = redis_client.lpop(queue_name)
            if job_id:
                key = f"job:{job_id}"
                if not redis_client.exists(key):
                    logger.warning(f"Job not found: {key}")
                    continue
                job_data = redis_client.hgetall(key)
                logger.info(f"🛠️ Processing job {job_id} from {queue_name} ({job_data.get('job_type')})")
                handler(job_id, job_data)
                job_found = True
                break
        if not job_found:
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    try:
        redis_client.ping()
        logger.info(f"✅ Connected to Redis at {REDIS_URL}")
        worker_loop()
    except Exception as e:
        logger.error(f"❌ Worker startup failed: {e}")

        logger.info(f"✅ Connected to Redis at {REDIS_URL}")
        worker_loop()
    except Exception as e:
        logger.error(f"❌ Worker startup failed: {e}")
