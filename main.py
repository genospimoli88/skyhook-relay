from flask import Flask, request, jsonify
import uuid, logging, os, redis, json
from concurrent.futures import ThreadPoolExecutor
import utils
import os

# Ensure necessary directories exist
for folder in ["uploads", "processed", "status"]:
    os.makedirs(folder, exist_ok=True)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("skyhook")

app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=10)
r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))

@app.route("/job", methods=["POST"])
def submit_job():
    if request.files:
        return jsonify({"error": "File uploads not allowed"}), 400
    data = request.get_json(force=True)
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400

    jobs = data.get("jobs")
    payment_token = data.get("payment_token") or data.get("stripe_token") or data.get("payment_method")
    if not payment_token:
        return jsonify({"error": "Payment token is required"}), 400

    if jobs:
        if not isinstance(jobs, list) or len(jobs) == 0:
            return jsonify({"error": "Jobs must be a non-empty list"}), 400
        total_cost = 0.0
        for job in jobs:
            job_type = job.get("type")
            data_url = job.get("data_url")
            cb_url = job.get("callback_url") or data.get("callback_url")
            if not job_type or not data_url or not cb_url:
                return jsonify({"error": "Each job must include type, data_url, and callback_url"}), 400
            try:
                cost = utils.get_job_price(job_type)
                total_cost += cost
            except Exception as e:
                return jsonify({"error": str(e)}), 400
        try:
            charge_id = utils.charge_payment(total_cost, payment_token)
        except utils.PaymentError as e:
            utils.send_alert(f"Payment failed for batch: {e}")
            return jsonify({"error": "Payment failed", "details": str(e)}), 402
        job_ids = []
        for job in jobs:
            job_id = str(uuid.uuid4())
            job["id"] = job_id
            job["callback_url"] = job.get("callback_url") or data.get("callback_url")
            r.lpush("job_queue", json.dumps(job))
            job_ids.append(job_id)
        return jsonify({"status": "accepted", "job_ids": job_ids, "charge_id": charge_id})
    else:
        job_type = data.get("type")
        data_url = data.get("data_url")
        callback_url = data.get("callback_url")
        if not job_type or not data_url or not callback_url:
            return jsonify({"error": "Missing type, data_url, or callback_url"}), 400
        try:
            cost = utils.get_job_price(job_type)
        except Exception as e:
            return jsonify({"error": str(e)}), 400
        try:
            charge_id = utils.charge_payment(cost, payment_token)
        except utils.PaymentError as e:
            utils.send_alert(f"Payment failed for job: {e}")
            return jsonify({"error": "Payment failed", "details": str(e)}), 402
        job_id = str(uuid.uuid4())
        data["id"] = job_id
        r.lpush("job_queue", json.dumps(data))
        return jsonify({"status": "accepted", "job_id": job_id, "charge_id": charge_id})

@app.route("/routes")
def list_routes():
    return jsonify([str(rule) for rule in app.url_map.iter_rules()])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
