# Skyhook Relay

A scalable job processing system designed to handle 50,000 jobs/day, built with FastAPI and deployed via Docker on RunPod.

## Features
- Job submission and status tracking
- Autoscaling workers based on queue size
- Stripe payment integration with refunds
- Webhook notifications with HMAC security
- Throttling and 429 backoff for provider APIs

## Setup
1. Clone the repo: `git clone https://github.com/your-username/skyhook-relay.git`
2. Install dependencies: `pip install -r requirements.txt`
3. Set environment variables (see `.env` example) or configure in RunPod.
4. Build Docker image: `docker build -t skyhook-relay .`
5. Run locally: `uvicorn job_processor:app --reload` or deploy to RunPod.

## Deployment
- Push Docker image to RunPod registry: `docker push registry.runpod.net/genospimoli88-skyhook-relay-main-dockerfile:latest`
- Configure RunPod with environment variables and start the container.

## API Documentation
See `api.md` for endpoint details.

## Logs
Check `/mnt/data/jobs/worker.log` for debugging.

## License
[Add your license, e.g., MIT]
