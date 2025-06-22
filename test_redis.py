import os
import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

try:
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    pong = r.ping()
    if pong:
        print("✅ Redis is connected and responding.")
    else:
        print("❌ Redis did not respond to ping.")
except Exception as e:
    print(f"❌ Redis connection failed: {e}")
