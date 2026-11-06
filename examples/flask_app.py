import atexit
import json
import os

from flask import Flask
from redis import Redis
from redis.backoff import NoBackoff
from redis.exceptions import RedisError
from redis.retry import Retry

app = Flask(__name__)
kv = Redis.from_url(
    os.getenv("EXPENSE_KV_URL", "redis://127.0.0.1:6380/0"),
    password=os.getenv("EXPENSE_KV_PASSWORD") or None,
    protocol=2,
    decode_responses=True,
    socket_connect_timeout=1,
    socket_timeout=1,
    max_connections=8,
    health_check_interval=5,
    retry=Retry(NoBackoff(), 0),
)
atexit.register(kv.close)


@app.get("/summary")
def summary():
    key = "demo:flask:v1:summary"
    try:
        cached = kv.get(key)
        if cached is not None:
            return {"source": "cache", "data": json.loads(cached)}
    except (RedisError, ValueError):
        pass
    data = {"total_minor": 12500, "currency": "RUB"}
    try:
        kv.set(key, json.dumps(data), ex=60)
    except RedisError:
        pass
    return {"source": "computed", "data": data}
