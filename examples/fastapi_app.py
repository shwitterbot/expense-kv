import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import RedisError


@asynccontextmanager
async def lifespan(app):
    app.state.kv = Redis.from_url(
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
    try:
        yield
    finally:
        await app.state.kv.aclose()


app = FastAPI(lifespan=lifespan)


@app.get("/summary")
async def summary(request: Request):
    kv = request.app.state.kv
    key = "demo:fastapi:v1:summary"
    try:
        cached = await kv.get(key)
        if cached is not None:
            return {"source": "cache", "data": json.loads(cached)}
    except (RedisError, ValueError):
        pass
    data = {"total_minor": 12500, "currency": "RUB"}
    try:
        await kv.set(key, json.dumps(data), ex=60)
    except RedisError:
        pass
    return {"source": "computed", "data": data}
