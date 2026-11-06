"""Copy the expensekv alias into your existing Django CACHES setting."""
import os

from redis.backoff import NoBackoff
from redis.retry import Retry

CACHES = {
    "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"},
    "expensekv": {
        "BACKEND": "examples.django_backend.ExpenseKVCache",
        "LOCATION": os.getenv("EXPENSE_KV_URL", "redis://127.0.0.1:6380/0"),
        "KEY_PREFIX": "demo:django:v1",
        "TIMEOUT": 60,
        "OPTIONS": {
            "protocol": 2,
            "password": os.getenv("EXPENSE_KV_PASSWORD") or None,
            "socket_connect_timeout": 1,
            "socket_timeout": 1,
            "max_connections": 8,
            "health_check_interval": 5,
            "retry": Retry(NoBackoff(), 0),
        },
    },
}
