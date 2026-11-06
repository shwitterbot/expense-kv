"""Run against a temporary authenticated ExpenseKV; never use the application's DB."""
import asyncio
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch
from urllib.request import urlopen

import django
import httpx
from redis import Redis
from redis.backoff import NoBackoff
from redis.exceptions import AuthenticationError, ConnectionError as RedisConnectionError
from redis.retry import Retry


def free_port():
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        return reservation.getsockname()[1]


class Integrations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = Path(__file__).resolve().parents[1]
        cls.directory = tempfile.TemporaryDirectory(prefix="expense-kv-integration-")
        cls.addClassCleanup(cls.directory.cleanup)
        port = free_port()
        password = secrets.token_urlsafe(24)
        url = f"redis://127.0.0.1:{port}/0"
        os.environ["EXPENSE_KV_URL"] = url
        os.environ["EXPENSE_KV_PASSWORD"] = password
        cls.process = subprocess.Popen(
            [str(root / "target/release/expense-kv")],
            env=dict(os.environ, KV_BIND=f"127.0.0.1:{port}", KV_PASSWORD=password,
                     KV_DATA_DIR=cls.directory.name, KV_TIMEOUT_SECONDS="60"),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        def stop_server():
            if cls.process.poll() is None:
                cls.process.terminate()
                cls.process.wait(timeout=5)
        cls.addClassCleanup(stop_server)
        cls.kv = Redis.from_url(url, password=password, protocol=2, decode_responses=True,
                               socket_timeout=1, socket_connect_timeout=1,
                               retry=Retry(NoBackoff(), 0))
        cls.addClassCleanup(cls.kv.close)
        deadline = time.monotonic() + 10
        while True:
            if cls.process.poll() is not None:
                raise RuntimeError("temporary ExpenseKV exited")
            try:
                assert cls.kv.ping()
                break
            except RedisConnectionError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.02)
        from django.conf import settings
        from examples.django_settings import CACHES
        settings.configure(CACHES=CACHES)
        django.setup()

    def test_redis_py_auth_and_pipeline(self):
        with Redis.from_url(os.environ["EXPENSE_KV_URL"], password="wrong", protocol=2,
                            retry=Retry(NoBackoff(), 0)) as wrong:
            with self.assertRaises(AuthenticationError):
                wrong.ping()
        with self.kv.pipeline(transaction=False) as pipe:
            pipe.set("demo:python:v1:json", '{"currency":"RUB"}', ex=60)
            pipe.get("demo:python:v1:json")
            self.assertEqual(pipe.execute(), [True, '{"currency":"RUB"}'])

    def test_django_cache(self):
        from django.core.cache import caches
        cache = caches["expensekv"]
        self.addCleanup(cache.close)
        cache.set("unicode", {"текст": "расходы"}, timeout=60)
        self.assertEqual(cache.get("unicode"), {"текст": "расходы"})
        self.assertFalse(cache.add("unicode", "overwrite", timeout=60))
        self.assertEqual(cache.set_many({"one": 1, "two": {"amount": 2}}, timeout=60), [])
        self.assertEqual(cache.get_many(["one", "two", "absent"]), {"one": 1, "two": {"amount": 2}})
        cache.set("counter", 10, timeout=60)
        self.assertEqual(cache.incr("counter", 2), 12)
        self.assertEqual(cache.decr("counter"), 11)
        self.assertTrue(cache.touch("counter", timeout=None))
        cache.set("zero", "gone", timeout=0)
        self.assertIsNone(cache.get("zero"))
        cache.set_many({"permanent": "value"}, timeout=None)
        self.assertEqual(cache.get("permanent"), "value")
        cache.delete_many(["one", "two"])
        self.assertEqual(cache.get_many(["one", "two"]), {})
        self.assertTrue(cache.delete("unicode"))

    def test_fastapi(self):
        from examples.fastapi_app import app
        self.kv.delete("demo:fastapi:v1:summary")
        async def check():
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
                    self.assertEqual((await client.get("/summary")).json()["source"], "computed")
                    self.assertEqual((await client.get("/summary")).json()["source"], "cache")
                    self.kv.set("demo:fastapi:v1:summary", "invalid-json")
                    self.assertEqual((await client.get("/summary")).json()["source"], "computed")
            with patch.dict(os.environ, {"EXPENSE_KV_PASSWORD": "wrong"}):
                async with app.router.lifespan_context(app):
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
                        self.assertEqual((await client.get("/summary")).json()["source"], "computed")
        asyncio.run(check())

    def test_flask(self):
        from examples import flask_app as demo
        self.addCleanup(demo.kv.close)
        self.kv.delete("demo:flask:v1:summary")
        with demo.app.test_client() as client:
            self.assertEqual(client.get("/summary").json["source"], "computed")
            self.assertEqual(client.get("/summary").json["source"], "cache")
            self.kv.set("demo:flask:v1:summary", "invalid-json")
            self.assertEqual(client.get("/summary").json["source"], "computed")
            with Redis.from_url(os.environ["EXPENSE_KV_URL"], password="wrong", protocol=2,
                                retry=Retry(NoBackoff(), 0)) as wrong:
                with patch.object(demo, "kv", wrong):
                    self.assertEqual(client.get("/summary").json["source"], "computed")

    def test_express(self):
        self.kv.delete("demo:express:v1:summary")
        root = Path(__file__).resolve().parents[1]
        for password, expected in [(os.environ["EXPENSE_KV_PASSWORD"], "cache"), ("wrong", "computed")]:
            port = free_port()
            with tempfile.TemporaryFile() as log:
                process = subprocess.Popen(["node", str(root / "examples/express.mjs")],
                                           env=dict(os.environ, PORT=str(port), EXPENSE_KV_PASSWORD=password),
                                           stdout=log, stderr=log)
                try:
                    deadline = time.monotonic() + 10
                    while True:
                        try:
                            with urlopen(f"http://127.0.0.1:{port}/summary", timeout=3) as response:
                                first = json.load(response)
                            break
                        except OSError:
                            if process.poll() is not None or time.monotonic() > deadline:
                                log.seek(0)
                                raise RuntimeError(log.read().decode())
                            time.sleep(0.02)
                    self.assertEqual(first["source"], "computed")
                    with urlopen(f"http://127.0.0.1:{port}/summary", timeout=3) as response:
                        self.assertEqual(json.load(response)["source"], expected)
                finally:
                    if process.poll() is None:
                        process.terminate()
                        process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
