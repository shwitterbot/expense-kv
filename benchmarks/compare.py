#!/usr/bin/env python3
"""Compare isolated local servers; never connect to an existing database."""
import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import statistics
import subprocess
import tempfile
import time
from contextlib import ExitStack, contextmanager


def frame(*args):
    return b"*%d\r\n" % len(args) + b"".join(
        b"$%d\r\n" % len(arg) + arg + b"\r\n" for arg in args
    )


@contextmanager
def server(binary, redis=False):
    with server_process(binary, redis) as (port, _):
        yield port


@contextmanager
def server_process(binary, redis=False):
    with tempfile.TemporaryDirectory(prefix="expense-kv-benchmark-") as directory:
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        if redis:
            command = [binary, "--bind", "127.0.0.1", "--port", str(port),
                       "--dir", directory, "--save", "", "--appendonly", "yes",
                       "--appendfsync", "always", "--auto-aof-rewrite-percentage", "0",
                       "--maxmemory", "64mb", "--maxmemory-policy", "noeviction",
                       "--io-threads", "1"]
        else:
            command = [str(Path(binary).resolve())]
        environment = dict(os.environ, KV_BIND=f"127.0.0.1:{port}",
                           KV_DATA_DIR=directory, KV_PASSWORD="", KV_MAX_BYTES="67108864",
                           KV_MAX_CLIENTS="128", KV_TIMEOUT_SECONDS="30",
                           KV_COMPACT_BYTES="1073741824")
        with tempfile.TemporaryFile() as log:
            process = subprocess.Popen(command, env=environment, stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        log.seek(0)
                        raise RuntimeError(log.read().decode(errors="replace"))
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.2) as client:
                            client.sendall(frame(b"PING"))
                            assert client.makefile("rb").readline() == b"+PONG\r\n"
                        break
                    except OSError:
                        time.sleep(0.02)
                else:
                    raise RuntimeError("server did not become ready")
                yield port, process.pid
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()


def seed(port, size):
    with socket.create_connection(("127.0.0.1", port), timeout=5) as client:
        client.sendall(frame(b"SET", b"bench:read", b"x" * size))
        assert client.makefile("rb").readline() == b"+OK\r\n"


def measure(port, operation, clients, pipeline, size, requests):
    args = ["redis-benchmark", "-h", "127.0.0.1", "-p", str(port),
            "-c", str(clients), "-P", str(pipeline), "-n", str(requests), "--csv"]
    if operation == "GET":
        seed(port, size)
        args += ["GET", "bench:read"]
    else:
        args += ["SET", "bench:write", "x" * size]
    result = subprocess.run(args, capture_output=True, text=True, check=True, timeout=120)
    rows = list(csv.DictReader(io.StringIO(result.stdout)))
    if len(rows) != 1 or not rows[0]["test"].startswith(operation):
        raise RuntimeError(f"unexpected benchmark output: {result.stdout} {result.stderr}")
    if "Error from server" in result.stderr or "Error from server" in result.stdout:
        raise RuntimeError(result.stderr + result.stdout)
    return {key: float(rows[0][key]) for key in ("rps", "avg_latency_ms", "p99_latency_ms")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/release/expense-kv")
    parser.add_argument("--baseline-binary")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--read-requests", type=int, default=50000)
    parser.add_argument("--write-requests", type=int, default=600)
    parser.add_argument("--operations", nargs="+", choices=("GET", "SET"), default=("GET", "SET"))
    parser.add_argument("--cases", nargs="+", choices=("single", "concurrent", "pipeline"),
                        default=("single", "concurrent", "pipeline"))
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results.json"))
    args = parser.parse_args()
    if min(args.runs, args.read_requests, args.write_requests) <= 0:
        parser.error("runs and request counts must be positive")
    for name in ("redis-server", "redis-benchmark"):
        if not shutil.which(name):
            parser.error(f"{name} must be installed")
    engines = [("expense-kv", args.binary, False),
               ("redis-aof-always", shutil.which("redis-server"), True)]
    if args.baseline_binary:
        engines.insert(0, ("expense-kv-before", args.baseline_binary, False))
    report = {
        "platform": platform.platform(), "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "redis": subprocess.check_output(["redis-server", "--version"], text=True).strip(),
        "benchmark": subprocess.check_output(["redis-benchmark", "--version"], text=True).strip(),
        "binaries_sha256": {label: hashlib.sha256(Path(binary).read_bytes()).hexdigest()
                            for label, binary, _ in engines},
        "workload": "GET reads one hot key; SET overwrites another; TCP loopback; writes sync before ACK",
        "runs": args.runs, "samples": [], "summary": [],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with ExitStack() as stack:
        ports = {label: stack.enter_context(server(binary, redis)) for label, binary, redis in engines}
        for size in (64, 4096):
            for case in args.cases:
                clients, pipeline = {"single": (1, 1), "concurrent": (50, 1),
                                     "pipeline": (50, 16)}[case]
                for operation in args.operations:
                    requests = args.read_requests if operation == "GET" else args.write_requests
                    requests = max(clients * pipeline, requests)
                    samples = {label: [] for label in ports}
                    for run in range(args.runs):
                        labels = list(ports)
                        labels = labels[run % len(labels):] + labels[:run % len(labels)]
                        for label in labels:
                            metrics = measure(ports[label], operation, clients, pipeline, size, requests)
                            sample = dict(engine=label, operation=operation, clients=clients,
                                          pipeline=pipeline, value_bytes=size, requests=requests,
                                          run=run + 1, **metrics)
                            samples[label].append(metrics)
                            report["samples"].append(sample)
                            args.output.write_text(json.dumps(report, indent=2) + "\n")
                    for label in ports:
                        summary = dict(engine=label, operation=operation, clients=clients,
                                       pipeline=pipeline, value_bytes=size, requests=requests,
                                       **{key: statistics.median(m[key] for m in samples[label])
                                          for key in samples[label][0]})
                        report["summary"].append(summary)
                        print(json.dumps(summary), flush=True)
                    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
