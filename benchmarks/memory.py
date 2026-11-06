#!/usr/bin/env python3
"""Measure isolated servers with identical data; all memory sizes are bytes."""
import argparse
import ctypes
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import shutil
import socket
import statistics
import subprocess
import sys
import threading
import time
from contextlib import ExitStack

from compare import frame, server_process


class RUsageV4(ctypes.Structure):
    # ABI from macOS SDK sys/resource.h, struct rusage_info_v4.
    _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [
        (name, ctypes.c_uint64) for name in """
        user_time system_time pkg_idle_wkups interrupt_wkups pageins wired_size
        resident_size phys_footprint proc_start_abstime proc_exit_abstime
        child_user_time child_system_time child_pkg_idle_wkups child_interrupt_wkups
        child_pageins child_elapsed_abstime diskio_bytesread diskio_byteswritten
        cpu_time_qos_default cpu_time_qos_maintenance cpu_time_qos_background
        cpu_time_qos_utility cpu_time_qos_legacy cpu_time_qos_user_initiated
        cpu_time_qos_user_interactive billed_system_time serviced_system_time
        logical_writes lifetime_max_phys_footprint instructions cycles billed_energy
        serviced_energy interval_max_phys_footprint runnable_time
        """.split()
    ]


if sys.platform == "darwin":
    LIBPROC = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    LIBPROC.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    LIBPROC.proc_pid_rusage.restype = ctypes.c_int


def linux_memory(status):
    fields = dict(line.split(":", 1) for line in status.splitlines() if ":" in line)
    return {"rss_bytes": int(fields["VmRSS"].split()[0]) * 1024,
            "rss_high_water_bytes": int(fields["VmHWM"].split()[0]) * 1024}


def process_memory(pid):
    if sys.platform == "darwin":
        usage = RUsageV4()
        if LIBPROC.proc_pid_rusage(pid, 4, ctypes.byref(usage)) != 0:
            raise OSError(ctypes.get_errno(), "proc_pid_rusage failed", pid)
        return {"rss_bytes": usage.resident_size,
                "physical_footprint_bytes": usage.phys_footprint,
                "lifetime_peak_footprint_bytes": usage.lifetime_max_phys_footprint}
    if sys.platform.startswith("linux"):
        return linux_memory(Path(f"/proc/{pid}/status").read_text())
    raise RuntimeError("memory measurements support macOS and Linux")


def system_memory():
    if sys.platform == "darwin":
        return {name: subprocess.check_output(command, text=True).strip()
                for name, command in (("vm_stat", ["vm_stat"]),
                                      ("swapusage", ["sysctl", "vm.swapusage"]),
                                      ("total_bytes", ["sysctl", "-n", "hw.memsize"]))}
    return {"meminfo": Path("/proc/meminfo").read_text()}


class Client:
    def __init__(self, port):
        self.socket = socket.create_connection(("127.0.0.1", port), timeout=60)
        self.reader = self.socket.makefile("rb")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.reader.close()
        self.socket.close()

    def reply(self):
        line = self.reader.readline()
        if not line.endswith(b"\r\n"):
            raise RuntimeError(f"truncated server response: {line!r}")
        kind, value = line[:1], line[1:-2]
        if kind == b"-":
            raise RuntimeError(value.decode(errors="replace"))
        if kind == b"+":
            return value
        if kind == b":":
            return int(value)
        if kind == b"$":
            size = int(value)
            if size == -1:
                return None
            data = self.reader.read(size)
            if len(data) != size or self.reader.read(2) != b"\r\n":
                raise RuntimeError("truncated bulk response")
            return data
        raise RuntimeError(f"unsupported response: {line!r}")

    def request(self, *args):
        self.socket.sendall(frame(*args))
        return self.reply()


def snapshot(pid):
    time.sleep(0.1)
    samples = []
    for _ in range(5):
        samples.append(process_memory(pid))
        time.sleep(0.02)
    return {key: int(statistics.median(row[key] for row in samples)) for key in samples[0]}


def monitored(pid, action):
    samples = [process_memory(pid)]
    errors = []
    stopped = threading.Event()

    def sample():
        while not stopped.wait(0.02):
            try:
                samples.append(process_memory(pid))
            except Exception as error:
                errors.append(error)
                return

    sampler = threading.Thread(target=sample)
    sampler.start()
    started = time.monotonic()
    try:
        result = action()
    finally:
        stopped.set()
        sampler.join()
    if errors:
        raise errors[0]
    samples.append(process_memory(pid))
    return result, dict(duration_seconds=time.monotonic() - started, samples=len(samples),
                        **{f"max_{key}": max(row[key] for row in samples) for key in samples[0]})


def key(index):
    return f"bench:{index:08d}".encode()


def load(port, count, size, ttl):
    rng = random.Random(42)
    batch_size = min(256, (1024 * 1024) // (size + 128))
    digest = hashlib.sha256()
    first = last = None
    with Client(port) as client:
        for start in range(0, count, batch_size):
            pairs = []
            for index in range(start, min(start + batch_size, count)):
                value = rng.randbytes(size)
                first = value if first is None else first
                last = value
                k = key(index)
                digest.update(k)
                digest.update(value)
                pairs.extend((k, value))
            if ttl:
                requests = [frame(b"SET", *pairs[i:i + 2], b"PX", b"600000")
                            for i in range(0, len(pairs), 2)]
                client.socket.sendall(b"".join(requests))
                for _ in requests:
                    assert client.reply() == b"OK"
            else:
                assert client.request(b"MSET", *pairs) == b"OK"
        assert client.request(b"DBSIZE") == count
        assert client.request(b"GET", key(0)) == first
        assert client.request(b"GET", key(count - 1)) == last
        if ttl:
            assert client.request(b"PTTL", key(0)) > 0
    return digest.hexdigest()


def internal_memory(port, count, redis):
    with Client(port) as client:
        assert client.request(b"DBSIZE") == count
        info = client.request(b"INFO").decode()
    fields = dict(line.split(":", 1) for line in info.splitlines()
                  if line and not line.startswith("#") and ":" in line)
    name = "used_memory" if redis else "used_memory_estimate"
    return {"metric": name, "bytes": int(fields[name]),
            "maxmemory_bytes": int(fields["maxmemory"])}


def get_load(port, seconds):
    command = ["redis-benchmark", "-h", "127.0.0.1", "-p", str(port), "-c", "50",
               "-P", "16", "-n", "1000000000", "--csv", "GET", key(0).decode()]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        try:
            stdout, stderr = process.communicate(timeout=seconds)
            raise RuntimeError(f"benchmark exited before its measurement window: {stdout} {stderr}")
        except subprocess.TimeoutExpired:
            process.terminate()
            stdout, stderr = process.communicate(timeout=5)
        if "Error" in stdout or "Error" in stderr or "Could not connect" in stderr:
            raise RuntimeError(stdout + stderr)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def experiment(binary, redis, count, size, ttl, seconds):
    with server_process(binary, redis) as (port, pid):
        empty = snapshot(pid)
        digest, loading = monitored(pid, lambda: load(port, count, size, ttl))
        internal = internal_memory(port, count, redis)
        loaded = snapshot(pid)
        with ExitStack() as clients:
            for _ in range(50):
                client = clients.enter_context(Client(port))
                assert client.request(b"PING") == b"PONG"
            idle_clients = snapshot(pid)
        snapshot(pid)
        _, active = monitored(pid, lambda: get_load(port, seconds))
        after_get = snapshot(pid)
        assert internal_memory(port, count, redis)["bytes"] >= 0
        with Client(port) as client:
            assert client.request(b"FLUSHDB") == b"OK"
        flushed_internal = internal_memory(port, 0, redis)
        flushed = snapshot(pid)
        return dict(dataset_sha256=digest, empty=empty, loaded=loaded, internal=internal,
                    loading=loading, idle_50_clients=idle_clients, get_50_clients=active,
                    after_get=after_get, flushed=flushed, flushed_internal=flushed_internal,
                    incremental_rss_bytes_per_key=(loaded["rss_bytes"] - empty["rss_bytes"]) / count)


def write_report(report, output):
    def mib(value):
        return f"{value / (1024 * 1024):.2f}"

    lines = ["# Сравнение памяти ExpenseKV и Redis", "",
             f"UTC: {report['timestamp_utc']}. Платформа: `{report['platform']}`.",
             f"Redis: `{report['redis']}`. Число прогонов: {report['runs']}.", "",
             "В таблицах медианы отдельных показателей по прогонам; MiB = 1 048 576 байт.", "",
             "| Ключей | Значение | TTL | Сервер | RSS пустого | RSS с данными | RSS 50 idle | Максимум RSS при GET | RSS после FLUSHDB |",
             "|---:|---:|---|---|---:|---:|---:|---:|---:|"]
    for row in report["summary"]:
        values = [row[name] for name in ("empty_rss", "loaded_rss", "idle_rss", "get_peak_rss", "flushed_rss")]
        lines.append(f"| {row['keys']} | {row['value_bytes']} B | {'да' if row['ttl'] else 'нет'} | {row['engine']} | " +
                     " | ".join(mib(value) for value in values) + " |")
    if sys.platform == "darwin":
        lines += ["", "## Physical footprint macOS, MiB", "",
                  "| Ключей | Значение | TTL | Сервер | С данными | При GET, максимум выборки | Пик за жизнь процесса |",
                  "|---:|---:|---|---|---:|---:|---:|"]
        for row in report["summary"]:
            lines.append(f"| {row['keys']} | {row['value_bytes']} B | {'да' if row['ttl'] else 'нет'} | {row['engine']} | " +
                         " | ".join(mib(row[k]) for k in ("loaded_footprint", "get_peak_footprint", "lifetime_peak_footprint")) + " |")
    lines += ["", "## Внутренний учёт и стоимость данных", "",
              "Эти внутренние метрики имеют разную методику: used_memory_estimate ExpenseKV — оценка записей; used_memory Redis — учёт аллокатора. Они не заменяют RSS и не используются для определения победителя.", "",
              "| Ключей | Значение | TTL | Сервер | Ключи + значения, MiB | Внутренний учёт, MiB | Прирост RSS / ключ, B |",
              "|---:|---:|---|---|---:|---:|---:|"]
    for row in report["summary"]:
        lines.append(f"| {row['keys']} | {row['value_bytes']} B | {'да' if row['ttl'] else 'нет'} | {row['engine']} | " +
                     f"{mib(row['payload_bytes'])} | {mib(row['internal_bytes'])} | {row['rss_bytes_per_key']:.1f} |")
    lines += ["", "## Методика и пределы вывода", "",
              "Для каждого прогона создаются новые процессы и временные каталоги. Порядок серверов чередуется. Данные идентичны: ключи bench:00000000 длиной 14 байт, уникальные бинарные значения из Random(42); совпадение SHA256 проверяется. До и после GET проверяется число ключей, проверяются первое/последнее значения и TTL. Eviction отсутствует.", "",
              "Наборы без TTL загружаются MSET с пакетами до 1 MiB полезных данных; с TTL — SET PX 600000. Загрузка входит в lifetime peak, но таблица RSS с данными снята после её завершения. TTL не истекает в течение опыта.", "",
              "Поскольку последовательности загрузки TTL и без TTL различаются, разницу между этими сценариями нельзя целиком приписывать стоимости TTL-индекса. Между двумя серверами внутри одного сценария данные и последовательность команд одинаковы.", "",
              f"Активная нагрузка: {report['get_seconds']} с GET существующего ключа, 50 постоянных соединений, pipeline 16. Клиент завершается после окна измерения. Эта серия измеряет память; throughput из неё не выводится. Пиковый RSS — максимум выборки с интервалом 20 мс и замерами до/после, поэтому короткий всплеск может быть пропущен. Пик footprint macOS берётся из счётчика ОС за всю жизнь процесса, включая загрузку.", "",
              "macOS: RSS и physical footprint получены через proc_pid_rusage, ABI rusage_info_v4. Linux: VmRSS и VmHWM из /proc/<pid>/status. RSS включает резидентные страницы процесса и может включать общие страницы; это не размер виртуального адресного пространства. Клиент, Python-измеритель, файловый кэш ОС и сетевые буферы ядра не входят в показатели сервера. Резидентные нативные стеки, пользовательские сетевые буферы, код и накладные расходы аллокатора входят в RSS. Physical footprint — отдельная учётная метрика macOS; её не следует приравнивать к RSS.", "",
              "При сжатии или вытеснении страниц RSS может оказаться меньше объёма данных и сильно меняться между прогонами. Для macOS поэтому приведён также physical footprint. Состояние памяти ОС и swap в конце серии записано в JSON; RSS не следует интерпретировать как размер всех выделений или сравнивать без учёта давления памяти.", "",
              "Оба сервера имеют лимит внутреннего учёта 64 MiB; он не означает лимит RSS. AOF включён, fsync перед подтверждением; RDB и автоматический rewrite Redis отключены, io-threads 1. У ExpenseKV порог компактации 1 GiB. SAVE, компактация, рестарт, медленные потребители и предельные 128 клиентов здесь не измеряются. После FLUSHDB таблицы, аллокатор и кеш потоков могут сохранять ранее выделенную память; один такой замер не доказывает утечку.", "",
              "## Повторение", "", "```sh", "cargo build --release --locked",
              "python3 benchmarks/memory.py", "```", "",
              f"Сырые результаты и SHA256 бинарников: [{output.name}]({output.name}).",
              "Определения Redis: [INFO memory](https://redis.io/docs/latest/commands/info/).", ""]
    output.with_suffix(".md").write_text("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/release/expense-kv")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--get-seconds", type=float, default=2)
    parser.add_argument("--output", type=Path, default=Path("benchmarks/memory-results.json"))
    args = parser.parse_args()
    if args.runs < 1 or not math.isfinite(args.get_seconds) or args.get_seconds < 0.5:
        parser.error("runs must be positive; get-seconds must be >= 0.5")
    for name in ("redis-server", "redis-benchmark"):
        if not shutil.which(name):
            parser.error(f"{name} must be installed")
    engines = [("expense-kv", args.binary, False),
               ("redis", shutil.which("redis-server"), True)]
    report = dict(platform=platform.platform(), timestamp_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                  redis=subprocess.check_output(["redis-server", "--version"], text=True).strip(),
                  runs=args.runs, get_seconds=args.get_seconds, samples=[], summary=[],
                  binaries_sha256={name: hashlib.sha256(Path(binary).read_bytes()).hexdigest()
                                   for name, binary, _ in engines})
    report["system_memory_start"] = system_memory()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for count, size, ttl in [(10000, 64, False), (50000, 64, False),
                             (10000, 4096, False), (512, 65536, False), (10000, 64, True)]:
        hashes = set()
        for run in range(args.runs):
            for name, binary, redis in engines[::1 if run % 2 == 0 else -1]:
                result = experiment(binary, redis, count, size, ttl, args.get_seconds)
                hashes.add(result["dataset_sha256"])
                assert len(hashes) == 1, "datasets differ"
                report["samples"].append(dict(engine=name, run=run + 1, keys=count,
                                               value_bytes=size, ttl=ttl, **result))
                args.output.write_text(json.dumps(report, indent=2) + "\n")
                print(json.dumps(dict(engine=name, run=run + 1, keys=count, value_bytes=size,
                                     ttl=ttl, loaded_rss=result["loaded"]["rss_bytes"],
                                     get_peak_rss=result["get_50_clients"]["max_rss_bytes"])), flush=True)
        for name, _, _ in engines:
            samples = [s for s in report["samples"] if s["engine"] == name and s["keys"] == count
                       and s["value_bytes"] == size and s["ttl"] == ttl]
            fields = {"empty_rss": ("empty", "rss_bytes"), "loaded_rss": ("loaded", "rss_bytes"),
                      "idle_rss": ("idle_50_clients", "rss_bytes"),
                      "get_peak_rss": ("get_50_clients", "max_rss_bytes"),
                      "flushed_rss": ("flushed", "rss_bytes"), "internal_bytes": ("internal", "bytes")}
            if sys.platform == "darwin":
                fields.update(loaded_footprint=("loaded", "physical_footprint_bytes"),
                              get_peak_footprint=("get_50_clients", "max_physical_footprint_bytes"),
                              lifetime_peak_footprint=("flushed", "lifetime_peak_footprint_bytes"))
            row = dict(engine=name, keys=count, value_bytes=size, ttl=ttl,
                       payload_bytes=count * (len(key(0)) + size),
                       rss_bytes_per_key=statistics.median(s["incremental_rss_bytes_per_key"] for s in samples),
                       **{label: statistics.median(s[phase][metric] for s in samples)
                          for label, (phase, metric) in fields.items()})
            report["summary"].append(row)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        write_report(report, args.output)
    report["system_memory_end"] = system_memory()
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    write_report(report, args.output)


if __name__ == "__main__":
    main()
