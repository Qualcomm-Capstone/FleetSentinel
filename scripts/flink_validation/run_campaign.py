#!/usr/bin/env python3
"""Run fixed-arrival Flink steady and recoverable sink-outage experiments.

This is an acceptance campaign for the local single-host Compose stack. It is
not a Kafka/Flink capacity benchmark. The producer is open-loop: records are
scheduled by wall clock and generator lag invalidates a run instead of silently
reducing the offered rate.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
import json
import math
from pathlib import Path
import platform
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.parse
import uuid

from confluent_kafka import Producer
from fastavro import schemaless_writer

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "exploration"))
sys.path.insert(0, str(ROOT / "exploration" / "scripts"))

from fleetsentinel_ingest.nuscenes_source import extract_native_signals  # noqa: E402
from ship_to_gateway import _signal_row  # noqa: E402
from nuscenes.nuscenes import NuScenes  # noqa: E402
from nuscenes.can_bus.can_bus_api import NuScenesCanBus  # noqa: E402


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def get_json(url: str) -> dict:
    return json.load(urllib.request.urlopen(url, timeout=10))


def percentile(values: list[int], q: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(len(ordered) * q) - 1)]


@dataclass
class Metrics:
    lock: threading.Lock = field(default_factory=threading.Lock)
    phase: str = "preflight"
    run: str = "not-started"
    fault_active: int = 0
    sent: int = 0
    stored: int = 0
    max_schedule_lag_ms: float = 0.0
    alert_latency_p95_ms: float = 0.0
    passed: int = 0

    def render(self) -> bytes:
        with self.lock:
            phase = self.phase.replace('"', '')
            run = self.run.replace('"', '')
            rows = [
                "# HELP fs_validation_info Current validation run.",
                "# TYPE fs_validation_info gauge",
                f'fs_validation_info{{run="{run}"}} 1',
                "# HELP fs_validation_phase Current validation phase.",
                "# TYPE fs_validation_phase gauge",
                f'fs_validation_phase{{phase="{phase}"}} 1',
                "# TYPE fs_validation_fault_active gauge",
                f"fs_validation_fault_active {self.fault_active}",
                "# TYPE fs_validation_records_sent_total counter",
                f"fs_validation_records_sent_total {self.sent}",
                "# TYPE fs_validation_records_stored gauge",
                f"fs_validation_records_stored {self.stored}",
                "# TYPE fs_validation_schedule_lag_ms gauge",
                f"fs_validation_schedule_lag_ms {self.max_schedule_lag_ms:.3f}",
                "# TYPE fs_validation_alert_latency_p95_ms gauge",
                f"fs_validation_alert_latency_p95_ms {self.alert_latency_p95_ms:.3f}",
                "# TYPE fs_validation_pass gauge",
                f"fs_validation_pass {self.passed}",
                "",
            ]
            return "\n".join(rows).encode()


METRICS = Metrics()


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/metrics":
            self.send_error(404)
            return
        body = METRICS.render()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args) -> None:
        return


def clickhouse_count(boot: str) -> int:
    query = (
        "SELECT count() AS n FROM fleet.signals FINAL "
        f"WHERE boot_id = '{boot}' FORMAT JSON"
    )
    request = urllib.request.Request(
        "http://localhost:8124/?user=fleet&password=fleet", data=query.encode()
    )
    return int(get_json_request(request)["data"][0]["n"])


def get_json_request(request: urllib.request.Request) -> dict:
    return json.load(urllib.request.urlopen(request, timeout=10))


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", "-f", str(ROOT / "infra/docker-compose.yml"), *args],
        cwd=ROOT,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def inspect_containers(names: list[str]) -> dict[str, dict]:
    process = subprocess.run(
        ["docker", "inspect", *names],
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    inspected = json.loads(process.stdout)
    return {
        item["Name"].lstrip("/"): {
            "status": item["State"]["Status"],
            "running": item["State"]["Running"],
            "oomKilled": item["State"]["OOMKilled"],
            "restartCount": item["RestartCount"],
            "startedAt": item["State"]["StartedAt"],
        }
        for item in inspected
    }


def consumer_group_lag(group: str) -> int:
    process = compose(
        "exec",
        "-T",
        "kafka1",
        "env",
        "KAFKA_OPTS=",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--describe",
        "--group",
        group,
    )
    lags = []
    for line in process.stdout.splitlines():
        columns = line.split()
        if len(columns) >= 6 and columns[0] == group and columns[5].isdigit():
            lags.append(int(columns[5]))
    if not lags:
        raise RuntimeError(f"no readable lag rows for consumer group {group}: {process.stdout}")
    return sum(lags)


def prometheus_range(query: str, start_ms: int, end_ms: int, step_seconds: float) -> dict:
    params = urllib.parse.urlencode(
        {
            "query": query,
            "start": f"{start_ms / 1000:.3f}",
            "end": f"{end_ms / 1000:.3f}",
            "step": step_seconds,
        }
    )
    response = get_json(f"http://127.0.0.1:19098/api/v1/query_range?{params}")
    series = response["data"]["result"]
    values = [point for item in series for point in item.get("values", [])]
    timestamps = sorted(float(point[0]) for point in values)
    numeric = [float(point[1]) for point in values]
    max_gap_ms = max(
        ((right - left) * 1000 for left, right in zip(timestamps, timestamps[1:])),
        default=0.0,
    )
    return {
        "query": query,
        "series": len(series),
        "samples": len(values),
        "min": min(numeric) if numeric else None,
        "max": max(numeric) if numeric else None,
        "maxGapMs": max_gap_ms,
    }


def preflight(out: Path, api_url: str) -> tuple[dict[str, str], dict[str, int], dict[str, int]]:
    overview = get_json("http://localhost:8081/overview")
    running = [
        job
        for job in get_json("http://localhost:8081/jobs/overview")["jobs"]
        if job["state"] == "RUNNING"
    ]
    expected_names = {"fleetsentinel-alerts", "fleetsentinel-storage"}
    jobs = {job["name"]: job["jid"] for job in running if job["name"] in expected_names}
    if set(jobs) != expected_names or len(running) != 2:
        raise RuntimeError(f"expected exactly the independent alert/storage jobs, got {running}")
    checkpoints = {
        name: get_json(f"http://localhost:8081/jobs/{job_id}/checkpoints")
        for name, job_id in jobs.items()
    }
    for name, status in checkpoints.items():
        if status["counts"]["completed"] < 5:
            raise RuntimeError(f"{name}: fewer than five completed checkpoints before measurement")
        if status["counts"]["failed"] != 0:
            raise RuntimeError(
                f"{name}: checkpoint failures before measurement: {status['counts']['failed']}"
            )
    consumer_lag = {
        "fleetsentinel-alerts": consumer_group_lag("fleetsentinel-alerts"),
        "fleetsentinel-storage": consumer_group_lag("fleetsentinel-storage"),
    }
    if any(lag != 0 for lag in consumer_lag.values()):
        raise RuntimeError(f"consumer groups have startup backlog: {consumer_lag}")
    targets = {}
    for name, url in {
        "jobmanager": "http://localhost:9249/metrics",
        "taskmanager": "http://localhost:9250/metrics",
        "api": f"{api_url}/actuator/health",
        "clickhouse": "http://localhost:8124/ping",
    }.items():
        try:
            body = urllib.request.urlopen(url, timeout=5).read(1024).decode(errors="replace")
            targets[name] = {"up": True, "sample": body[:160]}
        except Exception as error:  # evidence, not a hidden retry
            targets[name] = {"up": False, "error": str(error)}
    if not all(v["up"] for v in targets.values()):
        raise RuntimeError(f"required observability target down: {targets}")
    time.sleep(2)
    query = urllib.parse.urlencode({"query": 'up{job=~"flink-jobmanager|flink-taskmanager|flink-validation-harness"}'})
    prom = get_json(f"http://127.0.0.1:19098/api/v1/query?{query}")
    observed = {item["metric"].get("job"): float(item["value"][1]) for item in prom["data"]["result"]}
    required = {"flink-jobmanager", "flink-taskmanager", "flink-validation-harness"}
    if set(observed) != required or any(observed[name] != 1 for name in required):
        raise RuntimeError(f"Prometheus scrape gate failed: {observed}")
    runtime = inspect_containers(
        [
            "fleet-jobmanager",
            "fleet-taskmanager",
            "fleet-clickhouse",
            "fleet-kafka1",
            "fleet-kafka2",
            "fleet-kafka3",
        ]
    )
    unhealthy = {
        name: state
        for name, state in runtime.items()
        if not state["running"] or state["oomKilled"]
    }
    if unhealthy:
        raise RuntimeError(f"container runtime gate failed: {unhealthy}")
    containers = json.loads("[" + ",".join(compose("ps", "--format", "json").stdout.splitlines()) + "]")
    fingerprint = {
        "recordedAtMs": now_ms(),
        "scope": "single-host Docker Compose; load generator shares the host, so no capacity claim",
        "platform": platform.platform(),
        "python": platform.python_version(),
        "flink": overview,
        "jobs": jobs,
        "completedCheckpoints": {
            name: status["counts"]["completed"] for name, status in checkpoints.items()
        },
        "consumerLag": consumer_lag,
        "targets": targets,
        "prometheusUp": observed,
        "containerRuntime": runtime,
        "containers": [
            {"service": c["Service"], "image": c["Image"], "state": c["State"], "health": c["Health"]}
            for c in containers
        ],
    }
    (out / "preflight.json").write_text(json.dumps(fingerprint, ensure_ascii=False, indent=2))
    restart_counts = {}
    failed_checkpoint_counts = {}
    for name, job_id in jobs.items():
        restarts = get_json(f"http://localhost:8081/jobs/{job_id}/metrics?get=numRestarts")
        restart_counts[name] = int(restarts[0]["value"]) if restarts else 0
        failed_checkpoint_counts[name] = checkpoints[name]["counts"]["failed"]
    return jobs, restart_counts, failed_checkpoint_counts


class AlertCollector:
    def __init__(self, prefix: str, api_url: str):
        self.prefix = prefix
        self.api_url = api_url
        self.events: list[dict] = []
        self.errors: list[str] = []
        self.stop = threading.Event()

    def run(self) -> None:
        try:
            query = urllib.parse.urlencode({"vehicle": self.prefix})
            with urllib.request.urlopen(f"{self.api_url}/api/alerts?{query}", timeout=600) as response:
                for line in response:
                    if self.stop.is_set():
                        return
                    if line.startswith(b"data:"):
                        event = json.loads(line[5:])
                        if event["vehicleId"].startswith(self.prefix):
                            self.events.append({"receivedAtMs": now_ms(), **event})
        except Exception as error:
            if not self.stop.is_set():
                self.errors.append(str(error))


def templates(run: str) -> list[dict]:
    nusc = NuScenes(version="v1.0-mini", dataroot=str(ROOT / "data/nuscenes"), verbose=False)
    can = NuScenesCanBus(dataroot=str(ROOT / "data/nuscenes"))
    return [_signal_row(s) for s in extract_native_signals(nusc, nusc.scene[0], run, can)]


def run_load(
    *,
    out: Path,
    label: str,
    rate: int,
    warmup_seconds: int,
    measure_seconds: int,
    api_url: str,
    topic: str,
    fault_at_seconds: int | None = None,
    fault_seconds: int = 0,
) -> dict:
    run = f"{label}-{uuid.uuid4().hex[:8]}"
    vehicle = run
    schema = json.loads((ROOT / "schemas/vehicle-signal.avsc").read_text())
    rows = templates(run)
    producer = Producer(
        {
            "bootstrap.servers": "localhost:29092,localhost:29093,localhost:29094",
            "acks": "all",
            "enable.idempotence": True,
            "broker.address.family": "v4",
            "linger.ms": 5,
            "queue.buffering.max.messages": max(100_000, rate * 30),
        }
    )
    alerts = AlertCollector(run, api_url)
    threading.Thread(target=alerts.run, daemon=True).start()
    errors: list[str] = []
    points: list[dict] = []
    finished = threading.Event()
    sent = 0

    with METRICS.lock:
        METRICS.run = run
        METRICS.phase = "warmup" if warmup_seconds else label
        METRICS.fault_active = 0
        METRICS.sent = 0
        METRICS.stored = 0
        METRICS.max_schedule_lag_ms = 0
        METRICS.alert_latency_p95_ms = 0
        METRICS.passed = 0

    def poll_storage() -> None:
        while not finished.is_set():
            try:
                stored = clickhouse_count(run)
                with METRICS.lock:
                    METRICS.stored = stored
                    snapshot = {
                        "atMs": now_ms(),
                        "phase": METRICS.phase,
                        "fault": METRICS.fault_active,
                        "sent": METRICS.sent,
                        "stored": stored,
                    }
                points.append(snapshot)
            except Exception as error:
                points.append({"atMs": now_ms(), "phase": METRICS.phase, "storageError": str(error)})
            finished.wait(1)

    threading.Thread(target=poll_storage, daemon=True).start()
    total_seconds = warmup_seconds + measure_seconds
    total = total_seconds * rate
    began_mono = time.monotonic()
    began_ms = now_ms()
    fault_state = {
        "started_ms": None,
        "ended_ms": None,
        "faulted": False,
        "unpaused": False,
    }

    def inject_fault() -> None:
        """Inject the sink outage without blocking the open-loop producer clock."""
        assert fault_at_seconds is not None
        time.sleep(max(0.0, began_mono + fault_at_seconds - time.monotonic()))
        try:
            compose("pause", "clickhouse")
            fault_state["faulted"] = True
            fault_state["started_ms"] = now_ms()
            with METRICS.lock:
                METRICS.phase = "sink-outage"
                METRICS.fault_active = 1
            time.sleep(fault_seconds)
            compose("unpause", "clickhouse")
            fault_state["unpaused"] = True
            fault_state["ended_ms"] = now_ms()
            with METRICS.lock:
                METRICS.phase = "recovery-drain"
                METRICS.fault_active = 0
        except Exception as error:
            errors.append(f"fault injection: {error}")
        finally:
            if fault_state["faulted"] and not fault_state["unpaused"]:
                compose("unpause", "clickhouse", check=False)
                fault_state["unpaused"] = True
                fault_state["ended_ms"] = now_ms()
                with METRICS.lock:
                    METRICS.fault_active = 0

    fault_thread = None
    if fault_at_seconds is not None:
        fault_thread = threading.Thread(target=inject_fault, daemon=True)
        fault_thread.start()

    def delivery(error, _message) -> None:
        if error:
            errors.append(str(error))

    try:
        for index in range(total):
            elapsed = index / rate
            if elapsed >= warmup_seconds and fault_at_seconds is None:
                with METRICS.lock:
                    METRICS.phase = label

            due = began_mono + elapsed
            time.sleep(max(0.0, due - time.monotonic()))
            lag_ms = max(0.0, (time.monotonic() - due) * 1000)
            with METRICS.lock:
                METRICS.max_schedule_lag_ms = max(METRICS.max_schedule_lag_ms, lag_ms)

            timestamp_ms = began_ms + int(elapsed * 1000)
            record = dict(rows[index % len(rows)])
            record["sensor_time"] = timestamp_ms * 1000
            record["log_time"] = timestamp_ms * 1000
            # A real nuScenes signal remains the dominant load. Twice per second a
            # synthetic fleet-status record exercises the alert path under load.
            if index % max(1, rate // 2) == 0:
                record = {
                    "scene_id": "synthetic-monitor-under-load",
                    "channel": "fleet_status",
                    "sensor_time": timestamp_ms * 1000,
                    "log_time": timestamp_ms * 1000,
                    "values_num": {
                        "speed_mps": 3.0,
                        "mission_active": 1.0,
                        "planned_stop": 0.0,
                        "sensor_healthy": float((int(elapsed) // 5) % 2),
                    },
                    "values_vec": {},
                    "values_str": {"mission_id": "validation"},
                }
            buf = BytesIO()
            schemaless_writer(buf, schema, record)
            try:
                producer.produce(
                    topic,
                    key=vehicle,
                    value=buf.getvalue(),
                    headers=[
                        ("vehicle_id", vehicle),
                        ("boot_id", run),
                        ("seq", str(index)),
                        ("kind", "RECORD_KIND_SIGNAL"),
                    ],
                    on_delivery=delivery,
                )
            except BufferError as error:
                errors.append(f"producer queue full at seq={index}: {error}")
                break
            sent += 1
            producer.poll(0)
            with METRICS.lock:
                METRICS.sent = sent
    finally:
        if fault_thread is not None:
            fault_thread.join(timeout=fault_seconds + 30)
        if fault_state["faulted"] and not fault_state["unpaused"]:
            compose("unpause", "clickhouse", check=False)
            fault_state["unpaused"] = True
            fault_state["ended_ms"] = now_ms()
            with METRICS.lock:
                METRICS.fault_active = 0

    if producer.flush(30):
        errors.append("producer did not drain within 30 seconds")

    with METRICS.lock:
        METRICS.phase = "recovery-drain" if fault_state["faulted"] else "drain"
    drain_deadline = time.monotonic() + (90 if fault_state["faulted"] else 30)
    stored = -1
    while time.monotonic() < drain_deadline:
        try:
            stored = clickhouse_count(run)
            with METRICS.lock:
                METRICS.stored = stored
            if stored == sent:
                break
        except Exception as error:
            errors.append(f"drain query: {error}")
        time.sleep(0.5)

    finished.set()
    alerts.stop.set()
    measured_from = began_ms + warmup_seconds * 1000
    latencies = [
        e["receivedAtMs"] - e["eventTimeMs"]
        for e in alerts.events
        if e["eventTimeMs"] >= measured_from
        and e["type"] in ("SENSOR_FAULT", "SENSOR_RECOVERED")
    ]
    p95 = percentile(latencies, 0.95)
    max_lag = METRICS.max_schedule_lag_ms
    expected_alerts = len(range(warmup_seconds, total_seconds, 5))
    event_ids = [e["eventId"] for e in alerts.events]
    valid = not errors and not alerts.errors and max_lag <= 100 and sent == total
    passed = (
        valid
        and stored == sent
        and p95 is not None
        and p95 <= 1000
        and len(latencies) == expected_alerts
        and len(event_ids) == len(set(event_ids))
    )
    with METRICS.lock:
        METRICS.alert_latency_p95_ms = float(p95 or 0)
        METRICS.passed = int(passed)
        METRICS.phase = "complete" if passed else "failed"

    result = {
        "run": run,
        "label": label,
        "scope": "local single-host acceptance; not a capacity benchmark",
        "source": "nuScenes native signals with synthetic fleet_status twice per second",
        "rate": rate,
        "warmupSeconds": warmup_seconds,
        "measurementSeconds": measure_seconds,
        "startedAtMs": began_ms,
        "finishedAtMs": now_ms(),
        "faultStartedMs": fault_state["started_ms"],
        "faultEndedMs": fault_state["ended_ms"],
        "sent": sent,
        "storedFinal": stored,
        "maxScheduleLagMs": max_lag,
        "alertLatencyP95Ms": p95,
        "measuredAlerts": len(latencies),
        "expectedAlerts": expected_alerts,
        "uniqueEventIds": len(event_ids) == len(set(event_ids)),
        "errors": errors + alerts.errors,
        "valid": valid,
        "passed": passed,
        "points": points,
        "alerts": alerts.events,
    }
    (out / f"{label}.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--rate", type=int, default=1300)
    parser.add_argument("--warmup-seconds", type=int, default=30)
    parser.add_argument("--steady-seconds", type=int, default=60)
    parser.add_argument("--fault-measure-seconds", type=int, default=45)
    parser.add_argument("--fault-warmup-seconds", type=int, default=15)
    parser.add_argument("--fault-at-seconds", type=int, default=15)
    parser.add_argument("--fault-seconds", type=int, default=10)
    parser.add_argument("--skip-fault", action="store_true")
    parser.add_argument("--metrics-port", type=int, default=19100)
    parser.add_argument(
        "--metrics-bind",
        default="127.0.0.1",
        help="use 0.0.0.0 when Prometheus runs in the validation Compose profile",
    )
    parser.add_argument("--api-url", default="http://127.0.0.1:8080")
    parser.add_argument("--topic", default="telemetry.records")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)

    server = ThreadingHTTPServer((args.metrics_bind, args.metrics_port), MetricsHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        jobs, baseline_restarts, baseline_failed_checkpoints = preflight(args.out, args.api_url)
        steady = run_load(
            out=args.out,
            label="steady",
            rate=args.rate,
            warmup_seconds=args.warmup_seconds,
            measure_seconds=args.steady_seconds,
            api_url=args.api_url,
            topic=args.topic,
        )
        fault = None
        if not args.skip_fault:
            fault = run_load(
                out=args.out,
                label="sink-fault",
                rate=args.rate,
                warmup_seconds=args.fault_warmup_seconds,
                measure_seconds=args.fault_measure_seconds,
                # Fault timing is relative to the measured interval; startup alerts are warm-up.
                fault_at_seconds=args.fault_warmup_seconds + args.fault_at_seconds,
                fault_seconds=args.fault_seconds,
                api_url=args.api_url,
                topic=args.topic,
            )
        final_jobs = {
            name: get_json(f"http://localhost:8081/jobs/{job_id}")
            for name, job_id in jobs.items()
        }
        restart_delta = {}
        failed_checkpoint_delta = {}
        for name, job_id in jobs.items():
            restart_metric = get_json(
                f"http://localhost:8081/jobs/{job_id}/metrics?get=numRestarts"
            )
            current_restarts = int(restart_metric[0]["value"]) if restart_metric else 0
            restart_delta[name] = current_restarts - baseline_restarts[name]
            final_checkpoints = get_json(
                f"http://localhost:8081/jobs/{job_id}/checkpoints"
            )
            failed_checkpoint_delta[name] = (
                final_checkpoints["counts"]["failed"] - baseline_failed_checkpoints[name]
            )
        observed_until_ms = now_ms()
        observability = {
            name: prometheus_range(
                f'up{{job="{name}"}}',
                steady["startedAtMs"],
                observed_until_ms,
                0.5 if name == "flink-validation-harness" else 1.0,
            )
            for name in ("flink-jobmanager", "flink-taskmanager", "flink-validation-harness")
        }
        observability_ok = all(
            item["samples"] > 0
            and item["min"] == 1
            and item["maxGapMs"] <= (1500 if name == "flink-validation-harness" else 2500)
            for name, item in observability.items()
        )
        runtime = inspect_containers(["fleet-jobmanager", "fleet-taskmanager", "fleet-clickhouse"])
        runtime_ok = all(state["running"] and not state["oomKilled"] for state in runtime.values())
        summary = {
            "jobs": jobs,
            "steady": {k: steady[k] for k in ("run", "valid", "passed", "sent", "storedFinal", "maxScheduleLagMs", "alertLatencyP95Ms")},
            "sinkFault": (
                {k: fault[k] for k in ("run", "valid", "passed", "sent", "storedFinal", "maxScheduleLagMs", "alertLatencyP95Ms", "faultStartedMs", "faultEndedMs")}
                if fault is not None
                else None
            ),
            "restartDelta": restart_delta,
            "failedCheckpointDelta": failed_checkpoint_delta,
            "finalJobState": {name: job["state"] for name, job in final_jobs.items()},
            "observability": observability,
            "observabilityPassed": observability_ok,
            "containerRuntime": runtime,
            "containerRuntimePassed": runtime_ok,
            "passed": (
                steady["passed"]
                and (fault is None or fault["passed"])
                and all(job["state"] == "RUNNING" for job in final_jobs.values())
                and all(delta == 0 for delta in restart_delta.values())
                and all(delta == 0 for delta in failed_checkpoint_delta.values())
                and observability_ok
                and runtime_ok
            ),
        }
        (args.out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
        print(json.dumps(summary, ensure_ascii=False))
        if not summary["passed"]:
            raise SystemExit(1)
    finally:
        compose("unpause", "clickhouse", check=False)
        server.shutdown()


if __name__ == "__main__":
    main()
