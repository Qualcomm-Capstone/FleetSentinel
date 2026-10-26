"""Explicit integration gate: RUN_MQTT_INTEGRATION=1 pytest -q tests/test_mqtt_integration.py.

Starts an isolated mTLS Mosquitto container with repository ACLs and ephemeral PKI.
Kafka completion is controlled here to inject ACK loss/failure deterministically.
No existing containers, volumes, credentials or .env files are used.
"""
import json
import os
import queue
from pathlib import Path
import re
import shutil
import socket
import subprocess
import time
import uuid

import pytest

from fleetsentinel_ingest.collection_policy import Collector
from fleetsentinel_ingest.mqtt_bridge import Bridge
from fleetsentinel_ingest.mqtt_contract import canonical
from fleetsentinel_ingest.mqtt_runtime import Connection, EdgePump, EdgeRuntime, KafkaWriter, edge_receive
from fleetsentinel_ingest.telemetry_store import TelemetryStore

pytestmark = pytest.mark.skipif(os.environ.get("RUN_MQTT_INTEGRATION") != "1",
                                reason="explicit Docker integration gate")
ROOT = Path(__file__).resolve().parents[2]


def run(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT).strip()


def until(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("MQTT condition did not become true")


@pytest.fixture
def broker(tmp_path):
    config = tmp_path / "config"
    config.mkdir()
    for name in ("mosquitto.conf", "acl"):
        shutil.copyfile(ROOT / "infra/mqtt" / name, config / name)
    run("bash", str(ROOT / "scripts/gen-mqtt-certs.sh"), str(config / "pki"), "vehicle-1", "vehicle-2")
    data = tmp_path / "data"
    data.mkdir()
    container = "fleet-mqtt-test-" + uuid.uuid4().hex[:12]
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    run("docker", "run", "-d", "--rm", "--name", container,
        "--user", f"{os.getuid()}:{os.getgid()}",
        "-p", f"127.0.0.1:{port}:8883", "-v", f"{config}:/mosquitto/config:ro",
        "-v", f"{data}:/mosquitto/data", "eclipse-mosquitto:2.0.22")
    try:
        port = int(run("docker", "port", container, "8883/tcp").rsplit(":", 1)[1])
        yield port, config / "pki", container
    finally:
        run("docker", "stop", "--time", "2", container)


def connect(broker, name, topics):
    port, pki, _ = broker
    connection = Connection(name, "localhost", port, pki / "ca.crt", pki / f"{name}.crt",
                            pki / f"{name}.key", topics)
    connection.start()
    until(connection.client.is_connected)
    # Wait for broker subscription acknowledgement rather than assuming connect == ready.
    subscribed = []
    connection.client.on_subscribe = lambda *args: subscribed.append(True)
    for topic in topics:
        connection.client.subscribe(topic, 1)
    until(lambda: len(subscribed) >= len(topics))
    return connection


def test_mtls_acl_kafka_ack_loss_restart_and_downlink(broker, tmp_path):
    bridge_connection = connect(broker, "bridge", ["$share/ingestion/fleet/v1/+/up/+"])
    edge = connect(broker, "vehicle-1", ["fleet/v1/vehicle-1/ack/+", "fleet/v1/vehicle-1/command"])
    operator = connect(broker, "command-service", ["fleet/v1/+/reported"])
    store = TelemetryStore(tmp_path / "edge.sqlite", "vehicle-1")
    try:
        writes = []
        class Kafka:
            def send(self, topic, key, payload, headers, completed):
                writes.append((payload, headers, completed))
        bridge = Bridge(Kafka(), bridge_connection.publish)
        pump = EdgePump(store, edge.publish, retry_ms=100)
        store.enqueue("event", "signal", b"test-avro", 1000)
        pump.tick(1000)
        until(lambda: (bridge_connection.drain(bridge.receive), len(writes) == 1)[1])
        assert store.pending() == 1  # MQTT delivery alone does not release the record.
        # Simulate Kafka failure, retry, then a lost application ACK and edge restart.
        writes[0][-1](RuntimeError("Kafka failed"))
        pump.tick(1200)
        until(lambda: (bridge_connection.drain(bridge.receive), len(writes) == 2)[1])
        writes[-1][-1](None)
        until(lambda: not edge.messages.empty())
        edge.messages.get_nowait()  # fault injection: discard ACK before edge SQLite commit
        before = store.batch("event").wire()
        store.close()
        store = TelemetryStore(tmp_path / "edge.sqlite", "vehicle-1")
        assert store.batch("event").wire() == before
        pump = EdgePump(store, edge.publish)
        pump.tick(2000)
        until(lambda: (bridge_connection.drain(bridge.receive), len(writes) == 3)[1])
        assert writes[1][1] == writes[2][1]
        writes[-1][-1](None)
        until(lambda: (edge.drain(pump.receive), store.pending() == 0)[1])

        # Certificate identity must not publish another vehicle's topic or storage ACK.
        denied = []
        edge.client.on_publish = lambda c, u, mid, reason, props: denied.append(reason)
        edge.publish("fleet/v1/vehicle-2/up/event", b"forged")
        edge.publish("fleet/v1/vehicle-1/ack/event", b"forged")
        until(lambda: len(denied) >= 2)
        assert all(reason.is_failure for reason in denied[-2:])

        now = int(time.time() * 1000)
        command = dict(command_id="focus-1", vehicle_id="vehicle-1", type="collection_policy", version=1,
                       expires_at_ms=now + 5000,
                       policy=dict(normal_ms=10000, min_ms=1000, focus_ms=1000, focus_until_ms=now + 4000))
        collector = Collector(store)
        operator.publish("fleet/v1/vehicle-1/command", canonical(command))
        until(lambda: (edge.drain(lambda t, p: edge_receive(collector, pump, edge, t, p)),
                       collector.policy.version == 1)[1])
        reports = []
        until(lambda: (operator.drain(lambda t, p: reports.append(json.loads(p))), bool(reports))[1])
        assert reports[0]["status"] == "applied"
        # Transport disconnect/reconnect must resubscribe and preserve persistent edge data.
        edge.close()
        edge = connect(broker, "vehicle-1", ["fleet/v1/vehicle-1/ack/+", "fleet/v1/vehicle-1/command"])
        operator.publish("fleet/v1/vehicle-1/command", canonical(command))
        until(lambda: (edge.drain(lambda t, p: edge_receive(collector, pump, edge, t, p)),
                       not operator.messages.empty())[1])
        # Broker restart: edge outbox, not broker persistence, is the upload source of truth.
        run("docker", "restart", "--time", "2", broker[2])
        assert int(run("docker", "port", broker[2], "8883/tcp").rsplit(":", 1)[1]) == broker[0], "test broker endpoint changed on restart"
        store.enqueue("event", "signal", b"after-broker-restart", int(time.time() * 1000))
        pump = EdgePump(store, edge.publish, retry_ms=100)
        next_completion = len(writes)
        def recovered():
            nonlocal next_completion
            try:
                pump.tick(int(time.monotonic() * 1000))
                bridge_connection.drain(bridge.receive)
                while next_completion < len(writes):
                    writes[next_completion][-1](None)
                    next_completion += 1
                edge.drain(pump.receive)
            except ConnectionError:
                pass
            return store.pending() == 0
        until(recovered, timeout=20)
    finally:
        store.close()
        edge.close()
        bridge_connection.close()
        operator.close()


def test_focus_anomaly_capacity_and_invalid_command_over_real_mqtt(broker, tmp_path):
    bridge_connection = connect(broker, "bridge", ["$share/ingestion/fleet/v1/+/up/+"])
    edge = connect(broker, "vehicle-1", ["fleet/v1/vehicle-1/ack/+", "fleet/v1/vehicle-1/command"])
    operator = connect(broker, "command-service", ["fleet/v1/+/reported"])
    store = TelemetryStore(tmp_path / "pressured.sqlite", "vehicle-1", max_pending_bytes=80)
    try:
        writes, reports = [], []
        class Kafka:
            def send(self, topic, key, payload, headers, completed):
                writes.append(completed)
        bridge = Bridge(Kafka(), bridge_connection.publish)
        collector = Collector(store)
        samples = queue.Queue(maxsize=128)
        runtime = EdgeRuntime(collector, edge, samples)
        now = int(time.time() * 1000)
        focus_until = now + 60000
        operator.publish("fleet/v1/vehicle-1/command", canonical(dict(
            command_id="focus", vehicle_id="vehicle-1", type="collection_policy", version=1,
            expires_at_ms=now + 90000, policy={"focus_until_ms": focus_until})))

        def tick():
            runtime.tick(int(time.time() * 1000), int(time.monotonic() * 1000))
            bridge_connection.drain(bridge.receive)
            operator.drain(lambda topic, payload: reports.append(json.loads(payload)))

        until(lambda: (tick(), collector.policy.version == 1)[1])
        signal = dict(scene_id="test", channel="ms_imu", sensor_time=now * 1000, log_time=now * 1000,
                      values_num={"speed_mps": 1.0}, values_vec={}, values_str={})
        samples.put(json.dumps({"signal": signal, "anomaly": True}))
        samples.put(json.dumps(signal | {"channel": "ego_pose"}))
        until(lambda: (tick(), runtime.blocked and bool(writes))[1])
        assert collector.policy.focus_until_ms == focus_until
        assert store.pending("history") + store.deferred_history() == 1

        # No application ACK yet: commands must still work while admission is paused.
        for command in (
            dict(command_id="bad", type="collection_policy", version=2, policy={"thresholds": []}),
            dict(command_id="diagnostic", type="diagnostic"),
        ):
            operator.publish("fleet/v1/vehicle-1/command", canonical(command | {
                "vehicle_id": "vehicle-1", "expires_at_ms": now + 90000}))
        until(lambda: (tick(), {r["command_id"] for r in reports} >= {"bad", "diagnostic"})[1])
        results = {r["command_id"]: r["status"] for r in reports}
        assert results["bad"] == "rejected"
        assert results["diagnostic"] == "applied"
        assert collector.policy.version == 1
        completed = 0
        def recovered():
            nonlocal completed
            tick()
            while completed < len(writes):
                writes[completed](None)
                completed += 1
            return samples.empty() and runtime.pending is None and store.pending() == store.deferred_history() == 0
        until(recovered)
        assert not runtime.blocked
        assert store.ring_count() == 2
    finally:
        store.close()
        edge.close()
        bridge_connection.close()
        operator.close()


@pytest.fixture
def kafka():
    # A fresh single-node Kafka is enough for the wire contract, not a durability/HA claim.
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    container = "fleet-kafka-test-" + uuid.uuid4().hex[:12]
    run("docker", "run", "-d", "--rm", "--name", container,
        "-p", f"127.0.0.1:{port}:9092",
        "-e", "KAFKA_NODE_ID=1", "-e", "KAFKA_PROCESS_ROLES=broker,controller",
        "-e", "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
        "-e", f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:{port}",
        "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
        "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        "-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
        "-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
        "-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0", "apache/kafka:4.0.0")
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        admin = AdminClient({"bootstrap.servers": f"localhost:{port}"})
        def ready():
            try:
                return bool(admin.list_topics(timeout=1).brokers)
            except Exception:
                return False
        until(ready, timeout=60)
        admin.create_topics([NewTopic("telemetry.records", num_partitions=1, replication_factor=1)])["telemetry.records"].result(15)
        yield f"localhost:{port}"
    finally:
        run("docker", "stop", "--time", "2", container)


def test_real_kafka_delivery_receipt_and_avro_contract(broker, kafka, tmp_path):
    from confluent_kafka import Consumer
    import fastavro
    import io
    edge = connect(broker, "vehicle-1", ["fleet/v1/vehicle-1/ack/+"])
    bridge_connection = connect(broker, "bridge", ["$share/ingestion/fleet/v1/+/up/+"])
    writer = KafkaWriter(kafka)
    consumer = Consumer({"bootstrap.servers": kafka, "group.id": uuid.uuid4().hex,
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe(["telemetry.records"])
    try:
        with TelemetryStore(tmp_path / "edge.sqlite", "vehicle-1") as store:
            collector = Collector(store)
            now = int(time.time() * 1000)
            collector.observe(dict(scene_id="test", channel="fleet_status", sensor_time=now * 1000,
                                   log_time=now * 1000, values_num={"speed_mps": 3.0}, values_vec={}, values_str={}))
            pump = EdgePump(store, edge.publish)
            bridge = Bridge(writer, bridge_connection.publish)
            pump.tick(now)
            def delivered():
                bridge_connection.drain(bridge.receive)
                writer.poll()
                edge.drain(pump.receive)
                return store.pending() == 0
            until(delivered)
            received = []
            def consume():
                record = consumer.poll(0.1)
                if record is not None and not record.error():
                    received.append(record)
                return bool(received)
            until(consume)
            record = received[0]
            headers = dict(record.headers())
            assert record.key() == b"vehicle-1"
            assert headers["kind"] == b"RECORD_KIND_SIGNAL"
            assert headers["stream_id"] == b"status"
            assert headers["report_interval_ms"] == b"10000"
            decoded = fastavro.schemaless_reader(io.BytesIO(record.value()), collector.schema)
            assert decoded["values_num"]["speed_mps"] == 3.0
    finally:
        consumer.close()
        writer.close()
        edge.close()
        bridge_connection.close()


def test_clickhouse_roster_ignores_late_history_and_preserves_zero_coordinate():
    # Execute the API's actual SQL, not a second hand-maintained copy of its aggregation.
    source = (ROOT / "api/src/main/java/io/fleetsentinel/api/query/TelemetrySql.java").read_text()
    query = re.search(r'String VEHICLES = """(.*?)"""', source, re.S)[1]
    setup = """
    CREATE DATABASE fleet;
    CREATE TABLE fleet.signals (vehicle_id String, scene_id String, boot_id String, seq UInt64,
       sensor_time DateTime64(6, 'UTC'), lat Nullable(Float64), lon Nullable(Float64), values_str Map(String,String)) ENGINE=Memory;
    INSERT INTO fleet.signals VALUES
      ('v1','new','live',1,'2026-10-26 12:00:00',0,127,{'location':'current'}),
      ('v1','new','history',1,'2026-10-26 11:00:00',20,30,{'location':'past'}),
      ('v1','old','history',2,'2026-10-25 12:00:00',40,50,{'location':'older'}),
      ('v2','none','live',1,'2026-10-26 12:00:00',NULL,NULL,{});
    """
    result = subprocess.run(["docker", "run", "--rm", "-i", "--network", "none", "--entrypoint", "clickhouse",
                             "clickhouse:26.3", "local", "--multiquery"],
                            input=setup + query + " FORMAT JSONEachRow;", text=True, capture_output=True, timeout=30)
    assert result.returncode == 0, result.stderr
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    assert rows[0]["last_lat"] == 0 and rows[0]["last_lon"] == 127
    assert rows[0]["location"] == "current" and rows[0]["scene_id"] == "new"
    assert rows[1]["last_lat"] is None and rows[1]["last_lon"] is None
