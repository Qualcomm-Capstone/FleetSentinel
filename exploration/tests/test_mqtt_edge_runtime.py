"""Business failure paths: admission pressure must not stop recovery or commands."""
import json
import queue

import pytest

from fleetsentinel_ingest.collection_policy import Collector
from fleetsentinel_ingest.mqtt_contract import Batch, ack_for, canonical
from fleetsentinel_ingest.mqtt_runtime import EdgeRuntime
from fleetsentinel_ingest.telemetry_store import TelemetryStore


def signal(t=1000, channel="ego_pose"):
    return dict(scene_id="test", channel=channel, sensor_time=t * 1000, log_time=t * 1000,
                values_num={"speed_mps": 1.0}, values_vec={}, values_str={})


class FakeConnection:
    def __init__(self):
        self.online = True
        self.sent = []
        self.incoming = queue.Queue()

    def publish(self, topic, payload):
        if not self.online:
            raise ConnectionError("offline")
        self.sent.append((topic, payload))

    def drain(self, callback):
        while not self.incoming.empty():
            callback(*self.incoming.get_nowait())

    def ack_uploads(self):
        for topic, payload in self.sent:
            if "/up/" in topic:
                batch = Batch.parse(payload, topic)
                self.incoming.put((f"fleet/v1/v1/ack/{batch.stream}", canonical(ack_for(batch))))
        self.sent.clear()


@pytest.mark.parametrize("count", [1, 32, 128])
def test_given_available_samples_when_tick_runs_then_bounded_group_commits(tmp_path, count):
    with TelemetryStore(tmp_path / "edge.sqlite", "v1") as store:
        conn, samples = FakeConnection(), queue.Queue()
        runtime = EdgeRuntime(Collector(store), conn, samples)
        groups = []
        observe_batch = runtime.collector.observe_batch
        def record_batch(batch):
            groups.append(len(batch))
            observe_batch(batch)
        runtime.collector.observe_batch = record_batch
        for t in range(count):
            samples.put(json.dumps(signal(t)))

        runtime.tick(1000, 1000)

        assert groups == ([count] if count < 32 else [32] * (count // 32))
        assert store.ring_count() == count
        assert not runtime.pending
        assert samples.empty()


def test_given_anomaly_between_samples_when_admitted_then_commit_and_send_before_next_batch(tmp_path):
    import sqlite3
    path = tmp_path / "edge.sqlite"
    with TelemetryStore(path, "v1") as store:
        conn, samples = FakeConnection(), queue.Queue()
        collector = Collector(store)
        runtime = EdgeRuntime(collector, conn, samples)
        for item in (signal(1000), {"signal": signal(2000), "anomaly": True}, signal(3000)):
            samples.put(json.dumps(item))
        observe_batch = collector.observe_batch
        def check_batch(batch):
            if batch[0][0]["sensor_time"] == 3000000:
                with sqlite3.connect(path) as reader:
                    assert reader.execute("SELECT COUNT(*) FROM outbox WHERE stream='event'").fetchone()[0] == 1
                assert any(topic.endswith("/up/event") for topic, _ in conn.sent)
            observe_batch(batch)
        collector.observe_batch = check_batch

        runtime.tick(1000, 1000)

        assert store.ring_count() == 3
        assert not runtime.pending


def test_given_sqlite_full_when_tick_retries_then_keep_inputs_and_existing_ack_path(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "v1", max_db_bytes=256 * 1024) as store:
        conn, samples = FakeConnection(), queue.Queue()
        runtime = EdgeRuntime(Collector(store), conn, samples)
        store.enqueue("event", "signal", b"keep", 0)
        store.batch("event")  # Already persisted batch can still drain during pressure.
        store.db.executescript("""
            CREATE TABLE padding(data BLOB);
            CREATE TRIGGER fill BEFORE INSERT ON ring
            BEGIN INSERT INTO padding VALUES(zeroblob(524288)); END;
        """)
        for t in (1000, 2000):
            samples.put(json.dumps(signal(t)))

        runtime.tick(1000, 1000)

        assert runtime.blocked
        assert len(runtime.pending) == 2
        assert store.ring_count() == 0
        assert not runtime.collector.last
        conn.ack_uploads()
        runtime.tick(2000, 2000)
        assert store.pending("event") == 0
        assert runtime.blocked
        store.db.execute("DROP TRIGGER fill")
        runtime.tick(3000, 3000)
        assert not runtime.blocked
        assert not runtime.pending
        assert store.ring_count() == 2


def test_pressure_keeps_pending_input_commands_and_uploads_alive(tmp_path, caplog):
    with TelemetryStore(tmp_path / "edge.sqlite", "v1", max_pending_bytes=80) as store:
        conn, samples = FakeConnection(), queue.Queue(maxsize=128)
        runtime = EdgeRuntime(Collector(store, clock=lambda: 1000), conn, samples)
        samples.put(json.dumps({"signal": signal(channel="ms_imu"), "anomaly": True}))
        samples.put(json.dumps(signal()))
        samples.put(json.dumps(signal(2000, channel="fleet_status")))
        runtime.tick(1000, 1000)
        assert runtime.blocked
        assert samples.qsize() + len(runtime.pending) == 2
        assert store.pending("event") == 1
        assert len(conn.sent) == 2  # event and history published despite blocked admission
        assert "admission paused" in caplog.text
        conn.ack_uploads()
        conn.incoming.put(("fleet/v1/v1/command", canonical(dict(
            command_id="inspect", vehicle_id="v1", type="diagnostic", expires_at_ms=5000))))
        runtime.tick(2000, 2000)
        assert runtime.blocked  # Only one status record fits; the next is retained for retry.
        assert samples.empty()
        assert any("/reported" in topic for topic, _ in conn.sent)
        assert store.pending("event") == store.pending("history") == 0
        assert store.pending("status") == 1
        conn.ack_uploads()
        runtime.tick(3000, 3000)
        assert not runtime.blocked
        assert store.pending("status") == 1
        assert store.ring_count() == 3  # Failed attempts did not duplicate observations.


def test_full_outbox_restart_and_network_recovery_drain_before_readmitting(tmp_path):
    path = tmp_path / "edge.sqlite"
    with TelemetryStore(path, "v1", max_pending_bytes=80) as store:
        store.enqueue("event", "signal", b"x" * 80, 1000)
        original = store.batch("event").wire()
    with TelemetryStore(path, "v1", max_pending_bytes=80) as store:
        conn, samples = FakeConnection(), queue.Queue(maxsize=128)
        conn.online = False
        samples.put(json.dumps(signal()))
        samples.put(json.dumps(signal(2000)))
        runtime = EdgeRuntime(Collector(store), conn, samples)
        for now in (1000, 2000, 3000):
            runtime.tick(now, now)
        assert runtime.blocked
        assert samples.qsize() + len(runtime.pending) == 2
        assert store.batch("event").wire() == original
        conn.online = True
        runtime.tick(4000, 4000)
        assert conn.sent[0][1] == original
        conn.ack_uploads()
        runtime.tick(5000, 5000)
        assert not runtime.blocked
        assert store.pending("event") == 0
        assert store.pending("status") == 1
        assert samples.empty()


def test_full_outbox_history_command_is_reported_and_diagnostic_still_works(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "v1", max_pending_bytes=80) as store:
        store.record_local(b"history", 1000)
        store.enqueue("event", "signal", b"x" * 80, 1000)
        conn = FakeConnection()
        runtime = EdgeRuntime(Collector(store, clock=lambda: 1000), conn, queue.Queue())
        for command in (
            dict(command_id="history", type="history_request", start_ms=0, end_ms=1000),
            dict(command_id="diagnostic", type="diagnostic"),
        ):
            conn.incoming.put(("fleet/v1/v1/command", canonical(command | {
                "vehicle_id": "v1", "expires_at_ms": 5000})))
        runtime.tick(1000, 1000)
        replies = [json.loads(p) for t, p in conn.sent if t.endswith("/reported")]
        assert [r["status"] for r in replies] == ["applied", "applied"]
        assert replies[0]["records"] == 1
        assert store.deferred_history() == 1
        conn.ack_uploads()
        runtime.tick(2000, 2000)
        assert store.deferred_history() == 0
        assert store.pending("history") == 1


def test_cli_drains_anomaly_and_next_input_without_exiting_on_capacity(tmp_path, monkeypatch):
    import importlib.util
    import io
    from pathlib import Path
    import sys

    path = Path(__file__).resolve().parents[1] / "scripts" / "mqtt_ingest.py"
    spec = importlib.util.spec_from_file_location("mqtt_ingest_cli_test", path)
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)

    class Connection(FakeConnection):
        def __init__(self):
            super().__init__()
            self.rounds = 0
            self.uploads = []
            self.closed = False

        def start(self):
            pass

        def close(self):
            self.closed = True

        def publish(self, topic, payload):
            super().publish(topic, payload)
            self.uploads.append(topic)

        def drain(self, callback):
            self.rounds += 1
            self.ack_uploads()
            super().drain(callback)
            if self.rounds == 3:
                raise KeyboardInterrupt

    class InlineThread:
        def __init__(self, target, **kwargs):
            self.target = target

        def start(self):
            self.target()

    conn = Connection()
    database = tmp_path / "cli.sqlite"
    monkeypatch.setattr(cli, "Connection", lambda *args: conn)
    monkeypatch.setattr(cli.threading, "Thread", InlineThread)
    monkeypatch.setattr(cli.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(cli.time, "time", lambda: 1.0)
    monkeypatch.setattr(sys, "argv", ["mqtt_ingest.py", "edge", "--client-id", "v1", "--vehicle", "v1",
                                     "--db", str(database), "--max-pending-bytes", "80",
                                     "--ca", "unused", "--cert", "unused", "--key", "unused"])
    monkeypatch.setattr(sys, "stdin", io.StringIO("\n".join([
        json.dumps({"signal": signal(channel="ms_imu"), "anomaly": True}), json.dumps(signal())])))
    cli.main()
    assert conn.closed
    assert conn.uploads == ["fleet/v1/v1/up/event", "fleet/v1/v1/up/history", "fleet/v1/v1/up/status"]
    with TelemetryStore(database, "v1") as store:
        assert store.pending() == 0
        assert store.ring_count() == 2
