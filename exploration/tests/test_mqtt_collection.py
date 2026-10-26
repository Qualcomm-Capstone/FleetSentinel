"""Requirements first: durable upload, selective collection, command recovery."""
import json
import subprocess
import sys

import pytest

from fleetsentinel_ingest.mqtt_contract import Batch, Record, ack_for
from fleetsentinel_ingest.telemetry_store import TelemetryStore, CapacityError
from fleetsentinel_ingest.collection_policy import Collector, Policy
from fleetsentinel_ingest.mqtt_bridge import Bridge


def sample(t=1000, channel="ego_pose", value=1.0):
    return dict(scene_id="test", channel=channel, sensor_time=t * 1000,
                log_time=t * 1000, values_num={"speed_mps": value},
                values_vec={}, values_str={})


def test_batch_rejects_tampering_and_topic_impersonation():
    batch = Batch.create("vehicle-1", "boot", "status", [Record(0, "signal", b"abc", 1000)])
    assert Batch.parse(batch.wire(), batch.topic()) == batch
    with pytest.raises(ValueError):
        Batch.parse(batch.wire(), "fleet/v1/vehicle-2/up/status")
    value = json.loads(batch.wire())
    value["records"][0]["seq"] = 3
    with pytest.raises(ValueError):
        Batch.parse(json.dumps(value).encode(), batch.topic())


def test_restart_keeps_batch_identity_and_only_matching_application_ack_deletes(tmp_path):
    path = tmp_path / "edge.sqlite"
    store = TelemetryStore(path, "vehicle-1")
    store.enqueue("status", "signal", b"one", 1000)
    batch = store.batch("status")
    store.close()
    store = TelemetryStore(path, "vehicle-1")
    assert store.batch("status").wire() == batch.wire()
    invalid = ack_for(batch) | {"last_seq": 999}
    assert not store.acknowledge(invalid)
    assert not store.acknowledge(ack_for(batch) | {"first_seq": False})
    assert store.pending() == 1
    assert store.acknowledge(ack_for(batch))
    assert store.pending() == 0
    assert not store.acknowledge(ack_for(batch))
    assert store.enqueue("status", "signal", b"two", 2000) == 1
    store.close()


def test_history_cannot_block_event_ack_and_chunks_are_bounded(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "vehicle-1", max_batch_bytes=1024) as store:
        for _ in range(20):
            store.enqueue("history", "signal", b"x" * 100, 1000)
        store.enqueue("event", "signal", b"urgent", 1000)
        history = store.batch("history")
        event = store.batch("event")
        assert len(history.wire()) <= 1024
        assert len(history.records) < 20
        assert history.records[0].seq == event.records[0].seq == 0
        assert store.acknowledge(ack_for(event))
        assert store.pending("event") == 0
        assert store.pending("history") == 20


def test_outbox_pressure_does_not_silently_drop_unacked_records(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "vehicle-1", max_pending_bytes=6) as store:
        store.enqueue("event", "signal", b"123456", 1000)
        with pytest.raises(CapacityError):
            store.enqueue("event", "signal", b"7", 1001)
        assert store.pending() == 1
        assert store.acknowledge(ack_for(store.batch("event")))
        store.enqueue("event", "signal", b"abcdef", 1002)
        assert store.pending() == 1


def test_policy_local_recording_change_threshold_and_max_interval(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "vehicle-1") as store:
        collector = Collector(store, Policy(normal_ms=1000, min_ms=200,
                              thresholds={"speed_mps": 1}, channels=("ego_pose",)), clock=lambda: 1000)
        assert collector.observe(sample(1000), now_ms=1000) == "status"
        assert collector.observe(sample(1100, value=1.1), now_ms=1100) is None
        assert collector.observe(sample(1300, value=3), now_ms=1300) == "status"
        assert collector.observe(sample(2300, value=3), now_ms=2300) == "status"
        assert collector.observe(sample(2400, channel="ms_imu"), now_ms=2400) is None
        assert store.ring_count() == 5
        assert store.pending("status") == 3


def test_policy_command_is_durable_idempotent_versioned_and_expires(tmp_path):
    path = tmp_path / "edge.sqlite"
    command = dict(command_id="command-1", vehicle_id="vehicle-1", type="collection_policy",
                   version=2, expires_at_ms=5000,
                   policy=dict(normal_ms=1000, min_ms=100, focus_ms=100,
                               focus_until_ms=4000, channels=["ego_pose"]))
    with TelemetryStore(path, "vehicle-1") as store:
        collector = Collector(store, clock=lambda: 1000)
        assert collector.command(command)["status"] == "applied"
        assert collector.command(command)["status"] == "applied"
        assert collector.observe(sample(1000), now_ms=1000) == "status"
        assert collector.observe(sample(1100), now_ms=1100) == "status"
    with TelemetryStore(path, "vehicle-1") as store:
        collector = Collector(store, clock=lambda: 4100)
        assert collector.policy.version == 2
        assert collector.policy.interval(4100) == 1000
        assert collector.command(command | {"command_id": "old", "version": 1})["status"] == "rejected"
        assert collector.command(command | {"command_id": "expired", "expires_at_ms": 4000})["status"] == "expired"
        assert collector.command(command | {"version": 99})["status"] == "rejected"


def test_retention_deletes_only_ring_not_upload_outbox(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "vehicle-1", retention_ms=1000) as store:
        collector = Collector(store, clock=lambda: 1000)
        collector.observe(sample(1000), now_ms=1000)
        store.prune_ring(2001)
        assert store.ring_count() == 0
        assert store.pending() == 1


def test_anomaly_promotes_history_and_does_not_wait_for_normal_reporting(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "vehicle-1") as store:
        collector = Collector(store, clock=lambda: 1000)
        collector.observe(sample(900, channel="ms_imu"), now_ms=900)
        collector.observe(sample(1000, channel="ms_imu"), now_ms=1000, anomaly=True)
        assert store.pending("event") == 1
        assert store.pending("history") == 2
        assert collector.policy.interval(1100) == collector.policy.focus_ms


def test_anomaly_survives_history_pressure_and_history_resumes_after_restart(tmp_path):
    path = tmp_path / "edge.sqlite"
    with TelemetryStore(path, "v1", max_pending_bytes=80, retention_ms=1000,
                        max_ring_bytes=200) as store:
        collector = Collector(store)
        for timestamp in (800, 900, 1000):
            collector.observe(sample(timestamp, channel="ms_imu"), now_ms=timestamp)
        expected = [row[0] for row in store.db.execute("SELECT payload FROM ring ORDER BY id")]
        assert collector.observe(sample(1100, channel="ms_imu"), now_ms=1100, anomaly=True) == "event"
        event = store.batch("event")
        expected.append(event.records[0].payload)
        assert store.pending("event") == 1
        assert store.deferred_history() > 0
        focus_until = collector.policy.focus_until_ms

    with TelemetryStore(path, "v1", max_pending_bytes=80, retention_ms=1000,
                        max_ring_bytes=200) as store:
        assert store.batch("event").wire() == event.wire()
        assert Collector(store).policy.focus_until_ms == focus_until
        # Pinned history survives both TTL and pressure from new local observations.
        store.prune_ring(10000)
        for _ in range(10):
            store.record_local(b"new" * 20, 10000)
        assert store.db.execute("SELECT bytes FROM usage WHERE name='ring'").fetchone()[0] <= 200
        assert store.acknowledge(ack_for(event))
        received = []
        for _ in range(10):
            store.drain_history(limit=1)
            batch = store.batch("history")
            if batch:
                received.extend(r.payload for r in batch.records)
                assert store.acknowledge(ack_for(batch))
        assert received == expected
        assert store.deferred_history() == store.pending() == 0
        assert store.drain_history() == 0
        store.prune_ring(20000)
        assert store.ring_count() == 0


def test_history_promotion_and_cursor_rollback_together(tmp_path):
    path = tmp_path / "edge.sqlite"
    with TelemetryStore(path, "v1") as store:
        store.record_local(b"history", 1000)
        store.defer_history(0, 1000)
        with pytest.raises(RuntimeError, match="crash"):
            with store.atomic():
                assert store.drain_history() == 1
                raise RuntimeError("crash")
    with TelemetryStore(path, "v1") as store:
        assert store.pending() == 0
        assert store.deferred_history() == 1
        assert store.drain_history() == 1
        assert store.batch("history").records[0].seq == 0
        assert store.deferred_history() == 0


def test_process_exit_after_event_commit_keeps_deferred_history(tmp_path):
    path = tmp_path / "edge.sqlite"
    program = """
import os, sys
from fleetsentinel_ingest.telemetry_store import TelemetryStore
from fleetsentinel_ingest.collection_policy import Collector
store = TelemetryStore(sys.argv[1], 'v1')
collector = Collector(store)
row = dict(scene_id='test', channel='ms_imu', sensor_time=1000000,
           log_time=1000000, values_num={}, values_vec={}, values_str={})
collector.observe(row, now_ms=1000)
store.drain_history = lambda: os._exit(73)
collector.observe(row, now_ms=1100, anomaly=True)
"""
    result = subprocess.run([sys.executable, "-c", program, str(path)], check=False)
    assert result.returncode == 73
    with TelemetryStore(path, "v1", retention_ms=1000) as store:
        store.prune_ring(10000)
        assert store.pending("event") == 1
        assert store.deferred_history() == 2
        assert Collector(store).policy.focus_until_ms > 1100
        assert store.drain_history() == 2
        assert store.pending("history") == 2
        assert store.deferred_history() == 0


class FakeKafka:
    def __init__(self):
        self.writes = []

    def send(self, topic, key, payload, headers, completed):
        self.writes.append((topic, key, payload, headers, completed))


def test_bridge_ack_requires_every_kafka_write_and_scopes_stream_identity():
    kafka, replies = FakeKafka(), []
    bridge = Bridge(kafka, lambda topic, payload: replies.append((topic, json.loads(payload))))
    batch = Batch.create("vehicle-1", "boot", "history",
                         [Record(0, "signal", b"a", 1000), Record(1, "signal", b"b", 1000)])
    bridge.receive(batch.topic(), batch.wire())
    kafka.writes[1][-1](None)
    assert replies == []
    kafka.writes[0][-1](None)
    assert replies[0][1] == ack_for(batch)
    assert dict(kafka.writes[0][3])["stream_id"] == b"history"
    assert dict(kafka.writes[0][3])["kind"] == b"RECORD_KIND_SIGNAL"
    history_boot = dict(kafka.writes[0][3])["boot_id"]
    status = Batch.create("vehicle-1", "boot", "status", [Record(0, "signal", b"a", 1000)])
    bridge.receive(status.topic(), status.wire())
    assert history_boot != dict(kafka.writes[-1][3])["boot_id"]


def test_partial_failure_never_acknowledges_and_retry_is_safe():
    kafka, replies = FakeKafka(), []
    bridge = Bridge(kafka, lambda *args: replies.append(args))
    batch = Batch.create("vehicle-1", "boot", "event", [Record(0, "signal", b"a", 1000)])
    bridge.receive(batch.topic(), batch.wire())
    kafka.writes[-1][-1](RuntimeError("Kafka unavailable"))
    assert replies == []
    bridge.receive(batch.topic(), batch.wire())
    kafka.writes[-1][-1](None)
    assert len(replies) == 1


def test_bridge_bounds_inflight_and_ignores_repeated_callback():
    kafka, replies = FakeKafka(), []
    bridge = Bridge(kafka, lambda *args: replies.append(args), max_inflight=1)
    batch = Batch.create("v1", "boot", "status", [Record(0, "signal", b"a")])
    assert bridge.receive(batch.topic(), batch.wire())
    assert not bridge.receive(batch.topic(), batch.wire())
    next_batch = Batch.create("v1", "boot", "status", [Record(1, "signal", b"b")])
    assert not bridge.receive(next_batch.topic(), next_batch.wire())
    assert len(kafka.writes) == 1
    kafka.writes[-1][-1](None)
    kafka.writes[-1][-1](None)
    assert len(replies) == 1
    assert bridge.receive(next_batch.topic(), next_batch.wire())
    assert len(kafka.writes) == 2
    kafka.writes[-1][-1](None)
    assert len(replies) == 2


def test_history_command_pins_selection_when_outbox_is_full_and_is_idempotent(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "v1", max_pending_bytes=100) as store:
        collector = Collector(store, clock=lambda: 1000)
        for _ in range(5):
            collector.observe(sample(channel="ms_imu"))
        store.enqueue("event", "signal", b"x" * 100, 1000)
        command = dict(command_id="c1", vehicle_id="v1", type="history_request",
                       expires_at_ms=2000, start_ms=0, end_ms=1000)
        result = collector.command(command)
        assert result["status"] == "applied"
        assert result["records"] == 5
        assert collector.command(command) == result
        assert store.deferred_history() == 5
        assert store.drain_history() == 0
        store.prune_ring(10000000)
        assert store.ring_count() == 5
        assert store.acknowledge(ack_for(store.batch("event")))
        assert store.drain_history() > 0
        assert store.db.execute("SELECT COUNT(*) FROM receipts").fetchone()[0] == 1


def test_manual_focus_and_anomaly_episode_are_independent_across_restart(tmp_path):
    path = tmp_path / "edge.sqlite"
    with TelemetryStore(path, "v1") as store:
        c = Collector(store, clock=lambda: 1000)
        c.command(dict(command_id="focus", vehicle_id="v1", type="collection_policy", version=1,
                       expires_at_ms=121000, policy={"focus_until_ms": 120000}))
        c.observe(sample(900, channel="ms_imu"), now_ms=900)
        c.observe(sample(1000, channel="ms_imu"), now_ms=1000, anomaly=True)
        assert store.pending("history") == 2
        assert c.policy.focus_until_ms == 120000
    with TelemetryStore(path, "v1") as store:
        c = Collector(store, clock=lambda: 2000)
        c.observe(sample(2000, channel="ms_imu"), now_ms=2000, anomaly=True)
        assert store.pending("history") == 2  # Same incident, not another history request.
        assert c.policy.focus_until_ms == 120000
        c.observe(sample(33000, channel="ms_imu"), now_ms=33000, anomaly=True)
        assert store.pending("history") == 3  # New incident during the same manual focus.
        assert c.policy.focus_until_ms == 120000


def test_operator_policy_update_cannot_cancel_active_anomaly_focus(tmp_path):
    path = tmp_path / "edge.sqlite"
    with TelemetryStore(path, "v1") as store:
        c = Collector(store, clock=lambda: 2000)
        c.observe(sample(1000), now_ms=1000, anomaly=True)
        result = c.command(dict(command_id="normal", vehicle_id="v1", type="collection_policy",
                                version=1, expires_at_ms=10000, policy={"focus_until_ms": 0}))
        assert result["status"] == "applied"
        assert c.policy.interval(2000) == 1000
    with TelemetryStore(path, "v1") as store:
        c = Collector(store)
        assert c.policy.interval(2000) == 1000
        assert c.policy.interval(31000) == 10000


@pytest.mark.parametrize("policy", [
    {"thresholds": []}, {"thresholds": None}, {"thresholds": {"speed_mps": "fast"}},
    {"thresholds": {"speed_mps": True}}, {"channel_intervals_ms": []},
    {"channels": "ego_pose"}, {"focus_channels": None}, {"channels": [""]},
])
def test_invalid_policy_is_rejected_without_stopping_message_processing(tmp_path, policy):
    import queue
    from fleetsentinel_ingest.mqtt_contract import canonical
    from fleetsentinel_ingest.mqtt_runtime import Connection, EdgePump, edge_receive
    with TelemetryStore(tmp_path / "edge.sqlite", "v1") as store:
        c = Collector(store, clock=lambda: 1000)
        conn = object.__new__(Connection)
        conn.messages = queue.Queue()
        replies = []
        conn.publish = lambda topic, payload: replies.append(json.loads(payload))
        bad = dict(command_id="bad", vehicle_id="v1", type="collection_policy", version=1,
                   expires_at_ms=5000, policy=policy)
        good = dict(command_id="good", vehicle_id="v1", type="diagnostic", expires_at_ms=5000)
        for command in (bad, good):
            conn.messages.put(("fleet/v1/v1/command", canonical(command)))
        conn.drain(lambda t, p: edge_receive(c, EdgePump(store, conn.publish), conn, t, p))
        assert [r["status"] for r in replies] == ["rejected", "applied"]
        assert c.policy.version == 0
        assert c.command(bad) == replies[0]


def test_edge_pump_retry_and_puback_never_delete_outbox(tmp_path):
    from fleetsentinel_ingest.mqtt_runtime import EdgePump
    with TelemetryStore(tmp_path / "edge.sqlite", "v1") as store:
        sent = []
        pump = EdgePump(store, lambda topic, payload: sent.append((topic, payload)), retry_ms=1000)
        store.enqueue("status", "signal", b"a", 1000)
        store.enqueue("event", "signal", b"b", 1000)
        pump.tick(1000)
        assert len(sent) == 2 and "/event" in sent[0][0]
        pump.tick(1500)
        assert len(sent) == 2 and store.pending() == 2
        pump.tick(2500)
        assert len(sent) == 4 and sent[0] == sent[2]
        batch = Batch.parse(sent[0][1], sent[0][0])
        pump.receive(f"fleet/v1/v1/ack/event", json.dumps(ack_for(batch)).encode())
        assert store.pending("event") == 0


def test_command_dispatcher_keeps_desired_until_matching_report(tmp_path):
    from fleetsentinel_ingest.command_service import CommandStore
    command = dict(command_id="c1", vehicle_id="v1", type="diagnostic", expires_at_ms=5000)
    with CommandStore(tmp_path / "commands.sqlite") as service:
        service.submit(command, 1000)
        assert service.pending(1000) == [command]
        assert not service.report("fleet/v1/v2/reported", dict(vehicle_id="v1", command_id="c1", status="applied"))
    with CommandStore(tmp_path / "commands.sqlite") as service:
        assert service.pending(2000) == [command]
        assert service.report("fleet/v1/v1/reported", dict(vehicle_id="v1", command_id="c1", status="applied"))
        assert service.pending(2000) == []


def test_dispatcher_rejects_invalid_policy_before_persisting(tmp_path):
    from fleetsentinel_ingest.command_service import CommandStore
    base = dict(command_id="c1", vehicle_id="v1", type="collection_policy", version=1,
                expires_at_ms=5000)
    with CommandStore(tmp_path / "commands.sqlite") as service:
        for policy in ({"thresholds": []}, {"channels": "ego_pose"}, {"focus_until_ms": 6000},
                       {"unknown": 1}, None):
            with pytest.raises(ValueError):
                service.submit(base | {"policy": policy}, 1000)
            assert service.pending(1000) == []
        valid = base | {"policy": {"focus_until_ms": 4000}}
        service.submit(valid, 1000)
        assert service.pending(1000) == [valid]


def test_rejected_anomaly_admission_does_not_start_an_episode(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "v1", max_pending_bytes=80) as store:
        c = Collector(store)
        store.enqueue("status", "signal", b"x" * 80, 1000)
        with pytest.raises(CapacityError):
            c.observe(sample(1000), now_ms=1000, anomaly=True)
        assert c.anomaly_until_ms == 0
        assert store.get_meta("anomaly_until_ms") is None
        assert store.deferred_history() == store.ring_count() == 0
        store.acknowledge(ack_for(store.batch("status")))
        c.observe(sample(1000), now_ms=1000, anomaly=True)
        assert c.anomaly_until_ms == 31000
        assert store.pending("event") == 1
        assert store.pending("history") + store.deferred_history() == 1


def test_focus_expiry_announces_slower_cadence_without_waiting_normal_interval(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "v1") as store:
        collector = Collector(store, Policy(normal_ms=10000, focus_ms=1000, focus_until_ms=2000), clock=lambda: 1000)
        assert collector.observe(sample(), now_ms=1000) == "status"
        assert collector.observe(sample(2000), now_ms=2000) == "status"
        batch = store.batch("status")
        assert batch.records[-1].report_interval_ms == 10000


def test_size_pressure_retains_latest_ring(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "v1", max_ring_bytes=6) as store:
        store.record_local(b"1234", 1)
        store.record_local(b"5678", 2)
        assert store.ring_count() == 1


def test_abrupt_process_exit_keeps_committed_batch_and_rolls_back_partial_transaction(tmp_path):
    path = tmp_path / "edge.sqlite"
    child = '''
import os, sys
from fleetsentinel_ingest.telemetry_store import TelemetryStore
s = TelemetryStore(sys.argv[1], "v1")
s.enqueue("event", "signal", b"committed", 1000)
print(s.batch("event").batch_id, flush=True)
with s.atomic():
    s.enqueue("event", "signal", b"uncommitted", 1001)
    os._exit(17)
'''
    result = subprocess.run([sys.executable, "-c", child, str(path)], capture_output=True, text=True)
    assert result.returncode == 17, result.stderr
    with TelemetryStore(path, "v1") as store:
        assert store.pending() == 1
        assert store.batch("event").batch_id == result.stdout.strip()
        assert store.enqueue("event", "signal", b"next", 1002) == 1


def test_focus_only_accelerates_selected_channels_and_extra_channels_expire(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "v1") as store:
        policy = Policy(normal_ms=1000, min_ms=1000, focus_ms=100, focus_until_ms=2000,
                        channels=("ego_pose",), focus_channels=("ms_imu",),
                        channel_intervals_ms={"ego_pose": 3000})
        collector = Collector(store, policy)
        assert collector.observe(sample(1000), now_ms=1000) == "status"
        assert collector.observe(sample(1100), now_ms=1100) is None
        assert collector.observe(sample(1000, channel="ms_imu"), now_ms=1000) == "status"
        assert collector.observe(sample(1100, channel="ms_imu"), now_ms=1100) == "status"
        assert collector.observe(sample(2100, channel="ms_imu"), now_ms=2100) is None
        assert collector.observe(sample(4000), now_ms=4000) == "status"
        assert store.batch("status").records[-1].report_interval_ms == 3000
