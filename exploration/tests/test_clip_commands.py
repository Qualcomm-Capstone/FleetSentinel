"""MQTT clip commands share the telemetry receipt transaction."""
import json

import pytest

from fleetsentinel_ingest.collection_policy import Collector
from fleetsentinel_ingest.command_service import CommandStore
from fleetsentinel_ingest.telemetry_store import TelemetryStore


class Raw:
    def __init__(self, store):
        self.store = store
        with store.atomic():
            store.db.execute("CREATE TABLE test_intents (id TEXT PRIMARY KEY, start_us INTEGER, end_us INTEGER)")

    def request_clip(self, request_id, start_us, end_us, *, now_us, kind="operator"):
        self.store.db.execute("INSERT INTO test_intents VALUES (?,?,?)", (request_id, start_us, end_us))
        return {"request_id": request_id, "coverage": "partial", "files": []}

    def anomaly(self, request_id, event_us, *, now_us=None):
        return self.request_clip(request_id, event_us - 10000000, event_us + 20000000, now_us=now_us)

    def request_status(self, request_id):
        return {"request_id": request_id, "coverage": "partial", "files": []}

    def diagnostics(self):
        return {"pending_requests": self.store.db.execute("SELECT COUNT(*) FROM test_intents").fetchone()[0]}


def command(**changes):
    return dict(command_id="request-1", vehicle_id="v1", type="clip_request",
                start_us=500000, end_us=1000000, expires_at_ms=5000) | changes


def sample():
    return dict(scene_id="test", channel="ego_pose", sensor_time=1000000,
                log_time=1000000, values_num={}, values_vec={}, values_str={})


def test_given_clip_request_when_duplicate_then_single_durable_intent_and_replayable_receipt(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "v1") as store:
        raw = Raw(store)
        collector = Collector(store, clock=lambda: 1000, raw_store=raw)
        first = collector.command(command())
        assert first["status"] == "applied"
        assert first["clip"]["coverage"] == "partial"
        assert collector.command(command()) == first
        assert raw.diagnostics()["pending_requests"] == 1
        assert collector.command(command(end_us=900000))["status"] == "rejected"
        assert collector.command(command(command_id="query", type="clip_status", request_id="request-1"))["clip"]["request_id"] == "request-1"


def test_given_receipt_disk_failure_when_clip_accepted_then_intent_rolls_back(tmp_path):
    import sqlite3
    with TelemetryStore(tmp_path / "edge.sqlite", "v1") as store:
        raw = Raw(store)
        collector = Collector(store, clock=lambda: 1000, raw_store=raw)
        store.db.execute("CREATE TRIGGER refuse_receipt BEFORE INSERT ON receipts BEGIN SELECT RAISE(ABORT,'disk full'); END")
        with pytest.raises(sqlite3.IntegrityError):
            collector.command(command())
        assert raw.diagnostics()["pending_requests"] == 0


def test_given_one_anomaly_episode_when_multiple_samples_then_one_raw_intent(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "v1") as store:
        raw = Raw(store)
        collector = Collector(store, raw_store=raw)
        collector.observe(sample(), now_ms=1000, anomaly=True)
        collector.observe(sample(), now_ms=2000, anomaly=True)
        assert raw.diagnostics()["pending_requests"] == 1
        assert store.pending("event") == 2


def test_given_delayed_anomaly_when_dequeued_then_select_sensor_time_clip_with_receive_time_acceptance(tmp_path):
    from fleetsentinel_ingest.raw_recorder import RawRecorder
    from fleetsentinel_ingest.raw_store import RawStore
    with TelemetryStore(tmp_path / "edge.sqlite", "v1") as store:
        raw = RawStore(store, tmp_path / "managed", clock_us=lambda: 140000000)
        recorder = RawRecorder(raw)
        recorder.append(100000000, "/camera/front", b"event frame")
        recorder.close()
        recorder.append(140000000, "/camera/front", b"later frame")
        recorder.close()
        collector = Collector(store, raw_store=raw)
        collector.observe(sample() | {"sensor_time": 100000000}, now_ms=140000, anomaly=True)
        request_id, start, end, accepted = store.db.execute(
            "SELECT id,start_us,end_us,accepted_us FROM raw_requests").fetchone()
        assert (start, end, accepted) == (90000000, 120000000, 140000000)
        selected = raw.pending_files()
        assert len(selected) == 1 and selected[0]["t_start_us"] == 100000000
        assert raw.request_status(request_id)["file_ids"] == [selected[0]["id"]]
        assert collector.anomaly_until_ms == 170000  # Policy lifetime remains receive-time based.


def test_given_two_buffered_incidents_and_repeats_when_focus_is_active_then_two_durable_raw_episodes(tmp_path):
    from fleetsentinel_ingest.raw_store import RawStore
    database = tmp_path / "edge.sqlite"
    with TelemetryStore(database, "v1") as store:
        raw = RawStore(store, tmp_path / "managed")
        collector = Collector(store, raw_store=raw)
        for event_us, received_ms in ((100000000, 250000), (200000000, 251000), (201000000, 252000)):
            collector.observe(sample() | {"sensor_time": event_us}, now_ms=received_ms, anomaly=True)
        assert store.db.execute("SELECT start_us,end_us FROM raw_requests ORDER BY start_us").fetchall() == [
            (90000000, 120000000), (190000000, 220000000)]
        assert store.pending("event") == 3  # Existing signal path remains per sample.
    with TelemetryStore(database, "v1") as store:
        raw = RawStore(store, tmp_path / "managed")
        collector = Collector(store, raw_store=raw)
        # Replay of the old event after a later episode and process restart must
        # find its durable event-time episode, not create a third raw request.
        collector.observe(sample() | {"sensor_time": 100000000}, now_ms=253000, anomaly=True)
        assert store.db.execute("SELECT COUNT(*) FROM raw_requests").fetchone()[0] == 2
        assert store.pending("event") == 4


@pytest.mark.parametrize("second_event_us", [75000000, 120000000, 125000000])
def test_given_event_outside_existing_post_window_when_observed_then_create_its_own_clip(tmp_path, second_event_us):
    from fleetsentinel_ingest.raw_store import RawStore
    with TelemetryStore(tmp_path / "edge.sqlite", "v1") as store:
        raw = RawStore(store, tmp_path / "managed")
        collector = Collector(store, raw_store=raw)
        collector.observe(sample() | {"sensor_time": 100000000}, now_ms=250000, anomaly=True)
        collector.observe(sample() | {"sensor_time": second_event_us}, now_ms=251000, anomaly=True)
        windows = store.db.execute("SELECT start_us,end_us FROM raw_requests ORDER BY start_us").fetchall()
        assert windows == sorted([(90000000, 120000000),
                                  (second_event_us - 10000000, second_event_us + 20000000)])


@pytest.mark.parametrize("sensor_time", [-1, True, "100000000"])
def test_given_invalid_anomaly_timestamp_when_observed_then_reject_without_now_fallback(tmp_path, sensor_time):
    with TelemetryStore(tmp_path / "edge.sqlite", "v1") as store:
        raw = Raw(store)
        collector = Collector(store, raw_store=raw)
        with pytest.raises(ValueError, match="sensor_time"):
            collector.observe(sample() | {"sensor_time": sensor_time}, now_ms=140000, anomaly=True)
        assert raw.diagnostics()["pending_requests"] == 0
        assert store.pending() == 0


@pytest.mark.parametrize("changes", [{"start_us": True}, {"end_us": 61000001, "start_us": 0},
                                     {"start_us": 2, "end_us": 1}, {"start_us": -1},
                                     {"type": "clip_status", "request_id": "bad/request"}])
def test_given_invalid_clip_command_when_operator_submits_then_reject(changes, tmp_path):
    with CommandStore(tmp_path / "operator.sqlite") as store:
        with pytest.raises(ValueError):
            store.submit(command(**changes), now_ms=1000)


def test_given_valid_clip_commands_when_operator_submits_then_pending_commands_keep_shape(tmp_path):
    with CommandStore(tmp_path / "operator.sqlite") as store:
        store.submit(command(), now_ms=1000)
        status = command(command_id="query", type="clip_status", request_id="request-1")
        store.submit(status, now_ms=1000)
        assert store.pending(1000) == [status, command()]


def test_given_raw_disabled_when_clip_request_then_explicit_rejection(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "v1") as store:
        result = Collector(store, clock=lambda: 1000).command(command())
        assert result["status"] == "rejected"
        assert "raw" in result["reason"]
