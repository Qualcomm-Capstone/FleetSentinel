"""Managed raw spool invariants; all time and capacity limits are deterministic."""
import hashlib

import pytest
from mcap.reader import make_reader

from fleetsentinel_ingest.raw_recorder import RawRecorder, replay_mcap
from fleetsentinel_ingest.raw_store import RawStore
from fleetsentinel_ingest.telemetry_store import CapacityError, TelemetryStore


@pytest.fixture
def spool(tmp_path):
    telemetry = TelemetryStore(tmp_path / "edge.sqlite", "vehicle-1")
    store = RawStore(telemetry, tmp_path / "raw", retention_us=100, clock_us=lambda: 1000)
    yield store
    telemetry.close()


def record(store, times, **kwargs):
    recorder = RawRecorder(store, **kwargs)
    for timestamp in times:
        recorder.append(timestamp, "/camera/front", b"pixels", message_encoding="jpeg")
    recorder.close()


def test_future_and_overlapping_requests_share_files_and_expose_missing(spool):
    spool.request_clip("one", 0, 30, now_us=1000)
    spool.request_clip("two", 10, 35, now_us=1000)
    record(spool, [10, 19, 20, 29], rotation_us=10)
    assert len(spool.pending_files()) == 2
    first = spool.request_status("one")
    assert first["coverage"] == [[10, 30]]
    assert first["missing"] == [[0, 10]]
    assert first["partial"] is True
    assert spool.request_status("two")["missing"] == [[30, 35]]
    assert len(spool.request_status("two")["file_ids"]) == 2


def test_request_and_command_receipt_rollback_together(spool):
    record(spool, [1, 2])
    with pytest.raises(RuntimeError):
        with spool.telemetry.atomic():
            spool.request_clip("command", 0, 3, now_us=1000)
            spool.telemetry.db.execute("INSERT INTO receipts VALUES (?,?,?)", ("command", b"request", b"ack"))
            raise RuntimeError("crash before commit")
    assert spool.request_status("command") is None
    assert spool.pending_files() == []


def test_pinned_capacity_and_completed_receipt_binding(spool):
    spool.request_clip("one", 0, 10, now_us=1000)
    record(spool, [1, 9])
    item = spool.pending_files()[0]
    spool.max_bytes = item["size_bytes"]
    with pytest.raises(CapacityError):
        spool.ensure_space(1, now_us=2000)
    assert item["path"].exists()
    spool.bind_upload(item["id"], "job-1")
    for receipt in ({"id": "job-1", "state": "STORED"}, {"id": "job-2", "state": "COMPLETED"}):
        with pytest.raises(ValueError):
            spool.complete_file(item["id"], receipt)
    spool.complete_file(item["id"], {"id": "job-1", "state": "COMPLETED"})
    spool.prune(now_us=2000)
    assert not item["path"].exists()
    assert spool.request_status("one")["uploaded_file_ids"] == [item["id"]]


def test_coverage_does_not_bridge_a_missing_segment(spool):
    record(spool, [0, 9])
    record(spool, [20, 29])
    result = spool.request_clip("gap", 0, 30, now_us=1000)
    assert result["coverage"] == [[0, 10], [20, 30]]
    assert result["missing"] == [[10, 20]]


def test_stable_anomaly_and_request_fingerprints(spool):
    a = spool.anomaly("episode", 20_000_000, now_us=1000)
    assert a == spool.anomaly("episode", 20_000_000, now_us=1000)
    assert a["requested"] == [10_000_000, 40_000_000]
    with pytest.raises(ValueError):
        spool.request_clip("episode", 0, 1, now_us=1000)
    with pytest.raises(ValueError):
        spool.request_clip("too-long", 0, 60_000_001, now_us=1000)


def test_crash_recovers_only_synced_messages_and_preserves_pin(spool):
    spool.request_clip("crash", 0, 100, now_us=1000)
    recorder = RawRecorder(spool)
    recorder.append(10, "/camera/front", b"frame")
    recorder.append(20, "/camera/front", b"frame")
    recorder._fp.write(b"\x05\xff")  # torn trailing MCAP record, never a message
    recorder._fp.close()
    recovered = RawStore(spool.telemetry, spool.root, clock_us=lambda: 1000)
    item = recovered.pending_files()[0]
    assert item["t_start_us"] == 10
    assert item["t_end_us"] == 21
    assert recovered.request_status("crash")["missing"] == [[0, 10], [21, 100]]
    with item["path"].open("rb") as stream:
        assert len(list(make_reader(stream).iter_messages())) == 2


def test_replay_rotates_self_contained_files_without_touching_source(spool, tmp_path):
    source_store = RawStore(spool.telemetry, tmp_path / "source-managed", clock_us=lambda: 1000)
    recorder = RawRecorder(source_store, calibration={"CAM": '{"translation":[1,2,3]}'})
    for t in [0, 9, 10, 19]:
        recorder.append(t, "/vehicle/signal", b"{}", message_encoding="json",
                        schema=("signal", "jsonschema", b'{"type":"object"}'))
    recorder.close()
    source = next(source_store.root.glob("*.mcap"))
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    ticks = []
    replay_mcap(source, RawRecorder(spool, rotation_us=10), clock=ticks.append, shift_us=100)
    assert ticks == [100, 109, 110, 119]
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before
    spool.request_clip("replay", 100, 120, now_us=1000)
    for item in spool.pending_files():
        assert item["calibration"]["CAM"] == '{"translation":[1,2,3]}'
        with item["path"].open("rb") as stream:
            messages = list(make_reader(stream).iter_messages())
        assert all(schema.name == "signal" for schema, _, _ in messages)


def test_unmarked_dataset_is_never_adopted_or_deleted(tmp_path):
    root = tmp_path / "dataset"
    root.mkdir()
    source = root / "original.mcap"
    source.write_bytes(b"untouchable")
    with TelemetryStore(tmp_path / "edge.sqlite", "vehicle-1") as telemetry:
        with pytest.raises(ValueError):
            RawStore(telemetry, root)
    assert source.read_bytes() == b"untouchable"


def test_retention_uses_local_time_not_historical_sensor_time(spool):
    record(spool, [1, 2])
    path = next(spool.root.glob("*.mcap"))
    spool.prune(now_us=1050)
    assert path.exists()
    spool.prune(now_us=1101)
    assert not path.exists()


def test_exact_size_rotation_and_total_capacity_accounting(spool):
    spool.request_clip("bytes", 0, 20, now_us=1000)
    recorder = RawRecorder(spool, rotation_bytes=600)
    for timestamp in range(10):
        recorder.append(timestamp, "/camera/front", b"x" * 100)
    recorder.close()
    items = spool.pending_files()
    assert len(items) > 1
    assert all(item["size_bytes"] <= 600 for item in items)
    assert spool.used_bytes() == sum(item["path"].stat().st_size for item in items)
    with pytest.raises(CapacityError):
        RawRecorder(spool, rotation_bytes=600).append(100, "/big", b"x" * 600)


def test_deletion_intent_is_recovered_after_unlink_failure(spool, monkeypatch):
    record(spool, [0, 9])
    path = next(spool.root.glob("*.mcap"))
    def fail(_):
        raise OSError("simulated crash after deletion intent commit")
    monkeypatch.setattr(spool, "_finish_delete", fail)
    with pytest.raises(OSError):
        spool.prune(now_us=2000)
    # Once deletion intent committed, a later request must report missing.
    assert spool.request_clip("late", 0, 10, now_us=2000)["coverage"] == []
    recovered = RawStore(spool.telemetry, spool.root)
    assert not path.exists()
    assert recovered.used_bytes() == 0


def test_ack_and_pin_survive_database_restart(tmp_path):
    database = tmp_path / "edge.sqlite"
    telemetry = TelemetryStore(database, "vehicle-1")
    store = RawStore(telemetry, tmp_path / "raw")
    store.request_clip("durable", 0, 10, now_us=1)
    record(store, [1, 9])
    item = store.pending_files()[0]
    store.bind_upload(item["id"], "job")
    telemetry.close()
    with TelemetryStore(database, "vehicle-1") as restarted:
        store = RawStore(restarted, tmp_path / "raw")
        assert store.pending_files()[0]["id"] == item["id"]
        store.complete_file(item["id"], {"id": "job", "state": "COMPLETED"})
        assert store.pending_files() == []
        assert store.request_status("durable")["uploaded_file_ids"] == [item["id"]]


def test_capacity_blocked_is_visible_and_recoverable(spool):
    spool.request_clip("pin", 0, 10, now_us=1000)
    record(spool, [1, 9])
    item = spool.pending_files()[0]
    spool.max_bytes = item["size_bytes"]
    with pytest.raises(CapacityError):
        spool.ensure_space(1)
    assert spool.diagnostics()["capacity_blocked"]
    assert spool.diagnostics()["pinned_files"] == 1
    spool.max_bytes += 1
    spool.ensure_space(1)
    assert not spool.diagnostics()["capacity_blocked"]


def test_replay_refuses_its_own_prunable_spool_as_source(spool):
    record(spool, [0, 1])
    source = next(spool.root.glob("*.mcap"))
    with pytest.raises(ValueError, match="outside"):
        replay_mcap(source, RawRecorder(spool))
    assert source.exists()


def test_empty_crash_partial_is_accounted_and_pinned_until_resolved(spool):
    spool.request_clip("empty", 0, 10, now_us=1000)
    file_id, path = spool.begin_file(1, {"scene_id": None, "calibration": {}})
    path.write_bytes(b"\x89MCAP0\r\n")
    recovered = RawStore(spool.telemetry, spool.root, max_bytes=8)
    assert recovered.request_status("empty")["coverage"] == []
    assert recovered.used_bytes() == 8
    with pytest.raises(CapacityError):
        recovered.ensure_space(1)
    assert path.exists()


def test_prune_waits_for_other_thread_transaction_not_spurious_failure(spool):
    import threading
    entered, release = threading.Event(), threading.Event()
    failures = []
    def transaction():
        with spool.telemetry.atomic():
            entered.set()
            release.wait(timeout=5)
    owner = threading.Thread(target=transaction)
    owner.start()
    assert entered.wait(timeout=5)
    def prune():
        try:
            spool.ensure_space(1)
        except Exception as error:
            failures.append(error)
    pruner = threading.Thread(target=prune)
    pruner.start()
    release.set()
    owner.join(timeout=5)
    pruner.join(timeout=5)
    assert not owner.is_alive() and not pruner.is_alive()
    assert failures == []


def test_replay_maps_first_message_to_local_clock_after_metadata_pass(spool, tmp_path):
    source_store = RawStore(spool.telemetry, tmp_path / "source-managed")
    record(source_store, [123, 128])
    source = next(source_store.root.glob("*.mcap"))
    ticks = []
    replay_mcap(source, RawRecorder(spool, clock_us=lambda: 5_000), clock=ticks.append, shift_us=None)
    assert ticks == [5000, 5005]
