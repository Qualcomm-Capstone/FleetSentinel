"""SQLite durability, bounded storage and transactional observation batches."""
import sqlite3
import subprocess
import sys

import pytest

from fleetsentinel_ingest.collection_policy import Collector
from fleetsentinel_ingest.mqtt_contract import ack_for
from fleetsentinel_ingest.telemetry_store import CapacityError, TelemetryStore
from tests.test_mqtt_collection import sample


def test_given_queued_samples_when_batched_then_one_full_sync_transaction(tmp_path):
    path = tmp_path / "edge.sqlite"
    with TelemetryStore(path, "v1") as store:
        collector = Collector(store)
        statements = []
        store.db.set_trace_callback(statements.append)

        collector.observe_batch([(sample(t), t) for t in range(1000, 1032)])

        assert statements.count("COMMIT") == 1
        assert store.db.execute("PRAGMA synchronous").fetchone()[0] == 2
        # A separate connection sees the entire batch before the call returns.
        with sqlite3.connect(path) as reader:
            assert reader.execute("SELECT COUNT(*) FROM ring").fetchone()[0] == 32
        assert store.pending("status") == 1


@pytest.mark.parametrize("failure", ["statement", "commit"])
def test_given_batch_failure_when_retried_then_database_and_policy_cursor_rollback(tmp_path, failure):
    with TelemetryStore(tmp_path / "edge.sqlite", "v1") as store:
        collector = Collector(store)
        collector.observe(sample(1000), now_ms=1000)
        before = dict(collector.last)
        if failure == "statement":
            store.db.executescript("""
                CREATE TRIGGER fail BEFORE INSERT ON ring WHEN NEW.created_ms=21000
                BEGIN SELECT RAISE(ABORT, 'injected statement failure'); END;
            """)
        else:
            store.db.execute("PRAGMA foreign_keys=ON")
            store.db.executescript("""
                CREATE TABLE parent(id INTEGER PRIMARY KEY);
                CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);
                CREATE TRIGGER fail AFTER INSERT ON ring BEGIN INSERT INTO child VALUES(1); END;
            """)
        batch = [(sample(t), t) for t in (11000, 21000)]

        with pytest.raises(sqlite3.IntegrityError):
            collector.observe_batch(batch)

        assert collector.last == before
        assert store.ring_count() == store.pending("status") == 1
        assert not store.db.in_transaction
        store.db.execute("DROP TRIGGER fail")
        collector.observe_batch(batch)
        assert store.ring_count() == store.pending("status") == 3
        assert [r.seq for r in store.batch("status").records] == [0, 1, 2]


def test_given_database_limit_when_full_then_rollback_and_preserve_unacked_batch(tmp_path):
    path = tmp_path / "edge.sqlite"
    with TelemetryStore(path, "v1", max_db_bytes=256 * 1024) as store:
        store.enqueue("event", "signal", b"keep", 1000)
        original = store.batch("event")
        store.record_local(b"history", 1000)
        store.defer_history(0, 1000)

        with pytest.raises(CapacityError, match="SQLite"):
            with store.atomic():
                store.set_meta("large-metadata", "x" * (512 * 1024))

        assert not store.db.in_transaction
        assert store.get_meta("large-metadata") is None
        assert store.deferred_history() == 1
        assert store.batch("event").wire() == original.wire()
        assert store.storage_stats()["database_bytes"] <= 256 * 1024
        assert store.acknowledge(ack_for(original))
    with TelemetryStore(path, "v1", max_db_bytes=256 * 1024) as store:
        assert store.pending() == 0
        assert store.deferred_history() == 1
        assert store.storage_stats()["max_database_bytes"] == 256 * 1024


def test_given_existing_database_when_limit_too_small_then_reject_without_data_loss(tmp_path):
    path = tmp_path / "edge.sqlite"
    with TelemetryStore(path, "v1") as store:
        with store.atomic():
            store.set_meta("metadata", "x" * (512 * 1024))

    with pytest.raises(ValueError, match="existing database"):
        TelemetryStore(path, "v1", max_db_bytes=256 * 1024)

    with TelemetryStore(path, "v1") as store:
        assert len(store.get_meta("metadata")) == 512 * 1024


def test_given_full_disk_during_history_promotion_then_event_and_all_pins_survive(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "v1", max_db_bytes=256 * 1024) as store:
        collector = Collector(store)
        collector.observe(sample(900), now_ms=900)
        store.db.executescript("""
            CREATE TABLE padding(data BLOB);
            CREATE TRIGGER fill BEFORE INSERT ON outbox
            WHEN NEW.stream='history' AND NEW.seq=1
            BEGIN INSERT INTO padding VALUES(zeroblob(524288)); END;
        """)

        assert collector.observe(sample(1000), now_ms=1000, anomaly=True) == "event"

        assert store.pending("event") == 1
        assert store.pending("history") == 0
        assert store.deferred_history() == 2
        assert store.db.execute("SELECT next_seq FROM counters WHERE stream='history'").fetchone() is None
        store.db.execute("DROP TRIGGER fill")
        assert store.drain_history() == 2
        assert [r.seq for r in store.batch("history").records] == [0, 1]
        assert store.deferred_history() == 0


def test_given_wal_growth_when_reset_then_reclaim_and_report_storage(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "v1") as store:
        assert store.db.execute("PRAGMA journal_size_limit").fetchone()[0] == 8 * 1024 ** 2
        # Use a small retention target to exercise SQLite's real WAL reset.
        store.db.execute("PRAGMA journal_size_limit=32768")
        with store.atomic():
            store.set_meta("metadata", "x" * (128 * 1024))
        before = store.storage_stats()["wal_bytes"]
        assert before > 32768

        store.db.execute("PRAGMA wal_checkpoint(RESTART)")
        with store.atomic():
            store.set_meta("small", "value")

        assert store.storage_stats()["wal_bytes"] <= 32768
        assert store.db.execute("PRAGMA wal_autocheckpoint").fetchone()[0] == 1000
        report = Collector(store, clock=lambda: 1000).command(dict(
            command_id="inspect", vehicle_id="v1", type="diagnostic", expires_at_ms=5000))
        assert report["storage"]["database_bytes"] > 0
        assert report["storage"]["filesystem_free_bytes"] > 0


@pytest.mark.parametrize("crash_before_commit", [True, False])
def test_given_process_exit_when_reopened_then_only_committed_batch_survives(tmp_path, crash_before_commit):
    path = tmp_path / "edge.sqlite"
    program = """
import os, sys
from fleetsentinel_ingest.telemetry_store import TelemetryStore
from fleetsentinel_ingest.collection_policy import Collector
from tests.test_mqtt_collection import sample
store = TelemetryStore(sys.argv[1], 'v1')
collector = Collector(store)
observe = collector.observe
def crashing_observe(row, **kwargs):
    observe(row, **kwargs)
    if sys.argv[2] == 'True' and kwargs['now_ms'] == 11000:
        os._exit(73)
collector.observe = crashing_observe
collector.observe_batch([(sample(t), t) for t in (1000, 11000)])
os._exit(73)
"""
    result = subprocess.run([sys.executable, "-c", program, str(path), str(crash_before_commit)],
                            check=False, timeout=10)

    assert result.returncode == 73
    with TelemetryStore(path, "v1") as store:
        assert store.ring_count() == store.pending("status") == (0 if crash_before_commit else 2)
