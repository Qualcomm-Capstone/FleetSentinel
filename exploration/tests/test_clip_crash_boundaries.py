"""Fault injection against the real raw manifest and reopened SQLite database.

The remote transport is a deterministic stand-in. Real broker/object-store/DB
behavior is covered separately by ClipUploadIT and test_clip_mqtt_integration.
"""
import json
import sqlite3

import pytest

from fleetsentinel_ingest.clip_upload import ClipUploader
from fleetsentinel_ingest.raw_recorder import RawRecorder
from fleetsentinel_ingest.raw_store import RawStore
from fleetsentinel_ingest.telemetry_store import TelemetryStore
from .test_clip_upload import Transport


@pytest.mark.parametrize("boundary", [
    "put_reply_lost", "put_before_local_commit", "complete_reply_lost",
    "receipt_before_local_commit", "receipt_after_local_commit",
])
def test_restart_at_upload_commit_boundaries_preserves_file_and_reference_identity(tmp_path, boundary):
    database, root = tmp_path / "edge.sqlite", tmp_path / "raw"
    clock, transport = [1_000_000], Transport()
    store = TelemetryStore(database, "v1")
    try:
        raw = RawStore(store, root, retention_us=100, clock_us=lambda: clock[0])
        recorder = RawRecorder(raw)
        recorder.append(10, "/camera/front", b"frame-one")
        recorder.append(20, "/camera/front", b"frame-two")
        recorder.close()
        raw.request_clip("operator-1", 10, 21)
        file = raw.pending_files()[0]
        worker = ClipUploader(store, raw, transport, clock_us=lambda: clock[0], jitter=lambda: 0)
        worker.step()  # durable create identity and job binding

        if boundary == "put_reply_lost":
            transport.fail = OSError("object accepted bytes, response disappeared")
            worker.step()
            transport.fail = None
        elif boundary == "put_before_local_commit":
            original_save = worker._save

            def fail_part_commit(file_id, local):
                if local["parts"]:
                    raise sqlite3.OperationalError("disk unavailable after PUT")
                original_save(file_id, local)

            worker._save = fail_part_commit
            with pytest.raises(sqlite3.OperationalError):
                worker.step()
        else:
            worker.step()  # part and ETag committed
            if boundary == "complete_reply_lost":
                original_control = transport.control

                def lose_complete_reply(method, path, body=None):
                    result = original_control(method, path, body)
                    if path.endswith("/complete"):
                        raise TimeoutError("server accepted completion; response lost")
                    return result

                transport.control = lose_complete_reply
                worker.step()
                transport.control = original_control
            else:
                transport.final = "COMPLETED"
                if boundary == "receipt_before_local_commit":
                    # Fail after receipt and pin updates, before the outer commit.
                    # A real SQLite trigger proves the three tables roll back together.
                    store.db.executescript("""
                        CREATE TEMP TRIGGER reject_completion BEFORE UPDATE ON clip_uploads
                        WHEN NEW.state='COMPLETED'
                        BEGIN SELECT RAISE(ABORT, 'injected receipt commit failure'); END;
                    """)
                    with pytest.raises(sqlite3.IntegrityError):
                        worker.step()
                    assert store.db.execute("SELECT receipt FROM raw_files").fetchone()[0] is None
                else:
                    worker.step()

        committed = boundary == "receipt_after_local_commit"
        assert store.db.execute("SELECT pinned FROM raw_selections").fetchone()[0] == int(not committed)
        if not committed:
            clock[0] += 10_000_000
            raw.prune()  # retention elapsed, but an unconfirmed file must survive
            assert file["path"].is_file()
        manifest = store.db.execute("SELECT manifest FROM clip_uploads").fetchone()[0]
        store.close()

        # Reopen the database and spool, not just the uploader object.
        store = TelemetryStore(database, "v1")
        raw = RawStore(store, root, retention_us=100, clock_us=lambda: clock[0])
        worker = ClipUploader(store, raw, transport, clock_us=lambda: clock[0], jitter=lambda: 0)
        if committed:
            assert not worker.step()  # no network work after a durable receipt
        else:
            transport.final = "COMPLETED"
            for _ in range(6):
                worker.step()
                clock[0] += 2_000_000
                if worker.status_for_file(file["id"])["state"] == "COMPLETED":
                    break
        assert worker.status_for_file(file["id"])["state"] == "COMPLETED"
        assert raw.request_status("operator-1")["uploaded_file_ids"] == [file["id"]]
        assert store.db.execute("SELECT manifest FROM clip_uploads").fetchone()[0] == manifest
        assert json.loads(manifest)["request_id"] == file["id"]
        assert len(transport.creates) == 1
        expected_puts = 2 if boundary.startswith("put_") else 1
        assert len(transport.puts) == expected_puts
        assert all(data == transport.puts[0][1] for _, data in transport.puts)
        assert not raw.pending_files()
        raw.prune(now_us=clock[0] + 10_000_000)
        assert not file["path"].exists()
    finally:
        store.close()
