"""Transactional edge outbox and bounded local history.

SQLite WAL + FULL synchronous mode makes command effects, sequence allocation and
storage receipts atomic. A disk/size failure is surfaced, never treated as an ACK.
Use one owning process per database; SQLite handles threads inside that process.
"""
from __future__ import annotations

import sqlite3
import threading
import uuid
from contextlib import contextmanager

from .mqtt_contract import Batch, Record, STREAMS, MAX_BYTES, MAX_RECORDS, ack_for, identity, canonical


class CapacityError(RuntimeError):
    pass


class TelemetryStore:
    def __init__(self, path, vehicle_id, *, max_batch_bytes=MAX_BYTES,
                 max_pending_bytes=256 * 1024 * 1024, retention_ms=60 * 60 * 1000,
                 max_ring_bytes=256 * 1024 * 1024):
        self.vehicle_id = identity(vehicle_id)
        if not 512 <= max_batch_bytes <= MAX_BYTES or min(max_pending_bytes, retention_ms, max_ring_bytes) <= 0:
            raise ValueError("invalid storage bounds")
        self.max_batch_bytes, self.max_pending_bytes = max_batch_bytes, max_pending_bytes
        self.retention_ms, self.max_ring_bytes = retention_ms, max_ring_bytes
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS counters (stream TEXT PRIMARY KEY, next_seq INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox (
                stream TEXT, seq INTEGER, kind TEXT, payload BLOB, created_ms INTEGER,
                interval_ms INTEGER, PRIMARY KEY(stream, seq));
            CREATE TABLE IF NOT EXISTS batches (stream TEXT PRIMARY KEY, wire BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS ring (
                id INTEGER PRIMARY KEY, created_ms INTEGER, payload BLOB NOT NULL);
            CREATE INDEX IF NOT EXISTS ring_time ON ring(created_ms);
            CREATE TABLE IF NOT EXISTS history_pending (ring_id INTEGER PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS receipts (id TEXT PRIMARY KEY, command BLOB, result BLOB);
            CREATE TABLE IF NOT EXISTS usage (name TEXT PRIMARY KEY, bytes INTEGER NOT NULL);
            INSERT OR IGNORE INTO usage SELECT 'outbox', COALESCE(SUM(length(payload)),0) FROM outbox;
            INSERT OR IGNORE INTO usage SELECT 'ring', COALESCE(SUM(length(payload)),0) FROM ring;
            CREATE TRIGGER IF NOT EXISTS outbox_insert AFTER INSERT ON outbox BEGIN
                UPDATE usage SET bytes=bytes+length(NEW.payload) WHERE name='outbox'; END;
            CREATE TRIGGER IF NOT EXISTS outbox_delete AFTER DELETE ON outbox BEGIN
                UPDATE usage SET bytes=bytes-length(OLD.payload) WHERE name='outbox'; END;
            CREATE TRIGGER IF NOT EXISTS ring_insert AFTER INSERT ON ring BEGIN
                UPDATE usage SET bytes=bytes+length(NEW.payload) WHERE name='ring'; END;
            CREATE TRIGGER IF NOT EXISTS ring_delete AFTER DELETE ON ring BEGIN
                UPDATE usage SET bytes=bytes-length(OLD.payload) WHERE name='ring'; END;
        """)
        with self.atomic():
            stored = self.get_meta("vehicle_id")
            if stored and stored != vehicle_id:
                raise ValueError("database belongs to another vehicle")
            self.set_meta("vehicle_id", vehicle_id)
            self.boot_id = self.get_meta("boot_id") or uuid.uuid4().hex
            self.set_meta("boot_id", self.boot_id)

    @contextmanager
    def atomic(self):
        with self.lock:
            # Nested operations participate in the outer command/observation transaction.
            nested = self.db.in_transaction
            if not nested:
                self.db.execute("BEGIN IMMEDIATE")
            try:
                yield
                if not nested:
                    self.db.commit()
            except BaseException:
                if not nested:
                    self.db.rollback()
                raise

    def get_meta(self, key):
        with self.lock:
            row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return row[0] if row else None

    def set_meta(self, key, value):
        self.db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, value))

    def enqueue(self, stream, kind, payload, created_ms, interval_ms=1000):
        if stream not in STREAMS:
            raise ValueError("invalid stream")
        with self.atomic():
            row = self.db.execute("SELECT next_seq FROM counters WHERE stream=?", (stream,)).fetchone()
            seq = row[0] if row else 0
            record = Record(seq, kind, payload, interval_ms)
            if len(Batch.create(self.vehicle_id, self.boot_id, stream, [record]).wire()) > self.max_batch_bytes:
                raise CapacityError("record exceeds batch limit; raw media belongs in object storage")
            size = self.db.execute("SELECT bytes FROM usage WHERE name='outbox'").fetchone()[0]
            if size + len(payload) > self.max_pending_bytes:
                raise CapacityError("upload outbox full; operator intervention required")
            self.db.execute("INSERT INTO outbox VALUES (?,?,?,?,?,?)",
                            (stream, seq, kind, payload, created_ms, interval_ms))
            self.db.execute("INSERT OR REPLACE INTO counters VALUES (?,?)", (stream, seq + 1))
            return seq

    def batch(self, stream):
        with self.atomic():
            row = self.db.execute("SELECT wire FROM batches WHERE stream=?", (stream,)).fetchone()
            if row:
                return Batch.parse(row[0], f"fleet/v1/{self.vehicle_id}/up/{stream}")
            rows = self.db.execute("SELECT seq,kind,payload,interval_ms FROM outbox WHERE stream=? ORDER BY seq LIMIT ?",
                                   (stream, MAX_RECORDS)).fetchall()
            chosen, batch = [], None
            for row in rows:
                candidate = Batch.create(self.vehicle_id, self.boot_id, stream, chosen + [Record(*row)])
                if len(candidate.wire()) > self.max_batch_bytes:
                    break
                chosen.append(Record(*row))
                batch = candidate
            if batch:
                self.db.execute("INSERT INTO batches VALUES (?,?)", (stream, batch.wire()))
            return batch

    def acknowledge(self, ack):
        if not isinstance(ack, dict) or ack.get("stream") not in STREAMS:
            return False
        with self.atomic():
            row = self.db.execute("SELECT wire FROM batches WHERE stream=?", (ack["stream"],)).fetchone()
            if not row:
                return False
            batch = Batch.parse(row[0], f"fleet/v1/{self.vehicle_id}/up/{ack['stream']}")
            if canonical(ack) != canonical(ack_for(batch)):
                return False
            self.db.execute("DELETE FROM outbox WHERE stream=? AND seq BETWEEN ? AND ?",
                            (batch.stream, batch.records[0].seq, batch.records[-1].seq))
            self.db.execute("DELETE FROM batches WHERE stream=?", (batch.stream,))
            return True

    def pending(self, stream=None):
        with self.lock:
            return self.db.execute("SELECT COUNT(*) FROM outbox" + (" WHERE stream=?" if stream else ""),
                                   (stream,) if stream else ()).fetchone()[0]

    def record_local(self, payload, now_ms):
        with self.atomic():
            self.db.execute("INSERT INTO ring(created_ms,payload) VALUES (?,?)", (now_ms, payload))
            self.prune_ring(now_ms)
            size = self.db.execute("SELECT bytes FROM usage WHERE name='ring'").fetchone()[0]
            while size > self.max_ring_bytes:
                # Incident history is pinned until durably moved to the outbox. New
                # disposable samples may be evicted first; pinned bytes stay within
                # the same ring budget and are never duplicated in a second spool.
                row = self.db.execute("SELECT id,length(payload) FROM ring "
                                      "WHERE id NOT IN (SELECT ring_id FROM history_pending) "
                                      "ORDER BY id LIMIT 1").fetchone()
                if row is None:
                    raise CapacityError("pinned history exceeds ring budget; restore the previous capacity")
                self.db.execute("DELETE FROM ring WHERE id=?", (row[0],))
                size -= row[1]

    def prune_ring(self, now_ms):
        with self.atomic():
            self.db.execute("DELETE FROM ring WHERE created_ms<? "
                            "AND id NOT IN (SELECT ring_id FROM history_pending)",
                            (now_ms - self.retention_ms,))

    def defer_history(self, start_ms, end_ms):
        """Pin the currently retained snapshot; overlapping requests share rows."""
        with self.atomic():
            self.db.execute("INSERT OR IGNORE INTO history_pending SELECT id FROM ring "
                            "WHERE created_ms BETWEEN ? AND ?", (start_ms, end_ms))
            return self.db.execute("SELECT COUNT(*) FROM ring WHERE created_ms BETWEEN ? AND ?",
                                   (start_ms, end_ms)).fetchone()[0]

    def deferred_history(self):
        with self.lock:
            return self.db.execute("SELECT COUNT(*) FROM history_pending").fetchone()[0]

    def drain_history(self, limit=MAX_RECORDS):
        """Bounded retry. Outbox insertion and unpinning commit together.

        Capacity pressure leaves the remaining rows pinned, including records too
        large for the configured batch limit. Diagnostic reports expose the backlog.
        """
        if not 1 <= limit <= MAX_RECORDS:
            raise ValueError("invalid history drain limit")
        with self.atomic():
            rows = self.db.execute("SELECT r.id,r.payload,r.created_ms FROM ring r "
                                   "JOIN history_pending p ON p.ring_id=r.id ORDER BY r.id LIMIT ?",
                                   (limit,)).fetchall()
            moved = 0
            for row_id, payload, timestamp in rows:
                try:
                    self.enqueue("history", "signal", payload, timestamp)
                except CapacityError:
                    break
                self.db.execute("DELETE FROM history_pending WHERE ring_id=?", (row_id,))
                moved += 1
            return moved

    def ring_count(self):
        with self.lock:
            return self.db.execute("SELECT COUNT(*) FROM ring").fetchone()[0]

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
