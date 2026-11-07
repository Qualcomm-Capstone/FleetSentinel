"""Durable clip intent, shared pins and a bounded, explicitly managed raw spool.

Request windows and coverage use half-open microsecond intervals. Retention uses
the local clock, independently of the sensor clock. Only files registered in this
manifest can be removed; existing datasets cannot be adopted as a spool.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from pathlib import Path

from .telemetry_store import CapacityError


def now_us():
    return time.time_ns() // 1000


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class RawStore:
    def __init__(self, telemetry_store, root, *, retention_us=3_600_000_000,
                 max_bytes=128 * 1024 ** 3, clock_us=now_us):
        if retention_us <= 0 or max_bytes <= 0:
            raise ValueError("invalid raw storage bounds")
        self.telemetry = telemetry_store
        self.db = telemetry_store.db
        self.atomic = telemetry_store.atomic
        self.clock_us, self.retention_us, self.max_bytes = clock_us, retention_us, max_bytes
        root = Path(root).absolute()
        if root.is_symlink():
            raise ValueError("managed spool root cannot be a symlink")
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        marker = self.root / ".fleetsentinel-raw-spool"
        with self.atomic():
            identity = self.telemetry.get_meta("raw_store_identity") or uuid.uuid4().hex
            self.telemetry.set_meta("raw_store_identity", identity)
        if marker.exists():
            if marker.is_symlink() or marker.read_text() != identity:
                raise ValueError("spool marker does not belong to this database")
        else:
            if any(self.root.iterdir()):
                raise ValueError("refusing to adopt a nonempty, unmarked dataset directory")
            with marker.open("x") as stream:
                stream.write(identity)
                stream.flush()
                os.fsync(stream.fileno())
            sync_directory(self.root)
        # The shared connection is already WAL/FULL. Initialization is outside
        # command transactions; all operational changes use its nested atomic().
        with self.telemetry.lock:
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS raw_files (
                    id TEXT PRIMARY KEY, root TEXT NOT NULL, state TEXT NOT NULL,
                    t_start_us INTEGER NOT NULL, t_end_us INTEGER NOT NULL,
                    size_bytes INTEGER NOT NULL DEFAULT 0, sha256 TEXT,
                    created_us INTEGER NOT NULL, metadata TEXT NOT NULL,
                    job_id TEXT, receipt TEXT);
                CREATE TABLE IF NOT EXISTS raw_requests (
                    id TEXT PRIMARY KEY, root TEXT NOT NULL, start_us INTEGER NOT NULL,
                    end_us INTEGER NOT NULL, kind TEXT NOT NULL, accepted_us INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS raw_selections (
                    request_id TEXT NOT NULL, file_id TEXT NOT NULL,
                    pinned INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY(request_id, file_id));
                CREATE INDEX IF NOT EXISTS raw_files_time ON raw_files(root,t_start_us,t_end_us);
                CREATE INDEX IF NOT EXISTS raw_selections_file ON raw_selections(file_id,pinned);
            """)
        self.recover()

    def _path(self, file_id, suffix=".mcap"):
        # IDs are generated internally, but defend deletion against corrupt rows.
        if len(file_id) != 32 or any(c not in "0123456789abcdef" for c in file_id):
            raise ValueError("invalid managed file id")
        result = self.root / (file_id + suffix)
        if result.is_symlink():
            raise ValueError("managed file cannot be a symlink")
        return result

    def _select(self, file_id):
        self.db.execute("""INSERT OR IGNORE INTO raw_selections(request_id,file_id,pinned)
            SELECT r.id,f.id,CASE WHEN f.receipt IS NULL THEN 1 ELSE 0 END
            FROM raw_requests r JOIN raw_files f ON r.root=f.root
            WHERE f.id=? AND f.state IN ('OPEN','CLOSED')
            AND f.t_start_us<r.end_us AND f.t_end_us>r.start_us""", (file_id,))

    def request_clip(self, request_id, start_us, end_us, *, now_us=None, kind="operator"):
        if not isinstance(request_id, str) or not request_id or len(request_id) > 200:
            raise ValueError("invalid clip request id")
        if (type(start_us) is not int or type(end_us) is not int or start_us < 0
                or end_us <= start_us or end_us - start_us > 60_000_000):
            raise ValueError("clip window must be positive and at most 60 seconds")
        with self.atomic():
            old = self.db.execute("SELECT root,start_us,end_us,kind FROM raw_requests WHERE id=?",
                                  (request_id,)).fetchone()
            fingerprint = (str(self.root), start_us, end_us, kind)
            if old and old != fingerprint:
                raise ValueError("request id reused with different clip window")
            self.db.execute("INSERT OR IGNORE INTO raw_requests VALUES (?,?,?,?,?,?)",
                            (request_id, *fingerprint, self.clock_us() if now_us is None else now_us))
            self.db.execute("""INSERT OR IGNORE INTO raw_selections(request_id,file_id,pinned)
                SELECT ?,id,CASE WHEN receipt IS NULL THEN 1 ELSE 0 END FROM raw_files
                WHERE root=? AND state IN ('OPEN','CLOSED') AND t_start_us<? AND t_end_us>?""",
                            (request_id, str(self.root), end_us, start_us))
            return self.request_status(request_id)

    def anomaly(self, episode_id, event_us, *, now_us=None):
        return self.request_clip(episode_id, max(0, event_us - 10_000_000),
                                 event_us + 20_000_000, now_us=now_us, kind="anomaly")

    def request_status(self, request_id):
        with self.telemetry.lock:
            request = self.db.execute("SELECT start_us,end_us,kind FROM raw_requests WHERE id=? AND root=?",
                                      (request_id, str(self.root))).fetchone()
            if not request:
                return None
            start, end, kind = request
            files = self.db.execute("""SELECT f.id,f.state,f.t_start_us,f.t_end_us,f.receipt
                FROM raw_files f JOIN raw_selections s ON s.file_id=f.id
                WHERE s.request_id=? ORDER BY f.t_start_us,f.id""", (request_id,)).fetchall()
            intervals = sorted((max(start, lo), min(end, hi)) for _, state, lo, hi, receipt in files
                               if state == "CLOSED" or receipt is not None)
            coverage = []
            for lo, hi in intervals:
                if coverage and lo <= coverage[-1][1]:
                    coverage[-1][1] = max(coverage[-1][1], hi)
                else:
                    coverage.append([lo, hi])
            missing, cursor = [], start
            for lo, hi in coverage:
                if cursor < lo:
                    missing.append([cursor, lo])
                cursor = max(cursor, hi)
            if cursor < end:
                missing.append([cursor, end])
            return {"request_id": request_id, "kind": kind, "requested": [start, end],
                    "coverage": coverage, "missing": missing, "partial": bool(missing),
                    "file_ids": [f[0] for f in files],
                    "uploaded_file_ids": [f[0] for f in files if f[4] is not None]}

    def begin_file(self, timestamp_us, metadata):
        file_id = uuid.uuid4().hex
        with self.atomic():
            self.db.execute("""INSERT INTO raw_files
                (id,root,state,t_start_us,t_end_us,created_us,metadata)
                VALUES (?,?,'OPEN',?,?,?,?)""",
                (file_id, str(self.root), timestamp_us, timestamp_us + 1,
                 self.clock_us(), json.dumps(metadata, sort_keys=True)))
            self._select(file_id)
        return file_id, self._path(file_id, ".partial")

    def update_open(self, file_id, end_us, size_bytes, metadata):
        with self.atomic():
            self.db.execute("UPDATE raw_files SET t_end_us=?,size_bytes=?,metadata=? WHERE id=? AND state='OPEN'",
                            (end_us, size_bytes, json.dumps(metadata, sort_keys=True), file_id))
            self._select(file_id)

    def seal_file(self, file_id, start_us, end_us, metadata):
        partial, final = self._path(file_id, ".partial"), self._path(file_id)
        # No database transaction spans hashing, fsync or rename.
        if partial.exists():
            os.replace(partial, final)
            sync_directory(self.root)
        size, digest = final.stat().st_size, file_sha256(final)
        with self.atomic():
            self.db.execute("""UPDATE raw_files SET state='CLOSED',t_start_us=?,t_end_us=?,
                size_bytes=?,sha256=?,metadata=? WHERE id=?""",
                (start_us, end_us, size, digest, json.dumps(metadata, sort_keys=True), file_id))
            self._select(file_id)

    def pending_files(self):
        with self.telemetry.lock:
            rows = self.db.execute("""SELECT id,t_start_us,t_end_us,size_bytes,sha256,metadata
                FROM raw_files WHERE root=? AND state='CLOSED' AND receipt IS NULL
                AND EXISTS(SELECT 1 FROM raw_selections s WHERE s.file_id=id AND s.pinned=1)
                ORDER BY t_start_us,id""", (str(self.root),)).fetchall()
            return [dict(json.loads(meta), id=fid, path=self._path(fid), t_start_us=lo,
                         t_end_us=hi, size_bytes=size, sha256=digest)
                    for fid, lo, hi, size, digest, meta in rows]

    def bind_upload(self, file_id, job_id):
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("invalid server job id")
        with self.atomic():
            row = self.db.execute("SELECT job_id FROM raw_files WHERE id=? AND root=?",
                                  (file_id, str(self.root))).fetchone()
            if not row or row[0] not in (None, job_id):
                raise ValueError("server job differs from persisted file upload")
            self.db.execute("UPDATE raw_files SET job_id=? WHERE id=?", (job_id, file_id))

    def complete_file(self, file_id, receipt):
        with self.atomic():
            row = self.db.execute("SELECT job_id FROM raw_files WHERE id=? AND root=?",
                                  (file_id, str(self.root))).fetchone()
            if (not row or not row[0] or receipt.get("id") != row[0]
                    or receipt.get("state") != "COMPLETED"):
                raise ValueError("matching, durable server COMPLETED receipt required")
            self.db.execute("UPDATE raw_files SET receipt=? WHERE id=?", (json.dumps(receipt, sort_keys=True), file_id))
            self.db.execute("UPDATE raw_selections SET pinned=0 WHERE file_id=?", (file_id,))

    def used_bytes(self):
        with self.telemetry.lock:
            return self.db.execute("SELECT COALESCE(SUM(size_bytes),0) FROM raw_files WHERE root=? AND state!='DELETED'",
                                   (str(self.root),)).fetchone()[0]

    def diagnostics(self):
        with self.telemetry.lock:
            counts = dict(self.db.execute("SELECT state,COUNT(*) FROM raw_files WHERE root=? GROUP BY state",
                                          (str(self.root),)).fetchall())
            pinned = self.db.execute("""SELECT COUNT(*),COALESCE(SUM(size_bytes),0) FROM raw_files f
                WHERE root=? AND EXISTS(SELECT 1 FROM raw_selections s WHERE s.file_id=f.id AND s.pinned=1)""",
                (str(self.root),)).fetchone()
            requests = self.db.execute("SELECT COUNT(*) FROM raw_requests WHERE root=?", (str(self.root),)).fetchone()[0]
            return {"used_bytes": self.used_bytes(), "max_bytes": self.max_bytes,
                    "file_states": counts, "pinned_files": pinned[0], "pinned_bytes": pinned[1],
                    "requests": requests,
                    "capacity_blocked": self.telemetry.get_meta("raw_capacity_blocked:" + str(self.root)) == "1"}

    def _delete_one(self, now, need_bytes):
        with self.atomic():
            # The intent commit wins against a later request; an earlier accepted
            # request's pin always wins against deletion, including after restart.
            row = self.db.execute("""SELECT id FROM raw_files WHERE root=? AND state IN ('CLOSED','LOST')
                AND NOT EXISTS(SELECT 1 FROM raw_selections s WHERE s.file_id=id AND s.pinned=1)
                AND (created_us<? OR ?=1) ORDER BY created_us,id LIMIT 1""",
                (str(self.root), now - self.retention_us, int(need_bytes))).fetchone()
            if not row:
                return False
            self.db.execute("UPDATE raw_files SET state='DELETING' WHERE id=?", row)
        self._finish_delete(row[0])
        return True

    def _finish_delete(self, file_id):
        self._path(file_id).unlink(missing_ok=True)
        self._path(file_id, ".partial").unlink(missing_ok=True)
        sync_directory(self.root)
        with self.atomic():
            self.db.execute("UPDATE raw_files SET state='DELETED',size_bytes=0 WHERE id=?", (file_id,))

    def prune(self, *, now_us=None):
        with self.telemetry.lock:
            if self.db.in_transaction:
                raise RuntimeError("file pruning cannot run inside a command transaction")
        now = self.clock_us() if now_us is None else now_us
        while self._delete_one(now, self.used_bytes() > self.max_bytes):
            pass

    def ensure_space(self, additional_bytes, *, now_us=None):
        with self.telemetry.lock:
            if self.db.in_transaction:
                raise RuntimeError("capacity pruning cannot run inside a command transaction")
        now = self.clock_us() if now_us is None else now_us
        self.prune(now_us=now)
        while self.used_bytes() + additional_bytes > self.max_bytes:
            if not self._delete_one(now, True):
                with self.atomic():
                    self.telemetry.set_meta("raw_capacity_blocked:" + str(self.root), "1")
                raise CapacityError("raw capacity_blocked: selected unconfirmed files remain pinned")
        with self.atomic():
            self.telemetry.set_meta("raw_capacity_blocked:" + str(self.root), "0")

    def recover(self):
        from .raw_recorder import recover_file
        with self.telemetry.lock:
            rows = self.db.execute("SELECT id,state,metadata FROM raw_files WHERE root=? AND state IN ('OPEN','DELETING','CLOSED')",
                                   (str(self.root),)).fetchall()
        for file_id, state, metadata in rows:
            if state == "DELETING":
                self._finish_delete(file_id)
            elif state == "OPEN":
                recover_file(self, file_id, json.loads(metadata))
            elif not self._path(file_id).is_file():
                with self.atomic():
                    self.db.execute("UPDATE raw_files SET state='LOST',size_bytes=0 WHERE id=?", (file_id,))
