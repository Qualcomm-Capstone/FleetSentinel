"""Selected, closed raw files cross HTTPS independently of the MQTT signal loop.

The durable manifest precedes the first request; only a verified server COMPLETED
receipt releases a local pin. One worker uploads one 16 MiB part at a time.
"""
from __future__ import annotations

import http.client
import json
import logging
from pathlib import Path
import random
import socket
import ssl
import threading
import time
from urllib.parse import quote, urlsplit

LOG = logging.getLogger(__name__)
PART_BYTES = 16 * 1024 * 1024
RATE_BYTES = 4 * 1024 * 1024


class HttpFailure(Exception):
    def __init__(self, status):
        self.status = status
        # Do not retain presigned query strings or arbitrary server response bodies.
        super().__init__(f"HTTP {status}")


class ObjectTransport:
    """HTTPS PUT to signed object URLs; never sends vehicle client credentials."""
    def __init__(self, *, object_ca=None, monotonic=time.monotonic, sleep=time.sleep):
        self.object_tls = ssl.create_default_context()
        if object_ca is not None:
            self.object_tls.load_verify_locations(cafile=str(object_ca))
        self.monotonic, self.sleep = monotonic, sleep
        self._active, self._lock = None, threading.Lock()

    @staticmethod
    def validate_url(url):
        parsed = urlsplit(url)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.fragment):
            raise ValueError("upload requires an HTTPS URL without credentials or fragments")
        return parsed

    def _connection(self, url, context, timeout):
        parsed = self.validate_url(url)
        connection = http.client.HTTPSConnection(parsed.hostname, parsed.port or 443,
                                                 timeout=timeout, context=context)
        with self._lock:
            self._active = connection
        target = parsed.path or "/"
        if parsed.query:
            target += "?" + parsed.query
        return connection, target

    def _release(self, connection):
        with self._lock:
            if self._active is connection:
                self._active = None
        connection.close()

    def put(self, url, data):
        connection, target = self._connection(url, self.object_tls, 120)
        try:
            connection.putrequest("PUT", target)
            connection.putheader("Content-Length", str(len(data)))
            connection.putheader("Content-Type", "application/octet-stream")
            connection.endheaders()
            started = self.monotonic()
            for offset in range(0, len(data), 64 * 1024):
                chunk = data[offset:offset + 64 * 1024]
                # Pace before sending so even short parts obey the bandwidth cap.
                delay = started + (offset + len(chunk)) / RATE_BYTES - self.monotonic()
                if delay > 0:
                    self.sleep(delay)
                connection.send(chunk)
            response = connection.getresponse()
            if not 200 <= response.status < 300:
                raise HttpFailure(response.status)
            etag = response.getheader("ETag")
            if not etag:
                raise ValueError("object storage did not return ETag")
            return etag
        finally:
            self._release(connection)

    def close(self):
        with self._lock:
            connection = self._active
            if connection is not None:
                if connection.sock is not None:
                    try:
                        connection.sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                connection.close()


class ClipUploader:
    def __init__(self, store, raw_store, transport, *, clock_us=None, jitter=random.random):
        self.store, self.raw_store, self.transport = store, raw_store, transport
        self.clock_us = clock_us or (lambda: time.time_ns() // 1000)
        self.jitter = jitter
        self._stop, self._thread, self._step_lock = threading.Event(), None, threading.Lock()
        with self.store.atomic():
            self.store.db.execute("""CREATE TABLE IF NOT EXISTS clip_uploads (
                file_id TEXT PRIMARY KEY, manifest TEXT NOT NULL, job TEXT,
                parts TEXT NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL,
                next_retry_us INTEGER NOT NULL, error TEXT)""")

    def _load_or_create(self, file):
        with self.store.atomic():
            row = self.store.db.execute("SELECT manifest,job,parts,state,attempts,next_retry_us,error "
                                        "FROM clip_uploads WHERE file_id=?", (file["id"],)).fetchone()
            if row is None:
                keys = ("t_start_us", "t_end_us", "size_bytes", "sha256", "scene_id",
                        "sensor_channels", "sample_count", "calibration")
                manifest = {key: file[key] for key in keys}
                manifest.update(request_id=file["id"], part_size_bytes=PART_BYTES)
                wire = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
                self.store.db.execute("INSERT INTO clip_uploads VALUES (?,?,NULL,'{}','QUEUED',0,0,NULL)",
                                      (file["id"], wire))
                row = wire, None, "{}", "QUEUED", 0, 0, None
        return dict(manifest=json.loads(row[0]), job=json.loads(row[1]) if row[1] else None,
                    parts=json.loads(row[2]), state=row[3], attempts=row[4], next_retry_us=row[5], error=row[6])

    def _save(self, file_id, state):
        with self.store.atomic():
            self.store.db.execute("UPDATE clip_uploads SET job=?,parts=?,state=?,attempts=?,next_retry_us=?,error=? "
                                  "WHERE file_id=?", (json.dumps(state["job"]) if state["job"] else None,
                                  json.dumps(state["parts"]), state["state"], state["attempts"],
                                  state["next_retry_us"], state["error"], file_id))

    def status_for_file(self, file_id):
        with self.store.lock:
            row = self.store.db.execute("SELECT state,attempts,next_retry_us,error,job FROM clip_uploads "
                                       "WHERE file_id=?", (file_id,)).fetchone()
        if row is None:
            return {"state": "QUEUED"}
        return dict(state=row[0], attempts=row[1], next_retry_us=row[2], error=row[3],
                    job_id=json.loads(row[4])["id"] if row[4] else None)

    def diagnostics(self):
        with self.store.lock:
            return dict(self.store.db.execute("SELECT state,COUNT(*) FROM clip_uploads GROUP BY state"))

    def retry_failed(self):
        """Explicit operator recovery after fixing credentials or local availability.

        Immutable identity, metadata, job and ETags survive. Terminal server jobs
        are polled once and remain failed; this cannot replace a rejected job.
        """
        with self._step_lock, self.store.atomic():
            return self.store.db.execute("UPDATE clip_uploads SET state='RETRYING',attempts=0,"
                                         "next_retry_us=0,error=NULL WHERE state='FAILED'").rowcount

    def _accept_view(self, file_id, local, view, *, reset_attempts=True):
        if view.get("state") not in ("CREATING", "UPLOADING", "VERIFYING", "STORED", "COMPLETED",
                                     "ABORTING", "ABORTED", "FAILED"):
            raise ValueError("unknown server upload state")
        if (view.get("size_bytes") != local["manifest"]["size_bytes"]
                or view.get("sha256") != local["manifest"]["sha256"]
                or view.get("part_size_bytes") != PART_BYTES or not view.get("id")):
            raise ValueError("server receipt does not match durable manifest")
        if local["job"] and view["id"] != local["job"]["id"]:
            raise ValueError("server upload identity changed")
        if view["state"] == "UPLOADING" and (not isinstance(view.get("upload_id"), str) or not view["upload_id"]):
            raise ValueError("uploading job requires a multipart generation")
        if local["job"] and view.get("upload_id") != local["job"].get("upload_id"):
            # Lifecycle expiry may replace an MPU while retaining the immutable
            # job/object identity. ETags only belong to their original generation.
            local["parts"] = {}
        local.update(job=view, state=view["state"], attempts=0 if reset_attempts else local["attempts"],
                     error=view.get("error"), next_retry_us=0)
        # Binding and receipt commit are in the same local database transaction.
        with self.store.atomic():
            self.raw_store.bind_upload(file_id, view["id"])
            if view["state"] == "COMPLETED":
                self.raw_store.complete_file(file_id, view)
            elif view["state"] in ("CREATING", "VERIFYING", "STORED", "ABORTING"):
                local["next_retry_us"] = self.clock_us() + 1000000
            self._save(file_id, local)

    def step(self):
        """Do one due operation. Called only by the upload worker, never MQTT tick."""
        with self._step_lock:
            for file in self.raw_store.pending_files():
                local = self._load_or_create(file)
                if (local["state"] in ("COMPLETED", "FAILED", "ABORTED")
                        or local["next_retry_us"] > self.clock_us()):
                    continue
                self._advance(file, local)
                return True
        return False

    def _advance(self, file, local):
        uploading_part = False
        try:
            job = local["job"]
            if job is None:
                self._accept_view(file["id"], local, self.transport.control("POST", "", local["manifest"]))
                return
            path = "/" + quote(job["id"], safe="")
            # Refresh before every PUT/complete so stale persisted ETags cannot
            # cross a server-side generation restart, including after downtime.
            self._accept_view(file["id"], local, self.transport.control("GET", path), reset_attempts=False)
            job = local["job"]
            if job["state"] != "UPLOADING":
                return
            total_parts = (local["manifest"]["size_bytes"] + PART_BYTES - 1) // PART_BYTES
            missing = next((n for n in range(1, total_parts + 1) if str(n) not in local["parts"]), None)
            if missing is None:
                body = {"upload_id": job["upload_id"],
                        "parts": [{"part_number": n, "etag": local["parts"][str(n)]}
                                   for n in range(1, total_parts + 1)]}
                self._accept_view(file["id"], local, self.transport.control("POST", path + "/complete", body))
                return
            # Request a new URL for each attempt: expired URLs are never persisted.
            urls = self.transport.control("POST", path + "/part-urls", {"part_numbers": [missing]})
            if urls.get("upload_id") != job["upload_id"]:
                raise HttpFailure(409)  # Generation changed between GET and URL issuance.
            if urls["expires_at_us"] <= self.clock_us():
                raise OSError("part URL expired before upload")
            entries = urls["part_urls"]
            if len(entries) != 1 or entries[0]["part_number"] != missing:
                raise ValueError("part URL response does not match requested part")
            try:
                with Path(file["path"]).open("rb") as stream:
                    stream.seek((missing - 1) * PART_BYTES)
                    data = stream.read(PART_BYTES)
            except OSError as exc:
                raise ValueError("selected local file unavailable") from exc
            expected = min(PART_BYTES, local["manifest"]["size_bytes"] - (missing - 1) * PART_BYTES)
            if len(data) != expected:
                raise ValueError("selected local file size changed")
            uploading_part = True
            local["parts"][str(missing)] = self.transport.put(entries[0]["url"], data)
            local.update(state="UPLOADING", attempts=0, next_retry_us=0, error=None)
            self._save(file["id"], local)
        except (OSError, http.client.HTTPException, HttpFailure, ValueError, KeyError, TypeError) as exc:
            retry = (isinstance(exc, (OSError, http.client.HTTPException))
                     or isinstance(exc, HttpFailure) and
                     (exc.status == 429 or exc.status >= 500
                      or exc.status == 409 and local["job"] is not None
                      or uploading_part and exc.status in (401, 403, 404)))
            local["attempts"] += 1
            delay = min(60, 2 ** min(local["attempts"] - 1, 6)) * (1 + self.jitter())
            local.update(state="RETRYING" if retry else "FAILED",
                         next_retry_us=self.clock_us() + int(delay * 1000000) if retry else 0,
                         error=f"{type(exc).__name__}: {exc}")
            self._save(file["id"], local)
            LOG.warning("Raw clip %s: %s", file["id"], local["error"])

    def start(self):
        if self._thread is not None:
            raise RuntimeError("upload worker already started")
        self._thread = threading.Thread(target=self._run, name="clip-upload", daemon=True)
        self._thread.start()

    def _run(self):
        while not self._stop.is_set():
            try:
                progressed = self.step()
            except Exception:
                LOG.exception("Raw upload worker failed; selected files remain pinned")
                progressed = False
            self._stop.wait(0.05 if progressed else 0.5)

    def close(self):
        self._stop.set()
        if hasattr(self.transport, "close"):
            self.transport.close()
        if self._thread:
            self._thread.join(timeout=20)
            if self._thread.is_alive():
                raise RuntimeError("upload worker has not stopped; keep its database open")
