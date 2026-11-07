"""Durable REST transfer behavior with an injected clock and transport."""
import hashlib
import json

import pytest

from fleetsentinel_ingest.clip_upload import ClipUploader, HttpFailure, ObjectTransport
from fleetsentinel_ingest.telemetry_store import TelemetryStore


class Raw:
    def __init__(self, store, path):
        self.store = store
        self.files = [dict(id="stable-file", path=str(path), t_start_us=1, t_end_us=10,
                           size_bytes=path.stat().st_size, sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                           scene_id=None, sensor_channels=["/camera/front"], sample_count=1, calibration={})]
        self.bound, self.receipts = {}, []

    def pending_files(self):
        return self.files if not self.receipts else []

    def bind_upload(self, file_id, job_id):
        self.bound[file_id] = job_id

    def complete_file(self, file_id, receipt):
        assert receipt["id"] == self.bound[file_id]
        assert receipt["state"] == "COMPLETED"
        self.receipts.append(receipt)


class Transport:
    def __init__(self):
        self.calls, self.creates, self.puts = [], [], []
        self.fail = None
        self.final = "VERIFYING"
        self.upload_id = "mpu-1"
        self.complete_requested = False

    def control(self, method, path, body=None):
        self.calls.append((method, path, body))
        if path == "" and method == "POST":
            self.creates.append(body)
            self.metadata = body
            if self.fail:
                raise self.fail
            return self.view("UPLOADING")
        if path.endswith("/part-urls"):
            return {"part_urls": [{"part_number": body["part_numbers"][0], "url": "https://objects/part"}],
                    "expires_at_us": 9999999999, "upload_id": self.upload_id}
        if path.endswith("/complete"):
            self.complete_requested = True
        if method == "GET" and not self.complete_requested:
            return self.view("UPLOADING")
        return self.view(self.final)

    def view(self, state):
        return dict(id="server-job", state=state, upload_id=self.upload_id, size_bytes=self.metadata["size_bytes"],
                    sha256=self.metadata["sha256"], part_size_bytes=self.metadata["part_size_bytes"])

    def put(self, url, data):
        self.puts.append((url, data))
        if self.fail:
            raise self.fail
        return '"etag"'


@pytest.fixture
def setup(tmp_path):
    path = tmp_path / "clip.mcap"
    path.write_bytes(b"raw camera content")
    store = TelemetryStore(tmp_path / "edge.sqlite", "vehicle-1")
    raw, transport, clock = Raw(store, path), Transport(), [1000000]
    yield store, raw, transport, clock
    store.close()


def uploader(setup):
    store, raw, transport, clock = setup
    return ClipUploader(store, raw, transport, clock_us=lambda: clock[0], jitter=lambda: 0)


def test_given_selected_file_when_create_response_lost_then_manifest_and_backoff_survive_restart(setup):
    store, raw, transport, clock = setup
    worker = uploader(setup)
    transport.fail = OSError("connection lost after server created job")
    assert worker.step()
    first = transport.creates[0]
    saved = worker.status_for_file("stable-file")
    assert saved["state"] == "RETRYING"
    assert saved["next_retry_us"] > clock[0]
    assert not uploader(setup).step()
    clock[0] = saved["next_retry_us"]
    transport.fail = None
    assert uploader(setup).step()
    assert transport.creates == [first, first]
    assert first["request_id"] == "stable-file"
    assert first["part_size_bytes"] == 16 * 1024 * 1024
    assert not raw.receipts


def test_given_upload_when_stored_or_verifying_then_keep_pins_until_matching_completed_receipt(setup):
    store, raw, transport, clock = setup
    worker = uploader(setup)
    worker.step()  # create
    worker.step()  # PUT, persistent ETag
    worker = uploader(setup)  # restart must not PUT again
    worker.step()  # complete -> VERIFYING
    assert len(transport.puts) == 1 and not raw.receipts
    transport.final = "STORED"
    clock[0] += 2000000
    worker.step()
    assert not raw.receipts
    transport.final = "COMPLETED"
    clock[0] += 2000000
    worker.step()
    assert raw.receipts[0]["state"] == "COMPLETED"
    assert worker.status_for_file("stable-file")["state"] == "COMPLETED"


@pytest.mark.parametrize("failure,retry", [(HttpFailure(429), True), (HttpFailure(503), True),
                                          (HttpFailure(409), False), (HttpFailure(403), False)])
def test_given_control_error_when_transfer_attempted_then_retry_only_transient_failures(setup, failure, retry):
    _, raw, transport, _ = setup
    transport.fail = failure
    worker = uploader(setup)
    worker.step()
    assert worker.status_for_file("stable-file")["state"] == ("RETRYING" if retry else "FAILED")
    assert not raw.receipts


def test_given_expired_part_url_when_put_forbidden_then_refresh_without_forgetting_manifest(setup):
    _, raw, transport, clock = setup
    worker = uploader(setup)
    worker.step()
    transport.fail = HttpFailure(403)
    worker.step()
    clock[0] = worker.status_for_file("stable-file")["next_retry_us"]
    transport.fail = None
    worker.step()
    assert len([call for call in transport.calls if call[1].endswith("/part-urls")]) == 2
    assert len(transport.creates) == 1
    assert not raw.receipts


def test_given_no_selected_files_when_worker_runs_then_no_network_requests(setup):
    _, raw, transport, _ = setup
    raw.files.clear()
    assert not uploader(setup).step()
    assert not transport.calls


def test_given_fixed_credentials_when_explicitly_retrying_failed_then_preserve_manifest_and_resume(setup):
    _, raw, transport, _ = setup
    worker = uploader(setup)
    transport.fail = HttpFailure(403)
    worker.step()
    original = transport.creates[0]
    transport.fail = None
    assert not worker.step()  # Permanent failures never form an automatic retry loop.
    assert worker.retry_failed() == 1
    assert worker.step()
    assert transport.creates == [original, original]
    assert worker.status_for_file("stable-file")["state"] == "UPLOADING"
    assert not raw.receipts


def test_given_terminal_server_failure_when_explicitly_retrying_then_poll_same_job_once_and_keep_pin(setup):
    _, raw, transport, _ = setup
    worker = uploader(setup)
    worker.step()
    worker.step()
    transport.final = "FAILED"
    worker.step()
    assert worker.status_for_file("stable-file")["state"] == "FAILED"
    assert worker.retry_failed() == 1
    worker.step()
    assert transport.calls[-1][:2] == ("GET", "/server-job")
    assert not worker.step()
    assert len(transport.creates) == len(transport.puts) == 1
    assert not raw.receipts


def test_given_expired_generation_after_partial_put_when_resuming_then_clear_etags_before_reupload(setup, monkeypatch):
    from fleetsentinel_ingest import clip_upload
    monkeypatch.setattr(clip_upload, "PART_BYTES", 4)
    store, raw, transport, _ = setup
    worker = uploader(setup)
    worker.step()
    worker.step()
    assert json.loads(store.db.execute("SELECT parts FROM clip_uploads").fetchone()[0]) == {"1": '"etag"'}
    original_manifest = transport.creates[0].copy()
    transport.upload_id = "mpu-restarted"
    original_put = transport.put
    def put(url, data):
        job, parts = store.db.execute("SELECT job,parts FROM clip_uploads").fetchone()
        assert json.loads(job)["upload_id"] == "mpu-restarted"
        assert json.loads(parts) == {}  # Cleared durably before any bytes for new generation.
        return original_put(url, data)
    transport.put = put
    uploader(setup).step()  # Process restart must still discover generation replacement.
    assert [data for _, data in transport.puts] == [b"raw ", b"raw "]
    assert transport.creates == [original_manifest]
    assert not raw.receipts


def test_given_verifying_job_when_server_restarts_missing_mpu_then_reupload_and_fence_completion(setup):
    _, raw, transport, clock = setup
    worker = uploader(setup)
    worker.step()
    worker.step()
    worker.step()  # VERIFYING with one saved ETag
    transport.upload_id = "mpu-restarted"
    transport.complete_requested = False
    clock[0] += 2000000
    worker.step()
    worker.step()
    # New generation must receive bytes again, and completion must name it.
    assert len(transport.puts) == 2
    completes = [body for _, path, body in transport.calls if path.endswith("/complete")]
    assert [body["upload_id"] for body in completes] == ["mpu-1", "mpu-restarted"]
    assert len(transport.creates) == 1
    assert not raw.receipts


@pytest.mark.parametrize("status", [404, 409])
def test_given_stale_part_generation_when_put_rejected_then_backoff_refresh_and_retry(setup, status):
    _, raw, transport, clock = setup
    worker = uploader(setup)
    worker.step()
    transport.fail = HttpFailure(status)
    worker.step()
    assert worker.status_for_file("stable-file")["state"] == "RETRYING"
    clock[0] = worker.status_for_file("stable-file")["next_retry_us"]
    transport.fail = None
    transport.upload_id = "mpu-restarted"
    worker.step()
    assert len(transport.creates) == 1
    assert not raw.receipts


def test_given_url_generation_changes_after_get_then_do_not_upload_with_stale_generation(setup):
    _, raw, transport, _ = setup
    worker = uploader(setup)
    worker.step()
    original = transport.control
    def control(method, path, body=None):
        response = original(method, path, body)
        if path.endswith("/part-urls"):
            response["upload_id"] = "concurrent-new-generation"
        return response
    transport.control = control
    worker.step()
    assert not transport.puts
    assert worker.status_for_file("stable-file")["state"] == "RETRYING"
    assert not raw.receipts


def test_given_generation_changes_between_get_and_complete_then_refresh_after_409(setup):
    _, raw, transport, clock = setup
    worker = uploader(setup)
    worker.step()
    worker.step()
    original = transport.control
    fenced = []
    def control(method, path, body=None):
        if path.endswith("/complete") and not fenced:
            fenced.append(body["upload_id"])
            transport.upload_id = "mpu-raced"
            raise HttpFailure(409)
        return original(method, path, body)
    transport.control = control
    worker.step()
    assert worker.status_for_file("stable-file")["state"] == "RETRYING"
    clock[0] = worker.status_for_file("stable-file")["next_retry_us"]
    worker.step()
    worker.step()
    completes = [body for _, path, body in transport.calls if path.endswith("/complete")]
    assert fenced == ["mpu-1"]
    assert completes[0]["upload_id"] == "mpu-raced"
    assert len(transport.puts) == 2
    assert not raw.receipts


def test_given_repeated_part_failures_when_get_refresh_succeeds_then_backoff_still_increases(setup):
    _, _, transport, clock = setup
    worker = uploader(setup)
    worker.step()
    transport.fail = HttpFailure(503)
    delays = []
    for _ in range(3):
        worker.step()
        next_retry = worker.status_for_file("stable-file")["next_retry_us"]
        delays.append(next_retry - clock[0])
        clock[0] = next_retry
    assert delays == [1000000, 2000000, 4000000]


def test_given_manifest_when_first_request_executes_then_it_is_already_committed(setup):
    store, _, transport, _ = setup
    original = transport.control
    def control(method, path, body=None):
        assert not store.db.in_transaction
        assert json.loads(store.db.execute("SELECT manifest FROM clip_uploads").fetchone()[0]) == body
        return original(method, path, body)
    transport.control = control
    uploader(setup).step()


@pytest.mark.parametrize("url", ["http://objects/part", "https://user:pass@objects/part", "https://objects/part#x"])
def test_given_untrusted_part_url_when_invalid_then_reject_before_connection(url):
    with pytest.raises(ValueError):
        ObjectTransport.validate_url(url)


def test_given_completed_receipt_with_wrong_checksum_then_keep_raw_pin(setup):
    _, raw, transport, _ = setup
    original = transport.view
    transport.view = lambda state: original("COMPLETED") | {"sha256": "0" * 64}
    worker = uploader(setup)
    worker.step()
    assert worker.status_for_file("stable-file")["state"] == "FAILED"
    assert not raw.receipts


def test_given_tls_transport_when_put_then_no_vehicle_credentials_and_paced_bytes(monkeypatch):
    import fleetsentinel_ingest.clip_upload as module
    contexts, connections, elapsed = [], [], [0.0]

    class Context:
        def __init__(self):
            self.cert = None
        def load_cert_chain(self, cert, key):
            self.cert = (cert, key)

    def context(**kwargs):
        value = Context()
        contexts.append(value)
        return value

    class Response:
        status = 200
        def read(self, limit):
            return b'{"state":"UPLOADING"}'
        def getheader(self, name):
            return '"ETag"'

    class Connection:
        def __init__(self, host, port, *, timeout, context):
            self.host, self.timeout, self.context = host, timeout, context
            self.sent, self.headers, self.closed = [], {}, False
            connections.append(self)
        def request(self, method, path, **kwargs):
            self.requested = (method, path)
        def putrequest(self, method, path):
            self.requested = (method, path)
        def putheader(self, name, value):
            self.headers[name] = value
        def endheaders(self):
            pass
        def send(self, chunk):
            self.sent.append(chunk)
        def getresponse(self):
            return Response()
        def close(self):
            self.closed = True

    monkeypatch.setattr(module.ssl, "create_default_context", context)
    monkeypatch.setattr(module.http.client, "HTTPSConnection", Connection)
    def sleep(seconds):
        elapsed[0] += seconds
    transport = ObjectTransport(monotonic=lambda: elapsed[0], sleep=sleep)
    data = b"r" * 100000
    assert transport.put("https://objects/part?signature=secret", data) == '"ETag"'
    assert contexts[0].cert is None
    assert [(c.host, c.timeout) for c in connections] == [("objects", 120)]
    assert b"".join(connections[0].sent) == data
    assert max(map(len, connections[0].sent)) <= 64 * 1024
    assert elapsed[0] == pytest.approx(len(data) / (4 * 1024 * 1024))
    assert all(c.closed for c in connections)
    Response.status = 307
    with pytest.raises(HttpFailure) as failure:
        transport.put("https://objects/part", b"x")
    assert failure.value.status == 307
    assert len(connections) == 2  # No redirect request.


def test_given_real_managed_spool_when_operator_selects_then_upload_and_completed_receipt_unpin(tmp_path):
    from fleetsentinel_ingest.collection_policy import Collector
    from fleetsentinel_ingest.raw_recorder import RawRecorder
    from fleetsentinel_ingest.raw_store import RawStore
    with TelemetryStore(tmp_path / "edge.sqlite", "v1") as store:
        raw = RawStore(store, tmp_path / "managed", clock_us=lambda: 1000000)
        recorder = RawRecorder(raw)
        recorder.append(100000, "/camera/front", b"jpeg")
        recorder.append(900000, "/camera/front", b"jpeg2")
        recorder.close()
        transport = Transport()
        worker = ClipUploader(store, raw, transport, clock_us=lambda: 1000000)
        assert not worker.step()
        collector = Collector(store, clock=lambda: 1000, raw_store=raw, clip_uploader=worker)
        result = collector.command(dict(command_id="request-1", vehicle_id="v1", type="clip_request",
                                        start_us=100000, end_us=900001, expires_at_ms=5000))
        assert result["status"] == "applied"
        file_id = result["clip"]["file_ids"][0]
        worker.step()
        worker.step()
        assert store.db.execute("SELECT pinned FROM raw_selections").fetchone()[0] == 1
        transport.final = "COMPLETED"
        worker.step()
        assert store.db.execute("SELECT pinned FROM raw_selections").fetchone()[0] == 0
        assert raw.request_status("request-1")["uploaded_file_ids"] == [file_id]
        assert not raw.pending_files()
