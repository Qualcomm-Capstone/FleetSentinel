"""Real sockets/TLS: MQTT control, anonymous signed PUT, and MQTT independence."""
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import queue
import shutil
import ssl
import subprocess
import threading
import time

import pytest

from fleetsentinel_ingest.clip_upload import ClipUploader, HttpFailure
from fleetsentinel_ingest.clip_mqtt import MqttClipTransport
from fleetsentinel_ingest.collection_policy import Collector
from fleetsentinel_ingest.mqtt_runtime import EdgeRuntime
from fleetsentinel_ingest.raw_recorder import RawRecorder
from fleetsentinel_ingest.raw_store import RawStore
from fleetsentinel_ingest.telemetry_store import TelemetryStore


@pytest.fixture
def certificates(tmp_path):
    openssl = shutil.which("openssl")
    if not openssl:
        pytest.skip("openssl required for real TLS test certificates")
    def run(*args):
        subprocess.run([openssl, *map(str, args)], check=True, capture_output=True)
    ca, ca_key = tmp_path / "ca.crt", tmp_path / "ca.key"
    run("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=Clip Test CA",
        "-keyout", ca_key, "-out", ca)
    result = {"ca": ca}
    for name, san, usage in [("server", "DNS:localhost", "serverAuth"),
                             ("vehicle", "URI:spiffe://fleetsentinel/vehicle/v1", "clientAuth")]:
        key, csr, cert, ext = [tmp_path / f"{name}.{suffix}" for suffix in ("key", "csr", "crt", "ext")]
        run("req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={name}", "-keyout", key, "-out", csr)
        ext.write_text(f"subjectAltName={san}\nextendedKeyUsage={usage}\n")
        run("x509", "-req", "-in", csr, "-CA", ca, "-CAkey", ca_key, "-CAcreateserial", "-days", "1",
            "-extfile", ext, "-out", cert)
        result[name], result[name + "_key"] = cert, key
    return result


def test_given_slow_mqtt_control_then_telemetry_continues_and_https_put_has_no_vehicle_cert(tmp_path, certificates):
    certs = certificates
    entered, release, completed, stop = (threading.Event() for _ in range(4))
    seen = {"object_peers": [], "puts": [], "redirect": False}
    requests, responses = queue.Queue(), queue.Queue()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_PUT(self):
            seen["object_peers"].append(self.connection.getpeercert())
            seen["puts"].append(self.rfile.read(int(self.headers["Content-Length"])))
            if seen["redirect"]:
                self.send_response(307)
                self.send_header("Location", "https://localhost/stolen")
            else:
                self.send_response(200)
                self.send_header("ETag", '"test-etag"')
            self.send_header("Content-Length", "0")
            self.end_headers()

    objects = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certs["server"], certs["server_key"])
    context.load_verify_locations(cafile=certs["ca"])
    context.verify_mode = ssl.CERT_OPTIONAL
    objects.socket = context.wrap_socket(objects.socket, server_side=True)
    object_thread = threading.Thread(target=lambda: objects.serve_forever(poll_interval=.05), daemon=True)
    object_thread.start()

    def view(state):
        m = seen["manifest"]
        return dict(id="job-1", state=state, upload_id="mpu-1", size_bytes=m["size_bytes"],
                    sha256=m["sha256"], part_size_bytes=m["part_size_bytes"])

    def server():
        while not stop.is_set():
            try:
                message = requests.get(timeout=.05)
            except queue.Empty:
                continue
            op, body = message['operation'], message['body']
            if op == 'create':
                seen['manifest'] = body
                entered.set()
                if not release.wait(5):
                    return
                data = view('UPLOADING')
            elif op == 'status':
                data = view('COMPLETED' if seen.get('complete_requested') else 'UPLOADING')
            elif op == 'part_urls':
                data = dict(upload_id='mpu-1', expires_at_us=time.time_ns() // 1000 + 60000000,
                            part_urls=[dict(part_number=1, url=f'https://localhost:{objects.server_port}/part?signature=test')])
            elif op == 'complete':
                assert body == dict(upload_id='mpu-1', parts=[dict(part_number=1, etag='"test-etag"')])
                assert hashlib.sha256(seen['puts'][0]).hexdigest() == seen['manifest']['sha256']
                seen['complete_requested'] = True
                data = view('VERIFYING')
            else:
                raise AssertionError(op)
            responses.put(json.dumps(dict(version=1, correlation_id=message['correlation_id'],
                                          operation=op, status=200, data=data)).encode())

    server_thread = threading.Thread(target=server, daemon=True)
    server_thread.start()
    worker = None
    try:
        with TelemetryStore(tmp_path / 'edge.sqlite', 'v1') as store:
            class Raw(RawStore):
                def complete_file(self, file_id, receipt):
                    super().complete_file(file_id, receipt)
                    completed.set()
            class MQTT:
                sent = []
                def publish(self, topic, payload):
                    self.sent.append(topic)
                    if topic.endswith('/clip/request'):
                        requests.put(json.loads(payload))
                def drain(self, callback):
                    while not responses.empty():
                        callback('fleet/v1/v1/clip/response', responses.get_nowait())
            mqtt = MQTT()
            raw = Raw(store, tmp_path / 'managed')
            recorder = RawRecorder(raw)
            recorder.append(100000, '/camera/front', b'jpeg')
            recorder.close()
            raw.request_clip('request', 100000, 100001)
            transport = MqttClipTransport(mqtt, 'v1', object_ca=certs['ca'])
            worker = ClipUploader(store, raw, transport)
            samples = queue.Queue()
            samples.put(json.dumps(dict(scene_id='test', channel='ego_pose', sensor_time=1000000,
                                        log_time=1000000, values_num={}, values_vec={}, values_str={})))
            runtime = EdgeRuntime(Collector(store, clip_uploader=worker), mqtt, samples)
            worker.start()
            assert entered.wait(2)
            runtime.tick(1000, 1000)
            assert 'fleet/v1/v1/up/status' in mqtt.sent
            assert not completed.is_set()
            release.set()
            deadline = time.monotonic() + 5
            while not completed.is_set() and time.monotonic() < deadline:
                runtime.tick(1000, 1000)
                time.sleep(.01)
            assert completed.is_set()
            worker.close()
            worker = None
            assert raw.request_status('request')['uploaded_file_ids']
            assert seen['object_peers'] == [None]
            seen['redirect'] = True
            with pytest.raises(HttpFailure, match='307'):
                transport.put(f'https://localhost:{objects.server_port}/redirect', b'test')
            assert len(seen['puts']) == 2  # redirect was not followed
    finally:
        release.set()
        if worker:
            worker.close()
        stop.set()
        server_thread.join(2)
        objects.shutdown()
        objects.server_close()
        object_thread.join(2)
