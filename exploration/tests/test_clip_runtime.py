"""CLI/runtime wiring preserves late clock alignment and baseline telemetry."""
from pathlib import Path

from fleetsentinel_ingest.mqtt_runtime import RawReplayRuntime
from fleetsentinel_ingest.raw_store import RawStore
from fleetsentinel_ingest.telemetry_store import TelemetryStore


def test_given_source_when_runtime_replays_then_delegate_lazy_clock_alignment_without_pre_read(tmp_path, monkeypatch):
    from fleetsentinel_ingest import raw_recorder
    with TelemetryStore(tmp_path / "edge.sqlite", "v1") as store:
        raw = RawStore(store, tmp_path / "managed")
        # No source file is needed: runtime must not open it before the replay
        # adapter finishes metadata discovery and maps the first timestamp.
        source = tmp_path / "source.mcap"
        runtime = RawReplayRuntime(store, raw, [source])
        calls = []
        def replay(path, recorder, *, clock, shift_us):
            calls.append((path, shift_us))
            runtime._stop.set()
        monkeypatch.setattr(raw_recorder, "replay_mcap", replay)
        runtime._run()
        assert calls == [(source, None)]
        assert store.get_meta("raw_replay_state") == "IDLE"


def test_given_cli_raw_options_when_started_then_use_mqtt_control_and_legacy_gate(tmp_path, monkeypatch):
    import importlib.util
    import io
    import sys
    from fleetsentinel_ingest import clip_upload, clip_mqtt, legacy_upload_guard

    path = Path(__file__).resolve().parents[1] / "scripts" / "mqtt_ingest.py"
    spec = importlib.util.spec_from_file_location("clip_cli_wiring", path)
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    events = []

    class Connection:
        def __init__(self, *args):
            pass
        def start(self):
            events.append("mqtt-start")
        def close(self):
            events.append("mqtt-close")
        def drain(self, callback):
            raise KeyboardInterrupt()
        def publish(self, *args):
            pass
    class Worker:
        def __init__(self, *args):
            pass
        def start(self):
            events.append("worker-start")
        def close(self):
            events.append("worker-close")
        def retry_failed(self):
            events.append("explicit-retry")
            return 1
    class Thread:
        def __init__(self, target, **kwargs):
            self.target = target
        def start(self):
            self.target()
    def transport(connection, vehicle, *, object_ca):
        assert isinstance(connection, Connection)
        assert vehicle == "v1" and str(object_ca) == "object-ca"
        events.append("mqtt-control")
    def guard(path):
        assert path == tmp_path / "legacy"
        events.append("legacy-drained")
    monkeypatch.setattr(cli, "Connection", Connection)
    monkeypatch.setattr(cli, "RawReplayRuntime", Worker)
    monkeypatch.setattr(clip_upload, "ClipUploader", Worker)
    monkeypatch.setattr(clip_mqtt, "MqttClipTransport", transport)
    monkeypatch.setattr(legacy_upload_guard, "require_drained_legacy_uploads", guard)
    monkeypatch.setattr(cli.threading, "Thread", Thread)
    monkeypatch.setattr(cli.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(sys, "stdin", io.StringIO(""))
    monkeypatch.setattr(sys, "argv", ["mqtt_ingest.py", "edge", "--client-id", "v1-edge", "--vehicle", "v1",
                                     "--db", str(tmp_path / "edge.sqlite"), "--ca", "mqtt-ca", "--cert", "mqtt-cert",
                                     "--key", "mqtt-key", "--raw-spool", str(tmp_path / "managed"),
                                     "--object-ca", "object-ca",
                                     "--legacy-wal", str(tmp_path / "legacy"), "--retry-failed-clips"])
    cli.main()
    assert events == ["legacy-drained", "mqtt-control", "explicit-retry", "mqtt-start", "worker-start", "worker-start",
                      "worker-close", "worker-close", "mqtt-close"]
