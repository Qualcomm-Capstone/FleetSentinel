#!/usr/bin/env python3
"""Opt-in MQTT path: edge JSONL signal adapter, ingestion bridge, operator dispatcher.

Run from exploration with PYTHONPATH=. .venv/bin/python scripts/mqtt_ingest.py --help.
Input is vehicle-signal.avsc JSON (timestamps in microseconds), optionally wrapped as
{"signal": <row>, "anomaly": true}. No raw image or lidar payload belongs here.
"""
import argparse
import json
import logging
from pathlib import Path
import queue
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fleetsentinel_ingest.collection_policy import Collector, Policy
from fleetsentinel_ingest.command_service import CommandStore
from fleetsentinel_ingest.mqtt_bridge import Bridge
from fleetsentinel_ingest.mqtt_contract import canonical, identity
from fleetsentinel_ingest.mqtt_runtime import Connection, EdgeRuntime, KafkaWriter, RawReplayRuntime
from fleetsentinel_ingest.telemetry_store import TelemetryStore


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role", choices=["edge", "bridge", "operator"])
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8883)
    for flag in ("ca", "cert", "key"):
        parser.add_argument(f"--{flag}", type=Path, required=True)
    parser.add_argument("--client-id", required=True, help="unique stable MQTT session id")
    parser.add_argument("--vehicle")
    parser.add_argument("--db", type=Path, help="required for edge/operator; never automatically cleared")
    parser.add_argument("--policy", type=Path, help="initial edge policy JSON; persisted commands take precedence")
    parser.add_argument("--command", type=Path, help="operator command JSON; omit to resume pending commands")
    parser.add_argument("--bootstrap", default="localhost:29092")
    parser.add_argument("--retention-ms", type=int, default=3600000)
    parser.add_argument("--max-pending-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--max-db-bytes", type=int, default=2 * 1024 ** 3,
                        help="edge SQLite main-file limit including metadata; reserve extra disk for WAL/raw files")
    parser.add_argument("--raw-spool", type=Path, help="managed raw directory, separate from source datasets")
    parser.add_argument("--raw-mcap", type=Path, action="append", default=[],
                        help="read-only source MCAP to replay at wall-clock speed; repeat for multiple files")
    parser.add_argument("--object-ca", type=Path, help="additional object-storage CA; system roots trusted by default")
    parser.add_argument("--raw-max-bytes", type=int, default=128 * 1024 ** 3)
    parser.add_argument("--legacy-wal", type=Path, default=Path("./wal-segments"),
                        help="legacy upload state must be drained before MQTT raw collection starts")
    parser.add_argument("--retry-failed-clips", action="store_true",
                        help="explicitly retry local failed uploads after an operator fix; keep job IDs and parts")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    identity(args.client_id)
    if args.role in ("edge", "operator") and args.db is None:
        parser.error("--db is required")
    if args.raw_spool or args.raw_mcap or args.retry_failed_clips:
        if args.role != "edge" or not args.raw_spool:
            parser.error("raw clips require edge --raw-spool")
        if any(path.suffix.lower() != ".mcap" for path in args.raw_mcap):
            parser.error("--raw-mcap accepts only .mcap source files")
    if args.role == "edge":
        if not args.vehicle:
            parser.error("edge requires --vehicle")
        identity(args.vehicle)
        subscriptions = [f"fleet/v1/{args.vehicle}/ack/+", f"fleet/v1/{args.vehicle}/command",
                         f"fleet/v1/{args.vehicle}/clip/response"]
    elif args.role == "bridge":
        subscriptions = ["$share/ingestion/fleet/v1/+/up/+"]
    else:
        subscriptions = ["fleet/v1/+/reported"]
    connection = Connection(args.client_id, args.host, args.port, args.ca, args.cert, args.key, subscriptions)
    resource = None
    raw_runtime = clip_uploader = None
    try:
        if args.role == "edge":
            resource = TelemetryStore(args.db, args.vehicle, retention_ms=args.retention_ms,
                                      max_pending_bytes=args.max_pending_bytes, max_db_bytes=args.max_db_bytes)
            policy = Policy(**json.loads(args.policy.read_text())) if args.policy else Policy()
            raw_store = None
            if args.raw_spool:
                from fleetsentinel_ingest.raw_store import RawStore
                from fleetsentinel_ingest.clip_upload import ClipUploader
                from fleetsentinel_ingest.clip_mqtt import MqttClipTransport
                from fleetsentinel_ingest.legacy_upload_guard import require_drained_legacy_uploads
                require_drained_legacy_uploads(args.legacy_wal)
                raw_store = RawStore(resource, args.raw_spool, max_bytes=args.raw_max_bytes)
                transport = MqttClipTransport(connection, args.vehicle, object_ca=args.object_ca)
                clip_uploader = ClipUploader(resource, raw_store, transport)
                if args.retry_failed_clips:
                    logging.info("Requeued %d failed raw uploads", clip_uploader.retry_failed())
                raw_runtime = RawReplayRuntime(resource, raw_store, args.raw_mcap)
            collector = Collector(resource, policy, raw_store=raw_store, clip_uploader=clip_uploader)
            samples = queue.Queue(maxsize=128)
            def read_input():
                for line in sys.stdin:
                    samples.put(line)
            threading.Thread(target=read_input, daemon=True).start()
            edge_runtime = EdgeRuntime(collector, connection, samples)
        elif args.role == "bridge":
            resource = KafkaWriter(args.bootstrap)
            bridge = Bridge(resource, connection.publish)
            receive = bridge.receive
        else:
            resource = CommandStore(args.db)
            if args.command:
                resource.submit(json.loads(args.command.read_text()), int(time.time() * 1000))
            def receive(topic, payload):
                result = json.loads(payload)
                if resource.report(topic, result):
                    print(json.dumps(result, sort_keys=True), flush=True)
        connection.start()
        if clip_uploader:
            clip_uploader.start()
            raw_runtime.start()
        next_command = 0
        # Random initial phase avoids fleet-wide synchronized flushes.
        import random
        time.sleep(random.uniform(0, 1))
        while True:
            now = int(time.time() * 1000)
            try:
                if args.role == "edge":
                    edge_runtime.tick(now, int(time.monotonic() * 1000))
                else:
                    connection.drain(receive)
                    if args.role == "bridge":
                        resource.poll()
                    elif now >= next_command:
                        for command in resource.pending(now):
                            connection.publish(f"fleet/v1/{command['vehicle_id']}/command", canonical(command))
                        next_command = now + 2000
            except ConnectionError:
                pass  # Durable sender retries independently of the MQTT session.
            time.sleep(0.1)
    except KeyboardInterrupt:
        pass
    finally:
        if raw_runtime:
            raw_runtime.close()
        if clip_uploader:
            clip_uploader.close()
        connection.close()
        if resource:
            resource.close()


if __name__ == "__main__":
    main()
