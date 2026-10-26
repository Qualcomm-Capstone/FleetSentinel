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
from fleetsentinel_ingest.mqtt_runtime import Connection, EdgeRuntime, KafkaWriter
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
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    identity(args.client_id)
    if args.role in ("edge", "operator") and args.db is None:
        parser.error("--db is required")
    if args.role == "edge":
        if not args.vehicle:
            parser.error("edge requires --vehicle")
        identity(args.vehicle)
        subscriptions = [f"fleet/v1/{args.vehicle}/ack/+", f"fleet/v1/{args.vehicle}/command"]
    elif args.role == "bridge":
        subscriptions = ["$share/ingestion/fleet/v1/+/up/+"]
    else:
        subscriptions = ["fleet/v1/+/reported"]
    connection = Connection(args.client_id, args.host, args.port, args.ca, args.cert, args.key, subscriptions)
    resource = None
    try:
        if args.role == "edge":
            resource = TelemetryStore(args.db, args.vehicle, retention_ms=args.retention_ms,
                                      max_pending_bytes=args.max_pending_bytes)
            policy = Policy(**json.loads(args.policy.read_text())) if args.policy else Policy()
            collector = Collector(resource, policy)
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
            receive = lambda topic, payload: resource.report(topic, json.loads(payload))
        connection.start()
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
        connection.close()
        if resource:
            resource.close()


if __name__ == "__main__":
    main()
