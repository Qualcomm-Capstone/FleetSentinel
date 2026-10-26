"""MQTT -> Kafka bridge. At-least-once delivery; downstream keys are stable on retry."""
import threading
import uuid

from .mqtt_contract import Batch, ack_for, canonical


class Bridge:
    def __init__(self, kafka, publish, *, topic="telemetry.records", max_inflight=256):
        self.kafka, self.publish, self.topic = kafka, publish, topic
        self.max_inflight = max_inflight
        self.lock = threading.RLock()
        self.active = {}

    def receive(self, topic, payload):
        batch = Batch.parse(payload, topic)
        with self.lock:
            if batch.batch_id in self.active:
                return False
            if len(self.active) >= self.max_inflight:
                return False  # Edge retries without application ACK.
            state = dict(remaining=len(batch.records), failed=False, completed=set())
            self.active[batch.batch_id] = state

        def complete(index, error):
            ready = False
            with self.lock:
                if index in state["completed"]:
                    return
                state["completed"].add(index)
                state["failed"] |= error is not None
                state["remaining"] -= 1
                if state["remaining"] == 0:
                    self.active.pop(batch.batch_id, None)
                    ready = not state["failed"]
            if ready:
                self.publish(f"fleet/v1/{batch.vehicle_id}/ack/{batch.stream}", canonical(ack_for(batch)))

        # Separate boot namespaces retain the existing ClickHouse vehicle/boot/seq key.
        boot = str(uuid.uuid5(uuid.NAMESPACE_URL, f"fleet:v1:{batch.vehicle_id}:{batch.boot_id}:{batch.stream}"))
        for index, record in enumerate(batch.records):
            headers = [("vehicle_id", batch.vehicle_id.encode()), ("boot_id", boot.encode()),
                       ("source_boot_id", batch.boot_id.encode()), ("stream_id", batch.stream.encode()),
                       ("seq", str(record.seq).encode()), ("kind", f"RECORD_KIND_{record.kind.upper()}".encode()),
                       ("report_interval_ms", str(record.report_interval_ms).encode())]
            try:
                self.kafka.send(self.topic, batch.vehicle_id.encode(), record.payload, headers,
                                lambda error, i=index: complete(i, error))
            except Exception as exc:
                complete(index, exc)
        return True
