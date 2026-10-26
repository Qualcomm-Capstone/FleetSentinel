"""Bounded MQTT 5 runtime. Network callbacks never run SQLite or collection logic."""
import logging
import queue
import random
import ssl
import json

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from .mqtt_contract import STREAMS, MAX_BYTES, canonical
from .telemetry_store import CapacityError

LOG = logging.getLogger(__name__)


class EdgePump:
    def __init__(self, store, publish, retry_ms=5000):
        self.store, self.publish, self.retry_ms = store, publish, retry_ms
        self.next_send = {}

    def tick(self, now_ms):
        # One batch per stream per tick: priority without starving history.
        for stream in STREAMS:
            if now_ms < self.next_send.get(stream, 0):
                continue
            batch = self.store.batch(stream)
            if batch:
                self.publish(batch.topic(), batch.wire())
                self.next_send[stream] = now_ms + self.retry_ms

    def receive(self, topic, payload):
        prefix = f"fleet/v1/{self.store.vehicle_id}/ack/"
        if not topic.startswith(prefix):
            return False
        ack = json.loads(payload)
        if not isinstance(ack, dict) or ack.get("stream") != topic[len(prefix):]:
            return False
        if self.store.acknowledge(ack):
            self.next_send.pop(ack["stream"], None)
            return True
        return False


class EdgeRuntime:
    """Keep transport/commands alive while bounded sensor admission is paused.

    The blocked input stays in memory and the bounded input queue backpressures
    stdin. Only successful SQLite admission is durable; upstream adapters must
    retain unaccepted input if they need recovery across process termination.
    """
    def __init__(self, collector, connection, samples):
        self.collector, self.connection, self.samples = collector, connection, samples
        self.store = collector.store
        self.pump = EdgePump(self.store, connection.publish)
        self.pending = None
        self.blocked = False

    def tick(self, now_ms, monotonic_ms):
        try:
            self.connection.drain(lambda t, p: edge_receive(self.collector, self.pump, self.connection, t, p))
        except ConnectionError:
            pass  # Report publishing may fail; the command receipt is durable.
        self._send(monotonic_ms)
        for _ in range(128):
            if self.pending is None:
                try:
                    item = json.loads(self.samples.get_nowait())
                except queue.Empty:
                    break
                self.pending = (item, now_ms)
            item, received_ms = self.pending
            try:
                self.collector.observe(item.get("signal", item), now_ms=received_ms,
                                       anomaly=item.get("anomaly", False))
            except CapacityError as exc:
                if not self.blocked:
                    LOG.warning("Sensor admission paused: %s; uploads and ACK processing continue", exc)
                self.blocked = True
                break
            if self.blocked:
                LOG.info("Sensor admission resumed")
            self.blocked = False
            self.pending = None
        self.store.prune_ring(now_ms)
        if not self.blocked:
            self.store.drain_history()
        self._send(monotonic_ms)

    def _send(self, now_ms):
        try:
            self.pump.tick(now_ms)
        except ConnectionError:
            pass  # Disconnection must not prevent local collection or admission retries.


class Connection:
    def __init__(self, client_id, host, port, ca, cert, key, subscriptions):
        self.host, self.port = host, port
        self.messages = queue.Queue(maxsize=512)
        self.subscriptions = subscriptions
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt.MQTTv5)
        self.client.tls_set(ca_certs=str(ca), certfile=str(cert), keyfile=str(key),
                            tls_version=ssl.PROTOCOL_TLS_CLIENT)
        self.client.max_inflight_messages_set(20)
        self.client.max_queued_messages_set(256)
        self.client.reconnect_delay_set(min_delay=random.randint(1, 3), max_delay=60)
        self.client.on_connect = self._connected
        self.client.on_message = self._message

    def _connected(self, client, userdata, flags, reason, properties):
        if reason.is_failure:
            LOG.error("MQTT connection rejected: %s", reason)
            return
        for topic in self.subscriptions:
            client.subscribe(topic, qos=1)

    def _message(self, client, userdata, message):
        if len(message.payload) > MAX_BYTES or message.retain:
            # Execution commands and storage ACKs must never be retained.
            return
        try:
            self.messages.put_nowait((message.topic, bytes(message.payload)))
        except queue.Full:
            LOG.warning("MQTT input queue full; application sender must retry")

    def start(self):
        properties = Properties(PacketTypes.CONNECT)
        properties.SessionExpiryInterval = 3600
        properties.MaximumPacketSize = MAX_BYTES + 1024
        self.client.connect_async(self.host, self.port, keepalive=60, clean_start=False, properties=properties)
        self.client.loop_start()

    def publish(self, topic, payload):
        if not self.client.is_connected():
            raise ConnectionError("MQTT disconnected")
        properties = Properties(PacketTypes.PUBLISH)
        properties.MessageExpiryInterval = 60
        result = self.client.publish(topic, payload, qos=1, retain=False, properties=properties)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            raise ConnectionError(f"MQTT publish failed: {result.rc}")

    def drain(self, callback, limit=128):
        for _ in range(limit):
            try:
                topic, payload = self.messages.get_nowait()
            except queue.Empty:
                break
            try:
                callback(topic, payload)
            except (ValueError, KeyError, TypeError):
                LOG.exception("Invalid MQTT application message on %s", topic)

    def close(self):
        self.client.disconnect()
        self.client.loop_stop()


class KafkaWriter:
    def __init__(self, bootstrap):
        from confluent_kafka import Producer
        self.producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True,
                                  "acks": "all", "delivery.timeout.ms": 30000,
                                  "queue.buffering.max.messages": 32768,
                                  "queue.buffering.max.kbytes": 32768})

    def send(self, topic, key, payload, headers, completed):
        self.producer.produce(topic, value=payload, key=key, headers=headers,
                              on_delivery=lambda error, message: completed(error))

    def poll(self):
        self.producer.poll(0)

    def close(self):
        self.producer.flush(10)


def edge_receive(collector, pump, connection, topic, payload):
    if topic == f"fleet/v1/{collector.store.vehicle_id}/command":
        result = collector.command(json.loads(payload))
        connection.publish(f"fleet/v1/{collector.store.vehicle_id}/reported", canonical(result))
    else:
        pump.receive(topic, payload)
