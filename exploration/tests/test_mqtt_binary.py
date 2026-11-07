"""Binary upload compatibility, corrupted frames, and durable migration boundaries."""
import hashlib
import io
import json
from pathlib import Path

import fastavro
import pytest

from fleetsentinel_ingest.mqtt_bridge import Bridge
from fleetsentinel_ingest.mqtt_contract import (
    BINARY_MAGIC, MAX_BYTES, Batch, Record, ack_for,
)
from fleetsentinel_ingest.telemetry_store import CapacityError, TelemetryStore


# Captured from the v1 encoder before the binary migration. Do not regenerate.
LEGACY_WIRE = (
    b'{"batch_id":"9b11ab3eb4437e452cb4c2af69e25d71ce2d56791e4e63a2834a82b1bb18caeb",'
    b'"boot_id":"boot","records":[{"kind":"signal","payload":"AP9hYmM=",'
    b'"report_interval_ms":1000,"seq":0}],"stream":"status","vehicle_id":"vehicle-1","version":1}'
)
TOPIC = "fleet/v1/vehicle-1/up/status"
SCHEMA = fastavro.parse_schema(json.loads(
    (Path(__file__).resolve().parents[2] / "schemas/mqtt-batch-v2.avsc").read_text()))


def frame(body):
    return BINARY_MAGIC + hashlib.sha256(BINARY_MAGIC + body).digest() + body


def encode_body(value):
    output = io.BytesIO()
    fastavro.schemaless_writer(output, SCHEMA, value)
    return output.getvalue()


def test_old_wire_and_ack_remain_unchanged():
    legacy = Batch.parse(LEGACY_WIRE, TOPIC)
    assert legacy.wire_version == 1
    assert legacy.records == (Record(0, "signal", b"\x00\xffabc", 1000),)
    assert legacy.wire() == LEGACY_WIRE
    assert ack_for(legacy) == dict(
        version=1, vehicle_id="vehicle-1", boot_id="boot", stream="status",
        batch_id="9b11ab3eb4437e452cb4c2af69e25d71ce2d56791e4e63a2834a82b1bb18caeb",
        first_seq=0, last_seq=0,
    )


@pytest.mark.parametrize("stream", ["event", "status", "history"])
def test_binary_preserves_all_payload_bytes_and_extreme_valid_identifiers(stream):
    batch = Batch.create("v" * 80, "b" * 80, stream,
                         [Record(2**63 - 2, "signal", bytes(range(256)), 100),
                          Record(2**63 - 1, "perception", b"", 60000)])
    assert batch.wire().startswith(BINARY_MAGIC)
    assert Batch.parse(batch.wire(), batch.topic()) == batch


def test_upgrade_retries_persisted_v1_before_creating_v2(tmp_path):
    path = tmp_path / "edge.sqlite"
    with TelemetryStore(path, "vehicle-1") as store:
        with store.atomic():
            store.set_meta("boot_id", "boot")
            store.enqueue("status", "signal", b"\x00\xffabc", 1000)
            store.db.execute("INSERT INTO batches VALUES (?,?)", ("status", LEGACY_WIRE))
    with TelemetryStore(path, "vehicle-1") as store:
        legacy = store.batch("status")
        assert legacy.wire() == LEGACY_WIRE
        store.enqueue("status", "signal", b"new\x00\xff", 2000)
        assert store.batch("status").wire() == LEGACY_WIRE
        assert store.acknowledge(ack_for(legacy))
        binary = store.batch("status")
        assert binary.wire_version == 2
        assert binary.records == (Record(1, "signal", b"new\x00\xff"),)
        assert not store.acknowledge(ack_for(legacy))
    with TelemetryStore(path, "vehicle-1") as store:
        assert store.batch("status").wire() == binary.wire()
        assert store.acknowledge(ack_for(binary))
        assert store.pending() == 0


def test_bridge_produces_identical_kafka_records_for_both_formats():
    class Kafka:
        def __init__(self):
            self.writes = []

        def send(self, topic, key, payload, headers, done):
            self.writes.append((topic, key, payload, headers))
            done(None)

    legacy = Batch.parse(LEGACY_WIRE, TOPIC)
    binary = Batch.create(legacy.vehicle_id, legacy.boot_id, legacy.stream, legacy.records)
    kafka, replies = Kafka(), []
    bridge = Bridge(kafka, lambda topic, data: replies.append(json.loads(data)))
    bridge.receive(TOPIC, LEGACY_WIRE)
    bridge.receive(TOPIC, binary.wire())
    assert kafka.writes[0] == kafka.writes[1]
    assert replies == [ack_for(legacy), ack_for(binary)]
    assert legacy.batch_id != binary.batch_id


def test_every_truncation_and_digest_corruption_is_rejected():
    batch = Batch.create("vehicle-1", "boot", "status", [Record(0, "signal", b"\x00\xffabc")])
    wire = batch.wire()
    for end in range(len(wire)):
        with pytest.raises(ValueError):
            Batch.parse(wire[:end], TOPIC)
    for offset in (4, len(BINARY_MAGIC), len(wire) - 1):
        damaged = bytearray(wire)
        damaged[offset] ^= 1
        with pytest.raises(ValueError):
            Batch.parse(bytes(damaged), TOPIC)
    with pytest.raises(ValueError):
        Batch.parse(wire, "fleet/v1/vehicle-2/up/status")


@pytest.mark.parametrize("body", [b"", b"\x80", b"\xff" * 10, b"\x02\xff"])
def test_malformed_avro_with_valid_checksum_is_rejected(body):
    with pytest.raises(ValueError):
        Batch.parse(frame(body), TOPIC)


def test_trailing_data_is_rejected_even_with_valid_checksum():
    batch = Batch.create("vehicle-1", "boot", "status", [Record(0, "signal", b"abc")])
    with pytest.raises(ValueError, match="trailing"):
        Batch.parse(frame(encode_body(batch.body()) + b"extra"), TOPIC)


def test_noncanonical_integer_encoding_is_rejected():
    batch = Batch.create("vehicle-1", "boot", "status", [Record(0, "signal", b"abc")])
    body = encode_body(batch.body())
    # Same vehicle_id length encoded with a redundant varint byte.
    with pytest.raises(ValueError, match="noncanonical"):
        Batch.parse(frame(bytes([body[0] | 0x80, 0]) + body[1:]), TOPIC)


def test_exact_wire_size_boundary():
    oversized = Batch.create("vehicle-1", "boot", "status", [Record(0, "signal", bytes(MAX_BYTES))])
    overhead = len(oversized.wire()) - MAX_BYTES
    exact = Batch.create("vehicle-1", "boot", "status", [Record(0, "signal", bytes(MAX_BYTES - overhead))])
    assert len(exact.wire()) == MAX_BYTES
    assert Batch.parse(exact.wire(), TOPIC) == exact
    with pytest.raises(ValueError, match="byte limit"):
        Batch.parse(exact.wire() + b"\x00", TOPIC)


@pytest.mark.parametrize("invalid", ["empty", "too_many", "sequence_gap", "negative_seq", "interval"])
def test_semantically_invalid_binary_batches_are_rejected(invalid):
    body = Batch.create("vehicle-1", "boot", "status", [Record(0, "signal", b"abc")]).body()
    record = body["records"][0]
    if invalid == "empty":
        body["records"] = []
    elif invalid == "too_many":
        body["records"] = [record | {"seq": i} for i in range(129)]
    elif invalid == "sequence_gap":
        body["records"].append(record | {"seq": 2})
    elif invalid == "negative_seq":
        record["seq"] = -1
    else:
        record["report_interval_ms"] = 99
    with pytest.raises(ValueError):
        Batch.parse(frame(encode_body(body)), TOPIC)


def test_binary_recovers_base64_headroom_without_exceeding_wire_limit(tmp_path):
    with TelemetryStore(tmp_path / "edge.sqlite", "vehicle-1") as store:
        store.enqueue("history", "signal", bytes(50_000), 0)
        batch = store.batch("history")
        assert len(batch.wire()) < MAX_BYTES
        assert Batch.parse(batch.wire(), batch.topic()) == batch
        legacy = Batch.create(batch.vehicle_id, batch.boot_id, batch.stream, batch.records, wire_version=1)
        assert len(legacy.wire()) > MAX_BYTES
        with pytest.raises(CapacityError):
            store.enqueue("history", "signal", bytes(MAX_BYTES), 0)
    with pytest.raises(ValueError):
        Batch.parse(BINARY_MAGIC + bytes(MAX_BYTES), TOPIC)


@pytest.mark.parametrize("size,count", [(100, 1), (500, 1), (2000, 1), (100, 10), (500, 10)])
def test_wire_reduction_for_representative_uncompressed_batches(size, count):
    records = [Record(100000 + i, "signal", bytes(size), 10000) for i in range(count)]
    binary = Batch.create("vehicle-001", "a" * 32, "status", records)
    legacy = Batch.create("vehicle-001", "a" * 32, "status", records, wire_version=1)
    # Regression budget, not a claim about LTE billing or compressed traffic.
    assert len(binary.wire()) < len(legacy.wire()) * 0.8
