"""Versioned, bounded MQTT upload contract. PUBACK is not a storage receipt."""
from __future__ import annotations

import base64
import hashlib
import io
import json
import re
from dataclasses import dataclass
from pathlib import Path

import fastavro

MAX_BYTES = 64 * 1024
MAX_RECORDS = 128
STREAMS = ("event", "status", "history")
BINARY_MAGIC = b"FSMQ\x02"
_SCHEMA = fastavro.parse_schema(json.loads(
    (Path(__file__).resolve().parents[2] / "schemas" / "mqtt-batch-v2.avsc").read_text()))


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def identity(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", value):
        raise ValueError("invalid identity")
    return value


@dataclass(frozen=True)
class Record:
    seq: int
    kind: str
    payload: bytes
    report_interval_ms: int = 1000

    def value(self, *, binary=False):
        if type(self.seq) is not int or not 0 <= self.seq < 2**63:
            raise ValueError("invalid sequence")
        if self.kind not in ("signal", "perception"):
            raise ValueError("unsupported kind")
        if type(self.report_interval_ms) is not int or not 100 <= self.report_interval_ms <= 60000:
            raise ValueError("invalid reporting interval")
        if not isinstance(self.payload, bytes):
            raise ValueError("payload must be bytes")
        return dict(seq=self.seq, kind=self.kind,
                    payload=self.payload if binary else base64.b64encode(self.payload).decode(),
                    report_interval_ms=self.report_interval_ms)


@dataclass(frozen=True)
class Batch:
    vehicle_id: str
    boot_id: str
    stream: str
    records: tuple[Record, ...]
    batch_id: str
    wire_version: int = 2

    def body(self):
        body = dict(vehicle_id=self.vehicle_id, boot_id=self.boot_id,
                    stream=self.stream, records=[r.value(binary=self.wire_version == 2) for r in self.records])
        if self.wire_version == 1:
            body["version"] = 1
        return body

    def _encoded_body(self):
        if self.wire_version == 1:
            return canonical(self.body())
        output = io.BytesIO()
        fastavro.schemaless_writer(output, _SCHEMA, self.body())
        return output.getvalue()

    @classmethod
    def create(cls, vehicle_id, boot_id, stream, records, *, wire_version=2):
        identity(vehicle_id)
        identity(boot_id)
        if type(wire_version) is not int or wire_version not in (1, 2):
            raise ValueError("unsupported version")
        records = tuple(records)
        if stream not in STREAMS or not 1 <= len(records) <= MAX_RECORDS:
            raise ValueError("invalid stream or record count")
        for record in records:
            record.value(binary=True)
        if any(b.seq != a.seq + 1 for a, b in zip(records, records[1:])):
            raise ValueError("batch sequence must be contiguous")
        batch = cls(vehicle_id, boot_id, stream, records, "", wire_version)
        body = batch._encoded_body()
        # Domain-separate v2 IDs. Persisted v1 batches retain their original IDs.
        digest = hashlib.sha256((BINARY_MAGIC if wire_version == 2 else b"") + body).hexdigest()
        return cls(vehicle_id, boot_id, stream, records,
                   digest, wire_version)

    def wire(self):
        if self.wire_version == 2:
            return BINARY_MAGIC + bytes.fromhex(self.batch_id) + self._encoded_body()
        return canonical(self.body() | {"batch_id": self.batch_id})

    def topic(self):
        return f"fleet/v1/{self.vehicle_id}/up/{self.stream}"

    @classmethod
    def parse(cls, data, topic):
        if len(data) > MAX_BYTES:
            raise ValueError("batch exceeds byte limit")
        try:
            if data.startswith(b"FSMQ"):
                return cls._parse_binary(data, topic)
            value = json.loads(data)
            if type(value["version"]) is not int or value["version"] != 1:
                raise ValueError("unsupported version")
            records = [Record(r["seq"], r["kind"],
                              base64.b64decode(r["payload"], validate=True),
                              r["report_interval_ms"]) for r in value["records"]]
            batch = cls.create(value["vehicle_id"], value["boot_id"], value["stream"], records,
                               wire_version=1)
            if batch.batch_id != value["batch_id"] or batch.topic() != topic:
                raise ValueError("digest or topic identity mismatch")
            return batch
        except (KeyError, TypeError, UnicodeError, EOFError, IndexError, OverflowError) as exc:
            raise ValueError("malformed batch") from exc

    @classmethod
    def _parse_binary(cls, data, topic):
        body_start = len(BINARY_MAGIC) + 32
        if not data.startswith(BINARY_MAGIC) or len(data) <= body_start:
            raise ValueError("unsupported version or truncated binary batch")
        digest, body = data[len(BINARY_MAGIC):body_start], data[body_start:]
        if hashlib.sha256(BINARY_MAGIC + body).digest() != digest:
            raise ValueError("digest mismatch")
        reader = io.BytesIO(body)
        value = fastavro.schemaless_reader(reader, _SCHEMA)
        if reader.tell() != len(body):
            raise ValueError("trailing binary batch data")
        records = [Record(r["seq"], r["kind"], r["payload"], r["report_interval_ms"])
                   for r in value["records"]]
        batch = cls.create(value["vehicle_id"], value["boot_id"], value["stream"], records)
        if batch.batch_id != digest.hex() or batch.topic() != topic:
            raise ValueError("noncanonical batch or topic identity mismatch")
        return batch


def ack_for(batch):
    return dict(version=1, vehicle_id=batch.vehicle_id, boot_id=batch.boot_id,
                stream=batch.stream, batch_id=batch.batch_id,
                first_seq=batch.records[0].seq, last_seq=batch.records[-1].seq)
