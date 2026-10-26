"""Versioned, bounded MQTT upload contract. PUBACK is not a storage receipt."""
from __future__ import annotations

import base64
import hashlib
import json
import re
from dataclasses import dataclass

MAX_BYTES = 64 * 1024
MAX_RECORDS = 128
STREAMS = ("event", "status", "history")


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

    def value(self):
        if type(self.seq) is not int or not 0 <= self.seq < 2**63:
            raise ValueError("invalid sequence")
        if self.kind not in ("signal", "perception"):
            raise ValueError("unsupported kind")
        if type(self.report_interval_ms) is not int or not 100 <= self.report_interval_ms <= 60000:
            raise ValueError("invalid reporting interval")
        return dict(seq=self.seq, kind=self.kind,
                    payload=base64.b64encode(self.payload).decode(),
                    report_interval_ms=self.report_interval_ms)


@dataclass(frozen=True)
class Batch:
    vehicle_id: str
    boot_id: str
    stream: str
    records: tuple[Record, ...]
    batch_id: str

    def body(self):
        return dict(version=1, vehicle_id=self.vehicle_id, boot_id=self.boot_id,
                    stream=self.stream, records=[r.value() for r in self.records])

    @classmethod
    def create(cls, vehicle_id, boot_id, stream, records):
        identity(vehicle_id)
        identity(boot_id)
        records = tuple(records)
        if stream not in STREAMS or not 1 <= len(records) <= MAX_RECORDS:
            raise ValueError("invalid stream or record count")
        if any(b.seq != a.seq + 1 for a, b in zip(records, records[1:])):
            raise ValueError("batch sequence must be contiguous")
        batch = cls(vehicle_id, boot_id, stream, records, "")
        return cls(vehicle_id, boot_id, stream, records,
                   hashlib.sha256(canonical(batch.body())).hexdigest())

    def wire(self):
        return canonical(self.body() | {"batch_id": self.batch_id})

    def topic(self):
        return f"fleet/v1/{self.vehicle_id}/up/{self.stream}"

    @classmethod
    def parse(cls, data, topic):
        if len(data) > MAX_BYTES:
            raise ValueError("batch exceeds byte limit")
        try:
            value = json.loads(data)
            if type(value["version"]) is not int or value["version"] != 1:
                raise ValueError("unsupported version")
            records = [Record(r["seq"], r["kind"],
                              base64.b64decode(r["payload"], validate=True),
                              r["report_interval_ms"]) for r in value["records"]]
            batch = cls.create(value["vehicle_id"], value["boot_id"], value["stream"], records)
            if batch.batch_id != value["batch_id"] or batch.topic() != topic:
                raise ValueError("digest or topic identity mismatch")
            return batch
        except (KeyError, TypeError, UnicodeError) as exc:
            raise ValueError("malformed batch") from exc


def ack_for(batch):
    return dict(version=1, vehicle_id=batch.vehicle_id, boot_id=batch.boot_id,
                stream=batch.stream, batch_id=batch.batch_id,
                first_seq=batch.records[0].seq, last_seq=batch.records[-1].seq)
