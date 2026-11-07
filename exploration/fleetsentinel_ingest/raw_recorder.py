"""Rotating, self-contained MCAP recording and read-only dataset replay.

Unchunked MCAP records are flushed and fsynced before the SQLite coverage watermark
advances. A torn final record is discarded at recovery; complete records survive.
This intentionally favors an auditable crash boundary over compression throughput.
"""
from __future__ import annotations

import io
import json
import os
import struct
from pathlib import Path

from mcap.data_stream import RecordBuilder
from mcap.exceptions import McapError
from mcap.reader import make_reader
from mcap.records import Channel, DataEnd, Footer, Message, Metadata, Schema
from mcap.stream_reader import StreamReader
from mcap.writer import CompressionType, IndexType, Writer

from .raw_store import sync_directory
from .telemetry_store import CapacityError

MAGIC = b"\x89MCAP0\r\n"
TAIL_BYTES = 50  # DataEnd (13), Footer (29), trailing magic (8)


def _encoded(record):
    builder = RecordBuilder()
    record.write(builder)
    return builder.end()


def _writer(stream, metadata):
    writer = Writer(stream, use_chunking=False, compression=CompressionType.NONE,
                    index_types=IndexType.NONE, repeat_schemas=False, repeat_channels=False,
                    use_statistics=False, use_summary_offsets=False)
    writer.start(profile="fleetsentinel", library="fleetsentinel-ingest")
    writer.add_metadata("fleetsentinel.raw", {
        "scene_id": metadata.get("scene_id") or "",
        "calibration": json.dumps(metadata.get("calibration", {}), sort_keys=True),
    })
    return writer


class RawRecorder:
    def __init__(self, store, *, clock_us=None, rotation_us=10_000_000,
                 rotation_bytes=256 * 1024 ** 2, scene_id=None, calibration=None):
        if rotation_us <= 0 or rotation_bytes <= TAIL_BYTES:
            raise ValueError("invalid raw rotation bounds")
        self.store = store
        self.clock_us = clock_us or store.clock_us
        self.rotation_us, self.rotation_bytes = rotation_us, rotation_bytes
        self.scene_id, self.calibration = scene_id, dict(calibration or {})
        self._fp = self._writer = self._file_id = None
        self._channels = {}
        self._last = None
        self._count = 0

    def _metadata(self):
        return {"scene_id": self.scene_id, "calibration": self.calibration,
                "sensor_channels": sorted(self._channels), "sample_count": self._count}

    def _channel_size(self, topic, encoding, schema, metadata):
        if topic in self._channels:
            if self._channels[topic][1:] != (encoding, schema, metadata):
                raise ValueError("channel schema or encoding changed within a segment")
            return 0
        size = len(_encoded(Channel(1, topic, encoding, metadata, 1 if schema else 0)))
        if schema:
            name, schema_encoding, data = schema
            size += len(_encoded(Schema(1, data, schema_encoding, name)))
        return size

    def append(self, timestamp_us, topic, data, *, message_encoding="application/octet-stream",
               schema=None, channel_metadata=None):
        if type(timestamp_us) is not int or timestamp_us < 0:
            raise ValueError("timestamp must be nonnegative integer microseconds")
        if self._last is not None and timestamp_us < self._last:
            raise ValueError("raw input must be ordered by timestamp")
        if not isinstance(data, bytes) or not isinstance(topic, str) or not topic:
            raise ValueError("raw payload must be bytes with a nonempty topic")
        channel_metadata = dict(channel_metadata or {})
        message_bytes = 31 + len(data)
        if self._fp and timestamp_us - self._start >= self.rotation_us:
            self.close()
        channel_bytes = self._channel_size(topic, message_encoding, schema, channel_metadata)
        if self._fp and self._fp.tell() + channel_bytes + message_bytes + TAIL_BYTES > self.rotation_bytes:
            self.close()
            channel_bytes = self._channel_size(topic, message_encoding, schema, channel_metadata)
        if self._fp is None:
            self._count, self._channels = 0, {}
            probe = io.BytesIO()
            _writer(probe, self._metadata())
            header_bytes = probe.tell()
            needed = header_bytes + channel_bytes + message_bytes + TAIL_BYTES
            if needed > self.rotation_bytes:
                raise CapacityError("single raw message exceeds segment rotation byte limit")
            self.store.ensure_space(needed, now_us=self.clock_us())
            self._file_id, path = self.store.begin_file(timestamp_us, self._metadata())
            self._fp = path.open("xb")
            sync_directory(self.store.root)
            self._writer = _writer(self._fp, self._metadata())
            self._start = timestamp_us
        else:
            self.store.ensure_space(channel_bytes + message_bytes, now_us=self.clock_us())
        if topic not in self._channels:
            sid = self._writer.register_schema(name=schema[0], encoding=schema[1], data=schema[2]) if schema else 0
            cid = self._writer.register_channel(topic=topic, message_encoding=message_encoding,
                                                schema_id=sid, metadata=channel_metadata)
            self._channels[topic] = (cid, message_encoding, schema, channel_metadata)
        self._writer.add_message(channel_id=self._channels[topic][0], log_time=timestamp_us * 1000,
                                 publish_time=timestamp_us * 1000, data=data)
        self._fp.flush()
        os.fsync(self._fp.fileno())
        self._last, self._count = timestamp_us, self._count + 1
        # Reserve the exact footer bytes so closing cannot exceed total capacity.
        self.store.update_open(self._file_id, timestamp_us + 1,
                               self._fp.tell() + TAIL_BYTES, self._metadata())

    def tick(self, timestamp_us):
        if self._fp and timestamp_us - self._start >= self.rotation_us:
            self.close()

    def close(self):
        if self._fp is None:
            return
        self._writer.finish()
        self._fp.flush()
        os.fsync(self._fp.fileno())
        self._fp.close()
        self.store.seal_file(self._file_id, self._start, self._last + 1, self._metadata())
        self._fp = self._writer = self._file_id = None
        self._channels, self._count = {}, 0


def recover_file(store, file_id, metadata):
    """Recover only complete records, in place, with no full-file buffering."""
    partial, final = store._path(file_id, ".partial"), store._path(file_id)
    path = partial if partial.exists() else final
    channels, count, first, last, valid_end = {}, 0, None, None, 0
    if path.exists():
        with path.open("rb") as stream:
            try:
                for record in StreamReader(stream, record_size_limit=256 * 1024 ** 2).records:
                    if isinstance(record, (DataEnd, Footer)):
                        break
                    if isinstance(record, Channel):
                        channels[record.id] = record.topic
                    elif isinstance(record, Message):
                        timestamp = record.log_time // 1000
                        first = timestamp if first is None else min(first, timestamp)
                        last = timestamp if last is None else max(last, timestamp)
                        count += 1
                    valid_end = stream.tell()
            except (McapError, EOFError, struct.error):
                pass  # A partial final record has no coverage.
    if count == 0:
        with store.atomic():
            store.db.execute("UPDATE raw_files SET state='LOST',size_bytes=? WHERE id=?",
                             (path.stat().st_size if path.exists() else 0, file_id))
        return
    with path.open("r+b") as stream:
        stream.truncate(valid_end)
        stream.seek(valid_end)
        stream.write(_encoded(DataEnd(0)))
        stream.write(_encoded(Footer(0, 0, 0)))
        stream.write(MAGIC)
        stream.flush()
        os.fsync(stream.fileno())
    metadata.update(sensor_channels=sorted(set(channels.values())), sample_count=count)
    store.seal_file(file_id, first, last + 1, metadata)


def replay_mcap(path, recorder, *, clock=None, on_message=None, shift_us=0):
    """Replay source read-only; clock(timestamp_us) may pace or advance a virtual clock.

    Metadata is collected in a bounded streaming pass so every rotated output is
    independently interpretable, even when calibration appears late in the input.
    on_message receives the original (schema, channel, message) records. Pass
    shift_us=None to align the first frame to the local clock after metadata scan.
    """
    if Path(path).resolve().is_relative_to(recorder.store.root):
        raise ValueError("replay source must be outside the managed destination spool")
    with open(path, "rb") as stream:
        channels = {}
        for record in StreamReader(stream).records:
            if isinstance(record, Channel):
                channels[record.id] = record.topic
            elif isinstance(record, Metadata) and record.name == "fleetsentinel.raw":
                recorder.calibration.update(json.loads(record.metadata.get("calibration", "{}")))
                recorder.scene_id = record.metadata.get("scene_id") or recorder.scene_id
            elif isinstance(record, Message):
                topic = channels.get(record.channel_id)
                if topic == "/tf/calibration":
                    value = json.loads(record.data)
                    recorder.calibration[value["channel"]] = json.dumps(value, sort_keys=True)
                elif topic == "/vehicle/signal" and recorder.scene_id is None:
                    recorder.scene_id = json.loads(record.data).get("scene_id")
    try:
        with open(path, "rb") as stream:
            reader = make_reader(stream)
            summary = reader.get_summary()
            # Non-indexed files must already be timestamp ordered. Requesting
            # sorting on them makes the MCAP library retain the entire dataset.
            indexed = bool(summary and summary.chunk_indexes)
            for schema, channel, message in reader.iter_messages(log_time_order=indexed):
                if shift_us is None:
                    shift_us = recorder.clock_us() - message.log_time // 1000
                timestamp = message.log_time // 1000 + shift_us
                if clock:
                    clock(timestamp)
                recorder.append(timestamp, channel.topic, message.data,
                                message_encoding=channel.message_encoding,
                                schema=(schema.name, schema.encoding, schema.data) if schema else None,
                                channel_metadata=channel.metadata)
                if on_message:
                    on_message(schema, channel, message)
    finally:
        recorder.close()
