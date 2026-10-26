"""Collection decisions use receive time; sensor timestamps remain unchanged in Avro."""
from __future__ import annotations

import io
import json
import math
import time
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path

import fastavro

from .mqtt_contract import canonical, identity


@dataclass(frozen=True)
class Policy:
    version: int = 0
    normal_ms: int = 10000
    min_ms: int = 1000
    focus_ms: int = 1000
    focus_until_ms: int = 0
    channels: tuple[str, ...] = ("ego_pose", "fleet_status")
    focus_channels: tuple[str, ...] = ()
    channel_intervals_ms: dict[str, int] = field(default_factory=dict)
    thresholds: dict[str, float] = field(default_factory=lambda: {"speed_mps": 2.0, "battery_pct": 5.0})
    history_ms: int = 30000
    anomaly_focus_ms: int = 30000

    def __post_init__(self):
        if any(type(v) is not int for v in (self.version, self.normal_ms, self.min_ms, self.focus_ms,
                                           self.focus_until_ms, self.history_ms, self.anomaly_focus_ms)):
            raise ValueError("policy times/version must be integers")
        if self.version < 0 or self.focus_until_ms < 0:
            raise ValueError("negative policy version/focus time")
        if not 100 <= self.focus_ms <= self.normal_ms <= 60000 or not 100 <= self.min_ms <= self.normal_ms:
            raise ValueError("invalid reporting interval")
        if not 0 <= self.history_ms <= 3600000 or not 0 < self.anomaly_focus_ms <= 300000:
            raise ValueError("history/focus exceeds policy bounds")
        if (not isinstance(self.channels, (list, tuple))
                or not isinstance(self.focus_channels, (list, tuple))
                or not self.channels or len(self.channels) + len(self.focus_channels) > 32 or any(
                    not isinstance(c, str) or not c.strip() for c in (*self.channels, *self.focus_channels))):
            raise ValueError("invalid selected channels")
        for mapping in (self.channel_intervals_ms, self.thresholds):
            if not isinstance(mapping, dict) or any(not isinstance(k, str) or not k.strip() for k in mapping):
                raise ValueError("policy channel intervals/thresholds must be objects with signal names")
        if any(type(v) is not int or not 100 <= v <= 60000 for v in self.channel_intervals_ms.values()):
            raise ValueError("invalid per-channel reporting interval")
        try:
            invalid = any(type(v) not in (int, float) or not math.isfinite(v) or v < 0
                          for v in self.thresholds.values())
        except OverflowError:
            invalid = True
        if invalid:
            raise ValueError("invalid change threshold")

    @classmethod
    def from_command(cls, command):
        """The dispatcher and the vehicle enforce the same policy shape/lifetime."""
        if not isinstance(command.get("policy"), dict):
            raise ValueError("policy must be an object")
        version = command.get("version")
        if type(version) is not int or version <= 0:
            raise ValueError("policy version must be a positive integer")
        try:
            policy = cls(**(command["policy"] | {"version": version}))
        except TypeError as exc:
            raise ValueError("unknown policy fields") from exc
        if policy.focus_until_ms > command["expires_at_ms"]:
            raise ValueError("focus outlives command")
        return policy

    def interval(self, now_ms, channel=None):
        normal = self.channel_intervals_ms.get(channel, self.normal_ms)
        focused = not self.focus_channels or channel is None or channel in self.focus_channels
        return min(normal, self.focus_ms) if now_ms < self.focus_until_ms and focused else normal

    def selected(self, channel, now_ms):
        return channel in self.channels or (now_ms < self.focus_until_ms and channel in self.focus_channels)


class Collector:
    def __init__(self, store, policy=None, clock=None):
        self.store = store
        self.clock = clock or (lambda: int(time.time() * 1000))
        saved = store.get_meta("policy")
        self._policy = Policy(**json.loads(saved)) if saved else (policy or Policy())
        self.anomaly_until_ms = int(store.get_meta("anomaly_until_ms") or 0)
        self.last = {}
        schema = Path(__file__).resolve().parents[2] / "schemas" / "vehicle-signal.avsc"
        self.schema = fastavro.parse_schema(json.loads(schema.read_text()))

    @property
    def policy(self):
        # Operator policy and incident state have independent lifetimes. Old DBs
        # without incident metadata retain their saved policy until it expires.
        if self.anomaly_until_ms > self._policy.focus_until_ms:
            return replace(self._policy, focus_until_ms=self.anomaly_until_ms)
        return self._policy

    def observe(self, row, *, now_ms=None, anomaly=False):
        now = self.clock() if now_ms is None else now_ms
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, self.schema, row)
        payload = buf.getvalue()
        with self.store.atomic():
            self.store.record_local(payload, now)
            policy = self.policy
            anomaly_until = self.anomaly_until_ms
            if anomaly:
                anomaly_until = max(anomaly_until, now + policy.anomaly_focus_ms)
                policy = replace(policy, focus_until_ms=max(policy.focus_until_ms, anomaly_until))
                self.store.enqueue("event", "signal", payload, now, policy.focus_ms)
                # Promotion occurs once per anomaly episode, not once per anomalous sample.
                if self.anomaly_until_ms <= now:
                    self.store.defer_history(now - policy.history_ms, now)
                self.store.set_meta("policy", json.dumps(asdict(self._policy)))
                self.store.set_meta("anomaly_until_ms", str(anomaly_until))
                result = "event"
            else:
                result = None
            channel = row["channel"]
            previous = self.last.get(channel)
            interval = policy.interval(now, channel)
            elapsed = now - previous[0] if previous else interval
            changed = previous and (row["values_str"] != previous[1]["values_str"] or any(
                name in row["values_num"] and abs(row["values_num"][name] - previous[1]["values_num"].get(name, 0)) >= threshold
                for name, threshold in policy.thresholds.items()))
            if not anomaly and policy.selected(channel, now) and (
                    elapsed >= interval or (previous and previous[2] != interval)
                    or (changed and elapsed >= policy.min_ms)):
                self.store.enqueue("status", "signal", payload, now, interval)
                result = "status"
        # Advance memory only after SQLite commits successfully.
        self.anomaly_until_ms = anomaly_until
        if result == "status":
            self.last[channel] = (now, row, interval)
        if anomaly:
            # The event and selected history are already durable. A full history
            # outbox must not roll back the event or its focus policy.
            self.store.drain_history()
        return result

    def command(self, command):
        now = self.clock()
        try:
            command_id = identity(command["command_id"])
            wire = canonical(command)
            if len(wire) > 8192 or command["vehicle_id"] != self.store.vehicle_id:
                raise ValueError("command identity/size mismatch")
        except (ValueError, KeyError, TypeError):
            return {"status": "rejected", "reason": "invalid command"}
        result = dict(command_id=command_id, vehicle_id=self.store.vehicle_id, status="applied")
        updated = None
        with self.store.atomic():
            prior = self.store.db.execute("SELECT command,result FROM receipts WHERE id=?", (command_id,)).fetchone()
            if prior:
                return json.loads(prior[1]) if prior[0] == wire else result | {"status": "rejected", "reason": "id reused"}
            try:
                expires = command["expires_at_ms"]
                if type(expires) is not int or expires <= now:
                    result["status"] = "expired"
                elif expires > now + 300000:
                    raise ValueError("command lifetime exceeds five minutes")
                elif command["type"] == "collection_policy":
                    version = command["version"]
                    if type(version) is not int or version <= self.policy.version:
                        raise ValueError("stale policy version")
                    updated = Policy.from_command(command)
                    self.store.set_meta("policy", json.dumps(asdict(updated)))
                    result["version"] = version
                elif command["type"] == "history_request":
                    start, end = command["start_ms"], command["end_ms"]
                    if type(start) is not int or type(end) is not int or not now - self.store.retention_ms <= start <= end <= now:
                        raise ValueError("history window outside retention")
                    # Applied means the retained selection is durable, not that
                    # the upload has completed. Movement uses the bounded retry path.
                    result["records"] = self.store.defer_history(start, end)
                elif command["type"] == "diagnostic":
                    result.update(pending_records=self.store.pending(),
                                  deferred_history_records=self.store.deferred_history(),
                                  operator_focus_until_ms=self._policy.focus_until_ms,
                                  anomaly_focus_until_ms=self.anomaly_until_ms,
                                  policy_version=self.policy.version)
                else:
                    raise ValueError("unsupported command type")
            except (KeyError, TypeError, ValueError) as exc:
                result.update(status="rejected", reason=str(exc))
                updated = None
            self.store.db.execute("INSERT INTO receipts VALUES (?,?,?)", (command_id, wire, canonical(result)))
        if updated:
            self._policy = updated
            self.last.clear()  # Next available samples announce the new cadence immediately.
        return result
