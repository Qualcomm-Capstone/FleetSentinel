"""Single-owner durable operator dispatcher; mTLS service identity authorizes commands.

This local control-plane implementation is not a multi-replica database. A production
API must add per-operator authorization/audit and a shared transactional database.
"""
import json
import sqlite3

from .mqtt_contract import canonical, identity
from .collection_policy import Policy


class CommandStore:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS commands (id TEXT PRIMARY KEY, vehicle TEXT, expires INTEGER, desired BLOB, reported BLOB)")

    def submit(self, command, now_ms):
        identity(command["command_id"])
        identity(command["vehicle_id"])
        if command["type"] not in ("collection_policy", "history_request", "diagnostic"):
            raise ValueError("unsupported operator command")
        if type(command["expires_at_ms"]) is not int or not now_ms < command["expires_at_ms"] <= now_ms + 300000:
            raise ValueError("command must expire within five minutes")
        if command["type"] == "collection_policy":
            Policy.from_command(command)
        wire = canonical(command)
        if len(wire) > 8192:
            raise ValueError("command too large")
        with self.db:
            row = self.db.execute("SELECT desired FROM commands WHERE id=?", (command["command_id"],)).fetchone()
            if row and row[0] != wire:
                raise ValueError("command id already used")
            self.db.execute("INSERT OR IGNORE INTO commands VALUES (?,?,?,?,NULL)",
                            (command["command_id"], command["vehicle_id"], command["expires_at_ms"], wire))

    def pending(self, now_ms):
        return [json.loads(row[0]) for row in self.db.execute(
            "SELECT desired FROM commands WHERE reported IS NULL AND expires>? ORDER BY id", (now_ms,))]

    def report(self, topic, result):
        if not isinstance(result, dict) or result.get("status") not in ("applied", "rejected", "expired"):
            return False
        if topic != f"fleet/v1/{result.get('vehicle_id')}/reported":
            return False
        with self.db:
            cursor = self.db.execute("UPDATE commands SET reported=? WHERE id=? AND vehicle=? AND reported IS NULL",
                                     (canonical(result), result.get("command_id"), result.get("vehicle_id")))
            return cursor.rowcount == 1

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
