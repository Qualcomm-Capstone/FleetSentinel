"""Read-only cutover gate for the former gRPC clip WAL."""

from pathlib import Path
import struct
import zlib

from .wal import COMMIT_FILE, HEADER, KIND_SEGMENT_REF, SEGMENT_SUFFIX


def require_drained_legacy_uploads(root: Path) -> None:
    """Block cutover on pending uploads/references or an unreadable legacy WAL.

    Stop the old uploader before running this check. Recovery belongs to the old
    release: this gate never rewrites or truncates existing data.
    """
    root = Path(root)
    if not root.exists():
        return
    reason = _pending_reason(root)
    if reason:
        raise RuntimeError(
            f"Legacy clip WAL requires draining with the previous release: {root}: {reason}. "
            "Keep these files; do not delete them to bypass the cutover check."
        )


def _pending_reason(root: Path) -> str | None:
    if not root.is_dir():
        return "WAL path is not a directory"
    uploads = root / "uploads"
    if uploads.exists() and any(uploads.iterdir()):
        return "upload intents remain"
    commit = root / COMMIT_FILE
    committed = -1
    if commit.exists():
        data = commit.read_bytes()
        if len(data) != 8:
            return "invalid commit pointer"
        committed = struct.unpack("<Q", data)[0]
    for path in sorted(root.glob(f"*{SEGMENT_SUFFIX}")):
        with path.open("rb") as stream:
            while header := stream.read(HEADER.size):
                if len(header) != HEADER.size:
                    return f"truncated header in {path.name}"
                length, expected_crc, seq, kind = HEADER.unpack(header)
                crc = zlib.crc32(struct.pack("<QB", seq, kind))
                remaining = length
                while remaining:
                    chunk = stream.read(min(remaining, 65536))
                    if not chunk:
                        return f"truncated record in {path.name}"
                    crc = zlib.crc32(chunk, crc)
                    remaining -= len(chunk)
                if crc & 0xFFFFFFFF != expected_crc:
                    return f"corrupt record in {path.name}"
                if kind == KIND_SEGMENT_REF and seq > committed:
                    return "unacknowledged segment references remain"
    return None
