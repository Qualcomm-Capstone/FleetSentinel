"""온보드 WAL — 전송 전 durability를 위한 append-only 로컬 스풀.

설계 정본은 `docs/wal-design.md`. 이 구현은 **재생기용 단순화 버전**이며 그 설계의
핵심 성질을 실증하는 것이 목적이다:

  W-1 전송 전 durable      → 그룹 커밋 fsync
  W-2 재시작 시 재개       → 커밋 포인터
  W-3 디스크 유한          → 세그먼트 롤링 + 삭제
  W-6 손상 꼬리 복구       → 레코드별 CRC

## 레코드 포맷

    ┌────────┬─────────┬────────┬────────┬──────────────┐
    │ len:4  │ crc32:4 │ seq:8  │ kind:1 │ payload:len  │
    └────────┴─────────┴────────┴────────┴──────────────┘

`crc32`는 `seq + kind + payload`를 덮는다. 크래시 시점의 잘린 꼬리를 판별하는 유일한
수단이고, **CRC 실패를 오류가 아니라 "꼬리를 찾았다"로 해석**해 그 지점에서 잘라낸다.

`kind`로 경량 레코드와 `segment-ref`를 한 로그에 담는다(설계 §3.9 — WAL이 중량 경로의
outbox 역할을 겸한다).

## at-least-once면 충분하다

커밋 포인터가 뒤처져 있으면 재시작 후 이미 보낸 것을 다시 보낸다. 그건 중복이고 하류
`event_id` dedup이 흡수한다. WAL에서 exactly-once를 만들려 하면 복잡해지는데 이미 dedup
계층이 있으므로 그럴 필요가 없다 — 계층별 책임 분리.
"""

from __future__ import annotations

import os
import struct
import threading
import uuid
import time
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Literal

HEADER = struct.Struct("<IIQB")  # len, crc32, seq, kind
HEADER_SIZE = HEADER.size  # 17

RecordKind = Literal[0, 1]
KIND_SIGNAL = 0
KIND_SEGMENT_REF = 1

SEGMENT_SUFFIX = ".seg"
COMMIT_FILE = "COMMIT"
BOOT_FILE = "BOOT"
DROPPED_FILE = "DROPPED"
DROPPED_BODY = struct.Struct("<QQ")
DROPPED_RECORD = struct.Struct("<QQI")

# 기본값. 재생기용이라 설계 문서(64MB / 10ms)보다 작게 잡아 테스트가 빠르다.
DEFAULT_SEGMENT_BYTES = 4 * 1024 * 1024
DEFAULT_COMMIT_INTERVAL_S = 0.010
DEFAULT_COMMIT_BYTES = 4 * 1024


@dataclass(frozen=True, slots=True)
class WalRecord:
    seq: int
    kind: int
    payload: bytes


@dataclass(frozen=True, slots=True)
class WalStats:
    """관측 지표. 적체량이 곧 "네트워크가 못 따라간다"의 신호다(설계 §3.7)."""

    appended: int
    committed_seq: int
    pending_records: int
    pending_bytes: int
    segments: int
    disk_bytes: int
    fsync_count: int
    dropped_seq_ranges: list[tuple[int, int]]


class Wal:
    """append-only 세그먼트 로그.

    :param root: 로그 디렉터리
    :param segment_bytes: 세그먼트 롤링 임계
    :param commit_interval_s: 그룹 커밋 주기. 전원 손실 시 최대 이 구간이 날아간다
    :param max_disk_bytes: 상한. 초과 시 **가장 오래된 세그먼트를 버린다**(설계 §3.6) —
        쓰기를 막으면 센서 계층으로 역전파돼 프로토콜 바깥에서 조용히 사라지는데,
        버리는 쪽은 `seq` 결번으로 탐지 가능하다
    """

    def __init__(
        self,
        root: Path,
        *,
        segment_bytes: int = DEFAULT_SEGMENT_BYTES,
        commit_interval_s: float = DEFAULT_COMMIT_INTERVAL_S,
        commit_bytes: int = DEFAULT_COMMIT_BYTES,
        max_disk_bytes: int | None = None,
    ) -> None:
        # 세그먼트 파일을 만지는 모든 작업을 직렬화한다.
        #
        # ack이 **다른 스레드에서** 올 수 있다 — `GrpcTransport`의 CACK 리더가 그렇다.
        # 그러면 `commit()`이 세그먼트를 unlink 하는 동안 배송기 스레드의
        # `WalCursor._drain`이 같은 경로를 `exists()` 확인 후 `open()` 하게 되고,
        # 그 사이에 파일이 사라지면 `FileNotFoundError`로 죽는다.
        #
        # 재진입 락인 이유는 `append()` → `_enforce_disk_cap()` 처럼 안에서 다시
        # 잡는 경로가 있기 때문이다.
        #
        # ⚠️ 이 락을 네트워크 전송 구간까지 넓히면 안 된다. `WalShipper.pump()`는
        #    `cursor.read()`(락 안)와 `transport.send()`(락 밖)를 분리해서 쓴다.
        self._lock = threading.RLock()

        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.segment_bytes = segment_bytes
        self.commit_interval_s = commit_interval_s
        self.commit_bytes = commit_bytes
        self.max_disk_bytes = max_disk_bytes
        self.boot_id = self._resolve_boot_id()

        self._fh = None
        self._seg_start_seq = 0
        self._seg_bytes = 0
        self._unsynced = 0
        self._last_sync = time.monotonic()
        self._fsync_count = 0
        self._appended = 0
        self._dropped = self._recover_dropped()
        self._apply_dropped_ranges()

        self._next_seq = self._recover()
        self._open_segment(self._next_seq)

    def _resolve_boot_id(self) -> str:
        """이 WAL 인스턴스의 생애 식별자. 로그를 지우면 바뀐다.

        `seq`는 이 WAL이 발급하므로 로그가 사라지면 0부터 다시 시작한다. 하류 dedup이
        `last_seen`만 들고 있으면 리셋 이후 모든 레코드를 "이미 봤다"고 버려 **전량
        유실**이 된다. `boot_id`가 달라진 것을 보고 상태를 리셋해야 한다.

        따라서 dedup 키는 `seq`가 아니라 **(vehicle_id, boot_id, seq)** 다.
        """
        path = self.root / BOOT_FILE
        if path.exists():
            existing = path.read_text().strip()
            if existing:
                return existing
        boot_id = uuid.uuid4().hex
        tmp = self.root / (BOOT_FILE + ".tmp")
        tmp.write_text(boot_id)
        with open(tmp, "rb") as f:
            os.fsync(f.fileno())
        tmp.replace(path)
        return boot_id

    # ── 쓰기 ────────────────────────────────────────────────────────────

    def append(self, payload: bytes, kind: int = KIND_SIGNAL) -> int:
        """레코드를 추가하고 발급된 `seq`를 돌려준다.

        **센서 콜백을 막지 않는다** — 버퍼에 쓰고 조건이 되면 fsync한다. fsync를 매번 하면
        초당 1,295회가 되어 eMMC가 버티지 못한다(설계 §3.3).
        """
        with self._lock:
            seq = self._next_seq
            body = struct.pack("<QB", seq, kind) + payload
            crc = zlib.crc32(body) & 0xFFFFFFFF
            frame = HEADER.pack(len(payload), crc, seq, kind) + payload

            assert self._fh is not None
            self._fh.write(frame)
            self._seg_bytes += len(frame)
            self._unsynced += len(frame)
            self._next_seq = seq + 1
            self._appended += 1

            if self._seg_bytes >= self.segment_bytes:
                self._sync()
                self._fh.close()
                self._open_segment(self._next_seq)
            elif self._should_sync():
                self._sync()

            if self.max_disk_bytes is not None:
                self._enforce_disk_cap()
            return seq

    def _should_sync(self) -> bool:
        return (
            self._unsynced >= self.commit_bytes
            or (time.monotonic() - self._last_sync) >= self.commit_interval_s
        )

    def _sync(self) -> None:
        if self._fh is None or self._unsynced == 0:
            return
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._unsynced = 0
        self._last_sync = time.monotonic()
        self._fsync_count += 1

    def flush(self) -> None:
        """미동기화분을 강제로 durable하게 만든다. 정상 종료 경로에서 호출한다."""
        self._sync()

    # ── 읽기 ────────────────────────────────────────────────────────────

    def read_from(self, seq: int) -> Iterator[WalRecord]:
        """`seq` 이상인 레코드를 순서대로 돌려준다.

        손상된 꼬리를 만나면 **거기서 멈춘다**(예외를 던지지 않는다) — 크래시 시점의
        잘린 레코드는 정상적인 종료 조건이다.

        스캔 전에 쓰기 버퍼를 비운다(fsync는 아니다). 이게 없으면 방금 append한 레코드가
        보이지 않아 **배송기가 꼬리를 빠뜨린다** — 조용히 유실처럼 보이는 버그다.
        """
        if self._fh is not None:
            self._fh.flush()
        for path in self._segments():
            for rec in _scan_segment(path):
                if rec.seq >= seq:
                    yield rec

    # ── 커밋 ────────────────────────────────────────────────────────────

    def commit(self, seq: int) -> None:
        """서버가 확인한 최고 `seq`를 반영하고 소비된 세그먼트를 회수한다.

        정밀할 필요가 없다 — 뒤처지면 재시작 후 재전송이 생기고, 그건 중복이며
        하류 dedup이 흡수한다.
        """
        with self._lock:
            tmp = self.root / (COMMIT_FILE + ".tmp")
            tmp.write_bytes(struct.pack("<Q", seq))
            with open(tmp, "rb") as f:
                os.fsync(f.fileno())
            tmp.replace(self.root / COMMIT_FILE)  # 원자적 교체
            self._truncate_before(seq)

    @property
    def next_seq(self) -> int:
        """다음에 발급될 `seq`. 재개 시 "이번에 새로 적재한 구간"의 시작점이다.

        복구 후 값이므로 빈 WAL이면 0, 기존 로그가 있으면 그 다음 번호다.
        """
        with self._lock:
            return self._next_seq

    @property
    def committed_seq(self) -> int:
        with self._lock:
            p = self.root / COMMIT_FILE
            if not p.exists():
                return -1
            raw = p.read_bytes()
            return struct.unpack("<Q", raw)[0] if len(raw) == 8 else -1

    # ── 복구 ────────────────────────────────────────────────────────────

    def _recover(self) -> int:
        """유효한 마지막 레코드를 찾아 다음 `seq`를 결정하고 손상 꼬리를 잘라낸다.

        커밋 포인터보다 뒤로 갈 수는 없다. 배송기는 아직 fsync되지 않은 레코드도 보낼 수
        있고(그건 안전하다 — 이미 Kafka에 있다), 그 ack으로 커밋 포인터가 durable 지점을
        앞지를 수 있다. 그때 `seq`를 재사용하면 **하류 dedup이 새 레코드를 중복으로 버린다**.
        유실보다 나쁜 실패다 — 결번이 없어서 탐지되지 않는다.
        """
        floor = max(self.committed_seq + 1, max((hi + 1 for _, hi in self._dropped), default=0))
        segments = self._segments()
        if not segments:
            return floor
        last = segments[-1]
        valid_end = 0
        last_seq = -1
        for rec, end in _scan_segment_with_offset(last):
            valid_end = end
            last_seq = rec.seq
        # 손상된 꼬리를 잘라낸다. 이게 W-6을 닫는다.
        if valid_end < last.stat().st_size:
            with open(last, "r+b") as f:
                f.truncate(valid_end)
                os.fsync(f.fileno())
        if last_seq >= 0:
            return max(last_seq + 1, floor)
        # 마지막 세그먼트가 통째로 손상이면 그 앞 세그먼트에서 이어받는다
        for path in reversed(segments[:-1]):
            seqs = [r.seq for r in _scan_segment(path)]
            if seqs:
                return max(seqs[-1] + 1, floor)
        return floor

    def _recover_dropped(self) -> list[tuple[int, int]]:
        """용량 제한으로 삭제한 seq 범위를 복구하고 찢어진 journal 꼬리를 제거한다."""
        path = self.root / DROPPED_FILE
        if not path.exists():
            return []
        raw = path.read_bytes()
        ranges: list[tuple[int, int]] = []
        valid_end = 0
        for offset in range(0, len(raw), DROPPED_RECORD.size):
            frame = raw[offset : offset + DROPPED_RECORD.size]
            if len(frame) != DROPPED_RECORD.size:
                break
            lo, hi, expected_crc = DROPPED_RECORD.unpack(frame)
            body = DROPPED_BODY.pack(lo, hi)
            if zlib.crc32(body) & 0xFFFFFFFF != expected_crc or lo > hi:
                break
            ranges.append((lo, hi))
            valid_end = offset + DROPPED_RECORD.size
        if valid_end != len(raw):
            with open(path, "r+b") as f:
                f.truncate(valid_end)
                os.fsync(f.fileno())
        return ranges

    def _record_dropped(self, lo: int, hi: int) -> None:
        """삭제 의도를 먼저 durable하게 기록한다. 재시작 시 삭제를 redo할 수 있다."""
        body = DROPPED_BODY.pack(lo, hi)
        frame = DROPPED_RECORD.pack(lo, hi, zlib.crc32(body) & 0xFFFFFFFF)
        with open(self.root / DROPPED_FILE, "ab") as f:
            f.write(frame)
            f.flush()
            os.fsync(f.fileno())
        # 최초 journal 생성의 디렉터리 엔트리도 삭제 대상 세그먼트보다 먼저 보존한다.
        directory_fd = os.open(self.root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        self._dropped.append((lo, hi))

    def _apply_dropped_ranges(self) -> None:
        """journal 기록 뒤 unlink 전에 죽은 경우 삭제 작업을 재실행한다."""
        if not self._dropped:
            return
        for path in self._segments():
            seqs = [r.seq for r in _scan_segment(path)]
            if not seqs:
                continue
            if any(lo <= seqs[0] and seqs[-1] <= hi for lo, hi in self._dropped):
                path.unlink()

    # ── 세그먼트 관리 ───────────────────────────────────────────────────

    def _open_segment(self, start_seq: int) -> None:
        self._seg_start_seq = start_seq
        self._seg_bytes = 0
        path = self.root / f"{start_seq:020d}{SEGMENT_SUFFIX}"
        self._fh = open(path, "ab")
        self._seg_bytes = path.stat().st_size

    def _segments(self) -> list[Path]:
        return sorted(self.root.glob(f"*{SEGMENT_SUFFIX}"))

    def _truncate_before(self, seq: int) -> None:
        """세그먼트를 **파일 통째로** 삭제해 회수한다(설계 §3.2).

        마지막 레코드의 `seq`가 커밋 지점 이하인 세그먼트만 지운다. 활성 세그먼트는
        건드리지 않는다.
        """
        segments = self._segments()
        for path in segments[:-1]:
            seqs = [r.seq for r in _scan_segment(path)]
            if seqs and seqs[-1] <= seq:
                path.unlink()

    def _enforce_disk_cap(self) -> None:
        assert self.max_disk_bytes is not None
        while self._disk_bytes() > self.max_disk_bytes:
            segments = self._segments()
            if len(segments) <= 1:
                return  # 활성 세그먼트는 못 버린다
            victim = segments[0]
            seqs = [r.seq for r in _scan_segment(victim)]
            if seqs:
                # journal fsync가 unlink보다 먼저다. 중간에 죽으면 복구에서 삭제를 redo한다.
                self._record_dropped(seqs[0], seqs[-1])
            victim.unlink()

    def _disk_bytes(self) -> int:
        return sum(p.stat().st_size for p in self._segments())

    # ── 관측 ────────────────────────────────────────────────────────────

    def cursor(self, seq: int) -> WalCursor:
        """`seq`부터 이어 읽는 커서. 반복 호출하는 배송 경로에서 쓴다."""
        return WalCursor(self, seq)

    def stats(self) -> WalStats:
        committed = self.committed_seq
        pending = [r for r in self.read_from(committed + 1)]
        return WalStats(
            appended=self._appended,
            committed_seq=committed,
            pending_records=len(pending),
            pending_bytes=sum(HEADER_SIZE + len(r.payload) for r in pending),
            segments=len(self._segments()),
            disk_bytes=self._disk_bytes(),
            fsync_count=self._fsync_count,
            dropped_seq_ranges=list(self._dropped),
        )

    def close(self) -> None:
        self._sync()
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def __enter__(self) -> "Wal":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


# ── 스캔 유틸 ───────────────────────────────────────────────────────────


class WalCursor:
    """재개 가능한 리더. **배송기는 이걸 써야 한다.**

    :meth:`Wal.read_from` 은 호출마다 세그먼트를 처음부터 다시 읽는다(`read_bytes()`로
    통째로). 배송기는 배치마다 이걸 부르므로 세그먼트 하나를 배송하는 동안 같은 4 MB를
    수십 번 다시 읽는다 — **2차식**이다. 실측으로 처리량이 85,836 rec/s에서
    16,587 rec/s로 떨어졌다.

    커서는 `(세그먼트, 오프셋)`을 들고 있어 이어서 읽는다. 파일 핸들은 붙들지 않고 매번
    경로로 다시 연다 — 커밋이 세그먼트를 **삭제**하므로 열어둔 핸들은 지워진 inode를
    가리킬 수 있다.
    """

    def __init__(self, wal: "Wal", seq: int) -> None:
        self._wal = wal
        self._seq = seq
        self._path: Path | None = None
        self._offset = 0

    def read(self, limit: int) -> list[WalRecord]:
        """최대 `limit`개를 이어서 읽는다. 더 없으면 빈 리스트.

        WAL 락 안에서 돈다 — ack 스레드의 `commit()`이 세그먼트를 unlink 하는 것과
        직렬화되어야 `exists()` 직후 파일이 사라지는 창이 닫힌다. 전송은 호출부
        (`WalShipper.pump`)가 이 메서드 **바깥에서** 하므로 네트워크가 락을 물지 않는다.
        """
        with self._wal._lock:
            if self._wal._fh is not None:
                self._wal._fh.flush()  # 방금 append한 꼬리를 보이게 한다
            out: list[WalRecord] = []
            while len(out) < limit:
                if self._path is None and not self._locate():
                    break
                if self._drain(limit - len(out), out) == 0:
                    nxt = self._next_segment()
                    if nxt is None:
                        break
                    self._path, self._offset = nxt, 0
            return out

    def _locate(self) -> bool:
        segments = self._wal._segments()
        if not segments:
            return False
        target = segments[0]
        for path in segments:
            if _segment_start(path) <= self._seq:
                target = path
            else:
                break
        self._path = target
        self._offset = 0
        # 세그먼트 안에서 시작 지점을 찾는다. 이 한 번만 처음부터 훑는다.
        for rec, end in _scan_segment_with_offset(target):
            if rec.seq >= self._seq:
                break
            self._offset = end
        return True

    def _next_segment(self) -> Path | None:
        if self._path is None:
            return None
        cur = _segment_start(self._path)
        for path in self._wal._segments():
            if _segment_start(path) > cur:
                return path
        return None

    def _drain(self, limit: int, out: list[WalRecord]) -> int:
        if self._path is None or not self._path.exists():
            return 0
        n = 0
        with open(self._path, "rb") as f:
            f.seek(self._offset)
            while n < limit:
                head = f.read(HEADER_SIZE)
                if len(head) < HEADER_SIZE:
                    break
                length, crc, seq, kind = HEADER.unpack(head)
                payload = f.read(length)
                if len(payload) < length:
                    break  # 잘린 꼬리
                if zlib.crc32(struct.pack("<QB", seq, kind) + payload) & 0xFFFFFFFF != crc:
                    break  # 손상된 꼬리
                self._offset = f.tell()
                if seq >= self._seq:
                    out.append(WalRecord(seq=seq, kind=kind, payload=payload))
                    n += 1
        return n


def _segment_start(path: Path) -> int:
    return int(path.name[: -len(SEGMENT_SUFFIX)])


def _scan_segment(path: Path) -> Iterator[WalRecord]:
    for rec, _ in _scan_segment_with_offset(path):
        yield rec


def _scan_segment_with_offset(path: Path) -> Iterator[tuple[WalRecord, int]]:
    """레코드와 **그 레코드가 끝나는 오프셋**을 함께 돌려준다.

    오프셋이 필요한 이유는 복구 시 손상 꼬리를 잘라낼 지점을 알아야 하기 때문이다.
    CRC 실패·길이 초과는 예외가 아니라 **종료 조건**으로 취급한다.
    """
    data = path.read_bytes()
    pos = 0
    n = len(data)
    while pos + HEADER_SIZE <= n:
        length, crc, seq, kind = HEADER.unpack_from(data, pos)
        end = pos + HEADER_SIZE + length
        if end > n:
            return  # 잘린 꼬리
        payload = data[pos + HEADER_SIZE : end]
        if zlib.crc32(struct.pack("<QB", seq, kind) + payload) & 0xFFFFFFFF != crc:
            return  # 손상된 꼬리
        yield WalRecord(seq=seq, kind=kind, payload=payload), end
        pos = end
