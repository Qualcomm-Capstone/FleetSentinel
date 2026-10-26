"""기존 gRPC WAL의 영속화·재시작·용량 제한·손상 꼬리 복구 검증.

핵심은 `test_survives_sigkill_without_gaps` 다. 임의 시점에 프로세스를 강제 종료하고
재시작해 `seq` 결번이 없는지 확인한다. 중복은 허용한다 — WAL은 at-least-once면 충분하고
하류 dedup이 흡수한다.
"""

from __future__ import annotations

import os
import signal
import struct
import subprocess
import sys
import time
import zlib
from pathlib import Path

import pytest

from fleetsentinel_ingest.wal import (
    HEADER,
    HEADER_SIZE,
    KIND_SEGMENT_REF,
    KIND_SIGNAL,
    Wal,
    _scan_segment,
)


def test_append_and_read_roundtrip(tmp_path: Path) -> None:
    with Wal(tmp_path) as wal:
        seqs = [wal.append(f"rec-{i}".encode()) for i in range(50)]
        wal.flush()
    assert seqs == list(range(50))
    with Wal(tmp_path) as wal:
        got = [r.payload.decode() for r in wal.read_from(0)]
    assert got == [f"rec-{i}" for i in range(50)]


def test_seq_is_monotonic_across_restart(tmp_path: Path) -> None:
    """W-2 — 재시작 후 `seq`가 이어져야 한다. 겹치면 유실 탐지가 깨진다."""
    with Wal(tmp_path) as wal:
        for i in range(10):
            wal.append(f"a{i}".encode())
    with Wal(tmp_path) as wal:
        second = [wal.append(f"b{i}".encode()) for i in range(5)]
    assert second == list(range(10, 15))
    with Wal(tmp_path) as wal:
        all_seqs = [r.seq for r in wal.read_from(0)]
    assert all_seqs == list(range(15))


def test_kind_distinguishes_signal_and_segment_ref(tmp_path: Path) -> None:
    """§3.9 — 경량 레코드와 segment-ref를 한 로그에 담는다(outbox 겸용)."""
    with Wal(tmp_path) as wal:
        wal.append(b"signal", KIND_SIGNAL)
        wal.append(b"s3://bucket/scene.mcap", KIND_SEGMENT_REF)
        wal.flush()
    with Wal(tmp_path) as wal:
        recs = list(wal.read_from(0))
    assert [r.kind for r in recs] == [KIND_SIGNAL, KIND_SEGMENT_REF]


def test_torn_tail_is_truncated_not_raised(tmp_path: Path) -> None:
    """W-6 — 잘린 꼬리는 오류가 아니라 종료 조건이다."""
    with Wal(tmp_path) as wal:
        for i in range(20):
            wal.append(f"r{i}".encode())
        wal.flush()
    seg = sorted(tmp_path.glob("*.seg"))[-1]
    original = seg.stat().st_size
    # 마지막 레코드를 중간에서 자른다 = 쓰기 도중 크래시
    with open(seg, "r+b") as f:
        f.truncate(original - 3)

    with Wal(tmp_path) as wal:            # 복구가 예외 없이 끝나야 한다
        recs = list(wal.read_from(0))
    assert len(recs) == 19                # 마지막 하나만 사라진다
    assert [r.seq for r in recs] == list(range(19))
    assert seg.stat().st_size < original  # 잘라낸 흔적


def test_corrupted_payload_is_treated_as_tail(tmp_path: Path) -> None:
    """CRC 불일치도 꼬리로 취급한다 — 비트 반전이 조용히 통과하면 안 된다."""
    with Wal(tmp_path) as wal:
        for i in range(10):
            wal.append(f"r{i}".encode())
        wal.flush()
    seg = sorted(tmp_path.glob("*.seg"))[-1]
    data = bytearray(seg.read_bytes())
    # 5번째 레코드 페이로드의 한 바이트를 뒤집는다
    off = (HEADER_SIZE + 2) * 5 + HEADER_SIZE
    data[off] ^= 0xFF
    seg.write_bytes(bytes(data))

    with Wal(tmp_path) as wal:
        recs = list(wal.read_from(0))
    assert [r.seq for r in recs] == list(range(5))  # 손상 지점에서 멈춘다


def test_commit_reclaims_segments(tmp_path: Path) -> None:
    """W-3 — 커밋된 세그먼트는 파일 통째로 삭제된다."""
    wal = Wal(tmp_path, segment_bytes=256)
    for i in range(200):
        wal.append(f"record-{i:04d}".encode())
    wal.flush()
    before = len(list(tmp_path.glob("*.seg")))
    assert before > 3, "세그먼트가 여러 개로 롤링돼야 한다"

    wal.commit(150)
    after = len(list(tmp_path.glob("*.seg")))
    assert after < before
    # 커밋 이후 레코드는 살아 있어야 한다
    remaining = [r.seq for r in wal.read_from(151)]
    assert remaining and min(remaining) >= 151
    wal.close()


def test_disk_cap_drops_oldest_and_records_the_gap(tmp_path: Path) -> None:
    """§3.6 — 상한 초과 시 오래된 것을 버리되 **버린 구간을 기록**한다.

    쓰기를 막으면 센서 계층에서 조용히 사라지지만, 버리는 쪽은 탐지 가능하다.
    """
    wal = Wal(tmp_path, segment_bytes=256, max_disk_bytes=1024)
    for i in range(300):
        wal.append(f"record-{i:04d}".encode())
    wal.flush()
    stats = wal.stats()
    assert stats.disk_bytes <= 1024 * 2  # 상한 근처로 유지
    assert stats.dropped_seq_ranges, "버린 구간이 기록돼야 한다"
    dropped_hi = max(hi for _, hi in stats.dropped_seq_ranges)
    assert dropped_hi < 299  # 최신은 남는다
    wal.close()

    with Wal(tmp_path, segment_bytes=256, max_disk_bytes=1024) as reopened:
        assert reopened.stats().dropped_seq_ranges == stats.dropped_seq_ranges


def test_dropped_journal_recovers_from_torn_tail(tmp_path: Path) -> None:
    with Wal(tmp_path, segment_bytes=256, max_disk_bytes=1024) as wal:
        for i in range(300):
            wal.append(f"record-{i:04d}".encode())
        expected = wal.stats().dropped_seq_ranges
    journal = tmp_path / "DROPPED"
    valid_size = journal.stat().st_size
    with journal.open("ab") as f:
        f.write(b"torn")

    with Wal(tmp_path, segment_bytes=256, max_disk_bytes=1024) as reopened:
        assert reopened.stats().dropped_seq_ranges == expected
    assert journal.stat().st_size == valid_size


def test_dropped_journal_redoes_unlink_after_crash_boundary(tmp_path: Path) -> None:
    wal = Wal(tmp_path, segment_bytes=128)
    for i in range(100):
        wal.append(f"record-{i:04d}".encode())
    wal.flush()
    victim = wal._segments()[0]
    seqs = [r.seq for r in _scan_segment(victim)]
    wal._record_dropped(seqs[0], seqs[-1])
    wal.close()
    assert victim.exists(), "journal과 unlink 사이의 crash 경계를 만든다"

    with Wal(tmp_path, segment_bytes=128) as reopened:
        assert not victim.exists()
        assert reopened.stats().dropped_seq_ranges == [(seqs[0], seqs[-1])]


def test_recovery_does_not_reuse_sequences_from_discarded_segments(tmp_path: Path) -> None:
    wal = Wal(tmp_path, segment_bytes=128)
    for i in range(10):
        wal.append(f"record-{i:04d}".encode())
    wal.flush()
    # Deletion intent covers all retained records at this crash boundary.
    wal._record_dropped(0, 9)
    wal.close()

    with Wal(tmp_path, segment_bytes=128) as reopened:
        assert reopened.append(b"new") == 10


def test_stats_reports_pending_backlog(tmp_path: Path) -> None:
    """§3.7 — 적체량이 곧 '네트워크가 못 따라간다'의 지표다."""
    with Wal(tmp_path) as wal:
        for i in range(30):
            wal.append(f"r{i}".encode())
        wal.flush()
        wal.commit(9)
        st = wal.stats()
    assert st.committed_seq == 9
    assert st.pending_records == 20      # 10..29
    assert st.pending_bytes > 0
    assert st.fsync_count > 0


def test_group_commit_reduces_fsync_count(tmp_path: Path) -> None:
    """§3.3 — 레코드마다 fsync하지 않는다. 그게 이 설계의 전제다."""
    with Wal(tmp_path, commit_bytes=10_000, commit_interval_s=10.0) as wal:
        for i in range(500):
            wal.append(b"x" * 20)
        n = wal.stats().fsync_count
    assert n < 20, f"500 레코드에 fsync {n}회 — 그룹 커밋이 동작하지 않는다"


# ── SIGKILL 검증 (핵심) ──────────────────────────────────────────────────

_WRITER = """
import sys, time
sys.path.insert(0, {root!r})
from pathlib import Path
from fleetsentinel_ingest.wal import Wal
wal = Wal(Path({wal!r}), segment_bytes=8192, commit_interval_s=0.005)
i = 0
while True:
    wal.append(f"payload-{{i:06d}}".encode())
    i += 1
    if i % 50 == 0:
        time.sleep(0.001)
"""


def test_survives_sigkill_without_gaps(tmp_path: Path) -> None:
    """**W-1·W-2 핵심 검증** — 강제 종료 후 재개했을 때 `seq` 결번이 없어야 한다.

    중복은 허용한다(WAL은 at-least-once). 하지만 **결번은 유실**이고, 그러면
    "유실 없음" 주장이 무너진다.

    fsync 이후 구간만 보장 대상이다. 커널 페이지 캐시에만 있던 미동기화분은
    SIGKILL에서는 살아남지만(커널이 들고 있다) 전원 손실에서는 사라진다 —
    설계 문서 P-W1·P-W2가 그 구분을 다룬다.
    """
    wal_dir = tmp_path / "wal"
    src = _WRITER.format(root=str(Path(__file__).resolve().parent.parent), wal=str(wal_dir))
    script = tmp_path / "writer.py"
    script.write_text(src)

    proc = subprocess.Popen([sys.executable, str(script)])
    time.sleep(1.2)                       # 충분히 쓰게 둔다
    os.kill(proc.pid, signal.SIGKILL)     # 정리 기회를 주지 않는다
    proc.wait(timeout=5)
    assert proc.returncode != 0

    # 재시작: 복구가 예외 없이 끝나고 seq가 연속이어야 한다
    with Wal(wal_dir) as wal:
        recs = list(wal.read_from(0))
        resumed = wal.append(b"after-restart")

    assert len(recs) > 100, f"쓰인 레코드가 너무 적다: {len(recs)}"
    seqs = [r.seq for r in recs]
    assert seqs == list(range(len(seqs))), "seq에 결번이 있다 = 유실"
    assert resumed == len(seqs), "재시작 후 seq가 이어지지 않는다"

    # 페이로드도 온전해야 한다 (CRC가 통과했으니 당연하지만 명시적으로)
    for r in recs:
        assert r.payload == f"payload-{r.seq:06d}".encode()


def test_sigkill_during_segment_roll(tmp_path: Path) -> None:
    """세그먼트 경계에서 죽어도 복구돼야 한다 — 경계가 가장 취약한 지점이다."""
    wal_dir = tmp_path / "wal"
    src = _WRITER.format(root=str(Path(__file__).resolve().parent.parent), wal=str(wal_dir))
    script = tmp_path / "writer.py"
    script.write_text(src)
    proc = subprocess.Popen([sys.executable, str(script)])
    time.sleep(2.0)                       # 세그먼트가 여러 번 롤링될 시간
    os.kill(proc.pid, signal.SIGKILL)
    proc.wait(timeout=5)

    assert len(list(wal_dir.glob("*.seg"))) > 1, "세그먼트가 롤링돼야 한다"
    with Wal(wal_dir) as wal:
        seqs = [r.seq for r in wal.read_from(0)]
    assert seqs == list(range(len(seqs))), "세그먼트 경계에서 결번이 생겼다"


def test_seq_is_never_reused_when_commit_outruns_durable_data(tmp_path):
    """커밋 포인터가 durable 지점을 앞지르면 그 위에서 이어써야 한다.

    배송기는 아직 fsync되지 않은 레코드도 보낼 수 있고, 그 ack으로 커밋 포인터가
    durable 데이터를 앞지를 수 있다. 그때 `seq`를 재사용하면 하류 dedup이 새 레코드를
    중복으로 버린다 — **결번이 없어서 탐지되지 않는 유실**이므로 유실보다 나쁘다.
    """
    root = tmp_path / "wal"
    with Wal(root) as wal:
        for i in range(100):
            wal.append(f"r{i}".encode())
    # 게이트웨이가 500까지 ack했다고 가정 — 세그먼트에는 99까지만 있다
    (root / "COMMIT").write_bytes(struct.pack("<Q", 500))

    with Wal(root) as wal:
        assert wal.committed_seq == 500
        assert wal.append(b"next") == 501, "seq를 재사용하면 안 된다"


def test_read_from_sees_records_appended_without_fsync(tmp_path):
    """append 직후 fsync 없이도 읽을 수 있어야 한다.

    안 되면 배송기가 꼬리를 빠뜨리고, 그건 조용한 유실처럼 보인다.
    """
    with Wal(tmp_path / "wal", commit_bytes=1 << 30, commit_interval_s=1e9) as wal:
        for i in range(100):
            wal.append(f"r{i}".encode())
        assert wal.stats().fsync_count == 0, "이 테스트는 fsync가 없는 상태를 봐야 한다"
        assert [r.seq for r in wal.read_from(0)] == list(range(100))
