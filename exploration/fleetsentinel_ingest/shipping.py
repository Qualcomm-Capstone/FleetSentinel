"""ack 프로토콜 — WAL의 레코드를 게이트웨이로 보내고 커밋 지점을 전진시킨다.

## 무엇을 정해야 하는가

gRPC 양방향 스트림에는 애플리케이션 수준 ack이 없다. HTTP/2 흐름 제어는 **전송**만
보장하고 "게이트웨이가 Kafka에 안전하게 넣었다"는 말해주지 않는다. 그래서 직접 만든다.

| 결정 | 선택 | 근거 |
|---|---|---|
| ack 시점 | **Kafka 쓰기 성공(`acks=all`) 후** | 수신 직후 ack하면 게이트웨이 크래시에서 유실 |
| ack 내용 | **Cumulative Acknowledgement(CACK)** — 최고 연속 `seq` | 구간 목록보다 단순하고, 아래 이유로 정확히 성립 |
| CACK 주기 | `every_n` 개 **또는** `every_s` 초 중 먼저 | 개수만 두면 저부하에서 커밋이 멈춘다 |
| 재개 지점 | **클라이언트가 정한다** (`committed + 1`) | 게이트웨이 stateless 유지 — 아래 참조 |

## CACK이 정확히 성립하는 이유

Kafka producer 콜백은 **파티션별로 순서대로** 호출된다. 파티션 키가 `vehicle_id`이므로
한 차량의 레코드는 한 파티션에 가고, 따라서 완료 콜백도 보낸 순서대로 온다. 그래서
"연속으로 성공한 최고 `seq`"가 곧 "이 값 이하는 전부 안전하다"가 된다.

`enable.idempotence=true` 이면 `max.in.flight.requests.per.connection <= 5` 에서도
Kafka가 시퀀스 번호로 재정렬하므로 순서 보장이 유지된다. 즉 **처리량을 포기하지 않고**
CACK을 쓸 수 있다. 그래도 :class:`AckTracker` 는 순서 없는 완료를 받아낸다 —
이 보장에 의존하지 않는 편이 안전하고, 비용도 없다.

## 재개 지점을 클라이언트가 정하는 이유

기존 gRPC 경로는 차량의 WAL에 복구 지점을 보관한다. 게이트웨이는 활성 스트림의 ACK를
추적하되 재접속 간 공유 복구 상태를 두지 않는다.
([시스템 설계](../../docs/sdd.md) S-7).

그래서 클라이언트가 `wal.committed_seq + 1` 부터 재전송한다. 이미 Kafka에 들어갔지만 ack이
유실된 구간이 다시 온다. 하류 dedup(:mod:`.dedup`)은 유한 윈도우 안에서 중복을 제거한다.
재개 지점 협상을 위한 왕복은 필요하지 않다.

전송은 at-least-once이며, 저장·조회에서도 같은 식별자를 사용해 중복을 처리해야 한다.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Protocol

from .wal import Wal

__all__ = [
    "AckTracker",
    "Transport",
    "WalShipper",
    "ShipperStats",
    "DEFAULT_ACK_EVERY_N",
    "DEFAULT_ACK_EVERY_S",
]

#: ack 주기. 차량당 1,295 rec/s에서 128개 = 약 99ms → 차량당 10 ack/s.
#: 매 레코드 ack하면 500대에서 647k ack/s이고, 그건 낭비다.
DEFAULT_ACK_EVERY_N = 128
#: 저부하(정차 등)에서도 커밋이 전진하도록 하는 상한.
DEFAULT_ACK_EVERY_S = 0.2


class AckTracker:
    """게이트웨이 측. 순서 없이 완료되는 쓰기에서 CACK을 계산한다.

    :param start_seq: 이 스트림의 첫 `seq`. `ack_seq`는 `start_seq - 1`에서 시작한다
    :param every_n: 이만큼 전진하면 ack을 방출한다
    :param every_s: 이 시간이 지나면 전진량이 적어도 방출한다
    """

    def __init__(
        self,
        start_seq: int,
        *,
        every_n: int = DEFAULT_ACK_EVERY_N,
        every_s: float = DEFAULT_ACK_EVERY_S,
    ) -> None:
        self._ack = start_seq - 1
        self._done: set[int] = set()
        self.every_n = every_n
        self.every_s = every_s
        self._last_emitted = self._ack
        self._last_at = time.monotonic()
        self.max_pending = 0

    @property
    def ack_seq(self) -> int:
        """이 값 이하는 **전부** Kafka 쓰기가 성공했다."""
        return self._ack

    def pending(self) -> int:
        """완료됐지만 그 아래에 구멍이 있어 ack할 수 없는 레코드 수.

        이 값이 계속 자라면 어떤 레코드의 쓰기가 끝나지 않고 있다는 뜻이다.
        """
        return len(self._done)

    def complete(self, seq: int) -> None:
        """Kafka 쓰기 성공. 순서는 상관없다."""
        if seq <= self._ack:
            return  # 이미 ack한 구간의 중복 완료
        self._done.add(seq)
        while self._ack + 1 in self._done:
            self._ack += 1
            self._done.discard(self._ack)
        self.max_pending = max(self.max_pending, len(self._done))

    def take(self, now: float | None = None) -> int | None:
        """방출할 ack이 있으면 그 `seq`. 없으면 None.

        같은 값을 두 번 방출하지 않는다 — ack이 전진하지 않았다면 보낼 이유가 없다.
        """
        now = time.monotonic() if now is None else now
        if self._ack <= self._last_emitted:
            return None
        if self._ack - self._last_emitted >= self.every_n or now - self._last_at >= self.every_s:
            self._last_emitted = self._ack
            self._last_at = now
            return self._ack
        return None


class Transport(Protocol):
    """클라이언트 → 게이트웨이 단방향 전송. gRPC 스트림 자리."""

    def send(self, boot_id: str, seq: int, kind: int, payload: bytes) -> None: ...


@dataclass(slots=True)
class ShipperStats:
    sent: int = 0
    acked: int = 0
    commits: int = 0
    #: 전송했지만 ack을 못 받은 레코드 수. `max_inflight`에서 멈춘다
    inflight: int = 0


class WalShipper:
    """WAL을 읽어 전송하고 ack으로 커밋을 전진시킨다.

    커밋이 전진하면 WAL 세그먼트가 회수된다. 즉 **ack이 디스크 회수를 구동한다** —
    ack이 멈추면 WAL이 자라고, 상한에 닿으면 오래된 세그먼트를 버린다.
    유실이지만 `seq` 결번으로 탐지 가능한 유실이다.

    :param max_inflight: ack 없이 앞서 보낼 수 있는 최대 레코드 수. 역압 지점
    """

    def __init__(
        self,
        wal: Wal,
        transport: Transport,
        *,
        max_inflight: int = 1024,
    ) -> None:
        self.wal = wal
        self.transport = transport
        self.max_inflight = max_inflight
        # 재개 지점: 커밋된 다음. 이미 Kafka에 들어갔지만 ack이 유실된 구간이
        # 다시 갈 수 있다 — 의도된 at-least-once다.
        self._next = wal.committed_seq + 1
        self._resume_from = self._next
        self._cursor = wal.cursor(self._next)
        self.stats = ShipperStats()

    @property
    def resume_from(self) -> int:
        """이 스트림이 재전송을 시작한 `seq`. 첫 연결이면 0."""
        return self._resume_from

    def pump(self, budget: int | None = None) -> int:
        """보낼 수 있는 만큼 보내고 보낸 개수를 돌려준다.

        보낼 수 있는 상한은 `max_inflight - (미ack 레코드 수)` 다. ack이 오지 않으면
        창이 닫히고, 그 시점부터 WAL이 자란다 — 역압이 디스크로 흡수된다.
        """
        allowed = self.max_inflight - (self._next - 1 - self.wal.committed_seq)
        if allowed <= 0:
            return 0
        if budget is not None:
            allowed = min(allowed, budget)

        records = self._cursor.read(allowed)
        for rec in records:
            self.transport.send(self.wal.boot_id, rec.seq, rec.kind, rec.payload)
            self._next = rec.seq + 1
            self.stats.sent += 1
        self.stats.inflight = max(0, self._next - 1 - self.wal.committed_seq)
        return len(records)

    def on_ack(self, ack_seq: int) -> None:
        """게이트웨이의 CACK. **이 지점까지 WAL을 커밋해도 안전하다.**"""
        if ack_seq < self.wal.committed_seq:
            return  # 순서 뒤바뀐 ack — 커밋을 되돌리면 안 된다
        self.wal.commit(ack_seq)
        self.stats.acked = ack_seq
        self.stats.commits += 1
        self.stats.inflight = max(0, self._next - 1 - ack_seq)


# ── 테스트·실측용 게이트웨이 ─────────────────────────────────────────────


@dataclass(slots=True)
class LoopbackGateway:
    """게이트웨이 + Kafka 자리. 받은 레코드를 append-only 파일에 적는다.

    파일이 "Kafka에 들어간 것"이다. 클라이언트가 죽고 재개하면 이 파일에 중복이 생기고,
    :class:`~.dedup.SeqDedup` 을 통과시키면 정확히 한 번이 된다 — 그게 검증 대상이다.

    :param fail_write_from: 이 `seq` 이상은 쓰기가 실패한다고 가정한다. ack이 쓰기
        성공에만 의존하는지 확인하는 데 쓴다
    """

    log_path: object
    on_ack: Callable[[int], None]
    tracker: AckTracker | None = None
    every_n: int = DEFAULT_ACK_EVERY_N
    every_s: float = DEFAULT_ACK_EVERY_S
    fail_write_from: int | None = None
    received: int = 0
    written: int = 0
    _fh: object = field(default=None, repr=False)

    def send(self, boot_id: str, seq: int, kind: int, payload: bytes) -> None:
        if self._fh is None:
            self._fh = open(self.log_path, "a", buffering=1)
        if self.tracker is None:
            self.tracker = AckTracker(seq, every_n=self.every_n, every_s=self.every_s)
        self.received += 1

        if self.fail_write_from is not None and seq >= self.fail_write_from:
            return  # 쓰기 실패 → 완료 처리 안 함 → ack 전진 안 함

        # Kafka 쓰기 성공. 여기서만 완료로 표시한다.
        self._fh.write(f"{boot_id},{seq},{kind},{payload.hex()}\n")
        self.written += 1
        self.tracker.complete(seq)

        ack = self.tracker.take()
        if ack is not None:
            self.on_ack(ack)

    def flush_ack(self) -> None:
        """주기 조건을 무시하고 지금까지의 ack을 방출한다(스트림 종료 시)."""
        if self.tracker is None:
            return
        self.tracker.every_n = 1
        self.tracker.every_s = 0.0
        ack = self.tracker.take()
        if ack is not None:
            self.on_ack(ack)


def read_gateway_log(path: object) -> list[tuple[str, int, int, bytes]]:
    """게이트웨이 로그를 파싱한다. 중복이 그대로 들어있다."""
    out: list[tuple[str, int, int, bytes]] = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            boot_id, seq, kind, hexpayload = line.split(",", 3)
            out.append((boot_id, int(seq), int(kind), bytes.fromhex(hexpayload)))
    return out
