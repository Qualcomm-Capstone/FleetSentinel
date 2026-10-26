"""차량 → 게이트웨이 gRPC 전송. :class:`~.shipping.Transport` 의 실제 구현이다.

`LoopbackGateway`가 인프로세스 파일 쓰기로 대신하던 자리를 실제 네트워크로 채운다.
바뀌는 것은 전송 계층뿐이고 :class:`~.shipping.WalShipper` 의 계약은 그대로다 — WAL을 읽어
보내고, ack이 오면 커밋을 전진시킨다.

## mTLS와 신원

게이트웨이는 클라이언트 인증서를 **요구**하고, `vehicle_id`를 인증서 SAN URI에서만
취한다([SDD](../../docs/sdd.md) S-9). 그래서 여기서 `vehicle_id`를 "설정"하는 것은
사실상 **어느 인증서를 쓸지 고르는 것**이다. 메타데이터로 보내는 `x-vehicle-id`는 주장일
뿐이고, 인증서와 다르면 게이트웨이가 `PERMISSION_DENIED`로 끊는다.

주장을 굳이 보내는 이유는 불일치를 관측 가능하게 만들기 위해서다. 인증서를 잘못 심은
차량은 조용히 남의 ID로 쓰는 대신 즉시 거절당한다.

## ack 수신

게이트웨이의 CACK은 **역방향 스트림**으로 온다. 별도 스레드가 그것을 읽어
:meth:`~.shipping.WalShipper.on_ack` 를 부르고, 그러면 WAL 세그먼트가 회수된다.
즉 **ack이 디스크 회수를 구동한다** — 이 스레드가 멈추면 WAL이 자란다.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterator, Optional

import grpc

from .proto import ingest_pb2, ingest_pb2_grpc
from .wal import KIND_SEGMENT_REF, KIND_SIGNAL

__all__ = [
    "GrpcTransport",
    "TransportStats",
    "KIND_PERCEPTION",
    "load_credentials",
]

#: WAL의 kind 바이트. `wal.py`가 0·1만 정의하므로 객체 메타데이터 계층을 여기서 넓힌다.
#: 기존 WAL 파일과의 호환성을 위해 kind 값을 바꾸지 않는다.
KIND_PERCEPTION = 2

#: WAL kind → proto enum. proto3는 enum 0을 UNSPECIFIED로 예약하므로 값이 어긋난다.
#: 매핑을 한 곳에만 둔다.
_KIND_TO_WIRE = {
    KIND_SIGNAL: ingest_pb2.RECORD_KIND_SIGNAL,
    KIND_SEGMENT_REF: ingest_pb2.RECORD_KIND_SEGMENT_REF,
    KIND_PERCEPTION: ingest_pb2.RECORD_KIND_PERCEPTION,
}

#: 전송 큐 상한. 이 값에 닿으면 `send()`가 블록되고, 역압이 WalShipper까지 전파된다.
#: WalShipper의 `max_inflight`가 먼저 걸리는 것이 정상이고, 이건 마지막 방어선이다.
DEFAULT_QUEUE_SIZE = 8192

@dataclass(slots=True)
class TransportStats:
    sent: int = 0
    wire_sent: int = 0
    acks_received: int = 0
    last_ack: int = -1
    reconnect_attempts: int = 0
    reconnects: int = 0
    #: 게이트웨이가 스트림을 끊은 이유. 정상 종료면 None
    terminal_error: Optional[str] = field(default=None)


def load_credentials(pki_dir: Path | str, vehicle_id: str) -> grpc.ChannelCredentials:
    """`scripts/gen-certs.sh`가 구운 PKI에서 이 차량의 자격증명을 만든다.

    :param pki_dir: `gen-certs.sh`의 `PKI_DIR`
    :param vehicle_id: 인증서 디렉터리 이름이자 SAN URI에 박힌 ID
    """
    pki = Path(pki_dir)
    vehicle_dir = pki / "vehicles" / vehicle_id
    return grpc.ssl_channel_credentials(
        # 게이트웨이 서버 인증서를 검증할 CA. 공개 CA 신뢰 목록을 쓰지 않는다 —
        # 우리 CA가 서명한 게이트웨이에만 붙는다.
        root_certificates=(pki / "ca" / "ca.crt").read_bytes(),
        private_key=(vehicle_dir / f"{vehicle_id}.key").read_bytes(),
        certificate_chain=(vehicle_dir / f"{vehicle_id}.crt").read_bytes(),
    )


class GrpcTransport:
    """WAL 레코드를 게이트웨이로 보내고 CACK을 되돌려준다.

    :class:`~.shipping.Transport` 프로토콜을 만족하므로
    :class:`~.shipping.WalShipper` 에 그대로 넣을 수 있다.

    :param target: `host:port`
    :param credentials: :func:`load_credentials` 결과
    :param vehicle_id: 주장할 ID. 인증서와 달라야 할 이유가 없다면 인증서와 같게 둔다
    :param on_ack: CACK 콜백. 보통 `shipper.on_ack`. :class:`~.shipping.WalShipper` 가
        생성자에 transport를 요구하므로 순환이 생긴다 — 그때는 생략하고
        :meth:`set_ack_handler` 로 나중에 단다
    :param server_name: 서버 인증서 CN/SAN과 대조할 이름. 개발에서 IP로 붙을 때 필요
    """

    def __init__(
        self,
        target: str,
        credentials: grpc.ChannelCredentials,
        vehicle_id: str,
        on_ack: Optional[Callable[[int], None]] = None,
        *,
        server_name: Optional[str] = None,
        queue_size: int = DEFAULT_QUEUE_SIZE,
    ) -> None:
        if queue_size <= 0:
            raise ValueError("queue_size must be positive")
        options = []
        if server_name:
            options.append(("grpc.ssl_target_name_override", server_name))

        self._channel = grpc.secure_channel(target, credentials, options=options)
        self._stub = ingest_pb2_grpc.IngestStub(self._channel)
        self._target = target
        self._credentials = credentials
        self._options = options
        self._vehicle_id = vehicle_id
        self._on_ack: Optional[Callable[[int], None]] = on_ack
        self._queue_size = queue_size
        self._pending: OrderedDict[int, ingest_pb2.IngestRecord] = OrderedDict()
        self._condition = threading.Condition()
        self._stats = TransportStats()
        self._ack_thread: Optional[threading.Thread] = None
        self._started = False
        self._closed = threading.Event()
        self._boot_id: Optional[str] = None
        self._stream_generation = 0
        # 우리가 부른 close()로 채널이 닫히면 ack 리더가 CANCELLED를 본다. 그걸
        # terminal_error로 기록하면 **진짜 원인을 덮는다** — 호출부가 stall을 감지해
        # 닫았는데 보고는 "Channel closed"가 되어버린다.
        self._closing = False

    @property
    def stats(self) -> TransportStats:
        return self._stats

    def set_ack_handler(self, on_ack: Callable[[int], None]) -> None:
        """CACK 콜백을 나중에 단다.

        :class:`~.shipping.WalShipper` 는 생성자에 transport를 받고, transport는 ack을
        shipper에 돌려줘야 하므로 순환이 생긴다. 스트림을 열기 전이면 언제 달아도 된다.
        """
        if self._started:
            raise RuntimeError("스트림이 이미 열렸다 — ack 핸들러를 바꿀 수 없다")
        self._on_ack = on_ack

    def start(self, boot_id: str) -> None:
        """스트림을 연다. `boot_id`는 이 WAL 세션의 것이다.

        게이트웨이가 stateless라 재개 협상이 없다 — 어디서부터 보낼지는 이미
        :class:`~.shipping.WalShipper` 가 `committed_seq + 1`로 정했다.
        """
        if self._started:
            raise RuntimeError("이미 시작된 전송이다")
        if self._on_ack is None:
            # ack을 버리면 WAL 커밋이 영영 전진하지 않고 디스크가 찬다. 조용히 두지 않는다.
            raise RuntimeError("ack 핸들러가 없다 — set_ack_handler()를 먼저 부른다")
        self._started = True
        self._boot_id = boot_id

        # 스트림 종료는 channel 연결 복구와 별개다. 감독 스레드가 새 RPC를 만들고,
        # CACK되지 않은 pending 레코드를 다시 흘려 at-least-once를 유지한다.
        self._ack_thread = threading.Thread(
            target=self._supervise_stream, name="cack-reader", daemon=True
        )
        self._ack_thread.start()

    def send(self, boot_id: str, seq: int, kind: int, payload: bytes) -> None:
        """:class:`~.shipping.Transport` 구현. 큐가 차면 블록된다(역압)."""
        wire_kind = _KIND_TO_WIRE.get(kind)
        if wire_kind is None:
            # 조용히 UNSPECIFIED로 보내면 게이트웨이가 거절하고 그 레코드만 사라진다.
            # 스트림을 열기 전에 검증해 잘못된 입력이 네트워크 상태를 만들지 않게 한다.
            raise ValueError(f"매핑되지 않은 WAL kind: {kind}")
        if not self._started:
            self.start(boot_id)
        elif self._boot_id is not None and boot_id != self._boot_id:
            raise RuntimeError("하나의 transport에서 boot_id를 바꿀 수 없다")

        record = ingest_pb2.IngestRecord(seq=seq, kind=wire_kind, payload=payload)
        with self._condition:
            while len(self._pending) >= self._queue_size and not self._closed.is_set():
                self._condition.wait(timeout=0.1)
            if self._closed.is_set():
                raise RuntimeError("닫힌 transport에는 전송할 수 없다")
            self._pending[seq] = record
            self._condition.notify_all()
        self._stats.sent += 1

    def _drain_pending(self, generation: int) -> Iterator[ingest_pb2.IngestRecord]:
        """현재 RPC에서 pending 레코드를 순서대로 한 번씩 흘린다.

        새 RPC는 로컬 cursor를 처음부터 다시 시작하므로 CACK되지 않은 레코드가 재전송된다.
        이전 RPC의 request iterator는 generation이 바뀌면 즉시 종료한다.
        """
        sent: set[int] = set()
        while not self._closed.is_set():
            with self._condition:
                if generation != self._stream_generation:
                    return
                # CACK으로 pending에서 제거된 seq는 local set에서도 버려 크기를 제한한다.
                sent.intersection_update(self._pending)
                item = next(
                    ((seq, rec) for seq, rec in self._pending.items() if seq not in sent),
                    None,
                )
                if item is None:
                    self._condition.wait(timeout=0.1)
                    continue
            seq, record = item
            sent.add(seq)
            self._stats.wire_sent += 1
            yield record

    def _recreate_channel(self) -> None:
        self._channel.close()
        self._channel = grpc.secure_channel(
            self._target, self._credentials, options=self._options
        )
        self._stub = ingest_pb2_grpc.IngestStub(self._channel)

    def _supervise_stream(self) -> None:
        backoff = 0.1
        first = True
        while not self._closing:
            if not first:
                self._stats.reconnect_attempts += 1
                if self._closed.wait(backoff):
                    return
                self._recreate_channel()
                backoff = min(backoff * 2, 2.0)
            first = False
            with self._condition:
                self._stream_generation += 1
                generation = self._stream_generation
                self._condition.notify_all()
            assert self._boot_id is not None
            metadata = (
                ("x-vehicle-id", self._vehicle_id),
                ("x-boot-id", self._boot_id),
            )
            try:
                responses = self._stub.Stream(
                    self._drain_pending(generation),
                    metadata=metadata,
                    wait_for_ready=True,
                )
            except grpc.RpcError as e:
                if not self._closing:
                    self._stats.terminal_error = f"{e.code().name}: {e.details()}"
                continue
            if self._pump_acks(responses):
                return
            if self._stats.last_ack >= 0:
                self._stats.reconnects += 1
                backoff = 0.1

    def _pump_acks(self, responses) -> bool:
        try:
            for ack in responses:
                # 콜백이 실패하면 pending을 보존해 다음 RPC에서도 재전송할 수 있게 한다.
                if self._on_ack is not None:
                    self._on_ack(ack.ack_seq)
                self._stats.acks_received += 1
                self._stats.last_ack = ack.ack_seq
                with self._condition:
                    for seq in list(self._pending):
                        if seq <= ack.ack_seq:
                            self._pending.pop(seq, None)
                    self._condition.notify_all()
            return self._closing
        except grpc.RpcError as e:
            # 셀룰러 단절은 정상이다. 재연결하면 committed+1부터 다시 보낸다.
            if not self._closing:
                self._stats.terminal_error = f"{e.code().name}: {e.details()}"
            return self._closing
        except Exception as e:  # noqa: BLE001
            # RpcError가 아닌 것으로 리더가 죽으면 ack이 영영 안 온다. 조용히 스레드만
            # 사라지면 호출부는 terminal_error=None을 보고 stall로 오해한다.
            if not self._closing:
                self._stats.terminal_error = f"ack 리더 비정상 종료: {e!r}"
            return self._closing

    def close(self, timeout: float = 30.0) -> None:
        """재연결을 중지하고 채널과 ACK 리더를 닫는다.

        아직 CACK되지 않은 레코드는 WAL에 남아 다음 전송기에서 재전송된다.
        채널을 즉시 닫으므로 graceful drain이나 마지막 ACK 수신을 보장하지 않는다.
        """
        if not self._started:
            self._closed.set()
            self._channel.close()
            return
        self._closing = True
        self._closed.set()
        with self._condition:
            self._stream_generation += 1
            self._condition.notify_all()
        self._channel.close()
        if self._ack_thread is not None:
            self._ack_thread.join(timeout=timeout)

    def __enter__(self) -> "GrpcTransport":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
