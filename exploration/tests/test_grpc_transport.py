"""gRPC transport의 계약 — 네트워크 없이 검증 가능한 부분.

종단 동작(mTLS 핸드셰이크·사칭 차단·Kafka 적재)은 게이트웨이 쪽
`IngestStreamIT`가 실제 브로커로 검증한다. 여기서는 **차량 측이 지켜야 할 계약**만 본다:
kind 매핑이 정확한가, ack 핸들러 없이 스트림을 열지 않는가, 그리고
:class:`~fleetsentinel_ingest.shipping.Transport` 프로토콜을 만족하는가.
"""

from __future__ import annotations

import pytest
from types import SimpleNamespace

from fleetsentinel_ingest.grpc_transport import (
    KIND_PERCEPTION,
    _KIND_TO_WIRE,
    GrpcTransport,
)
from fleetsentinel_ingest.proto import ingest_pb2
from fleetsentinel_ingest.shipping import Transport
from fleetsentinel_ingest.wal import KIND_SEGMENT_REF, KIND_SIGNAL


class TestKindMapping:
    """proto3는 enum 0을 UNSPECIFIED로 예약하므로 WAL kind와 값이 어긋난다.

    매핑이 한 곳에만 있어야 하고, 그 한 곳이 정확해야 한다. 어긋나면 신호가 객체 메타데이터
    토픽으로 가는 종류의 오류가 되고 하류에서 찾기 어렵다.
    """

    def test_every_wal_kind_maps(self):
        assert set(_KIND_TO_WIRE) == {KIND_SIGNAL, KIND_SEGMENT_REF, KIND_PERCEPTION}

    def test_mapping_is_exact(self):
        assert _KIND_TO_WIRE[KIND_SIGNAL] == ingest_pb2.RECORD_KIND_SIGNAL
        assert _KIND_TO_WIRE[KIND_SEGMENT_REF] == ingest_pb2.RECORD_KIND_SEGMENT_REF
        assert _KIND_TO_WIRE[KIND_PERCEPTION] == ingest_pb2.RECORD_KIND_PERCEPTION

    def test_never_maps_to_unspecified(self):
        # UNSPECIFIED로 보내면 게이트웨이가 라우팅할 수 없어 그 레코드만 조용히 사라진다.
        assert ingest_pb2.RECORD_KIND_UNSPECIFIED not in _KIND_TO_WIRE.values()

    def test_wal_kinds_are_distinct_from_wire(self):
        # WAL 0(SIGNAL)과 wire 0(UNSPECIFIED)이 다르다는 사실 자체가 매핑의 존재 이유다.
        assert KIND_SIGNAL == 0
        assert ingest_pb2.RECORD_KIND_SIGNAL != KIND_SIGNAL


class TestTransportProtocol:
    def test_satisfies_transport_protocol(self):
        # WalShipper가 요구하는 것은 send(boot_id, seq, kind, payload) 하나다.
        assert isinstance(GrpcTransport, type)
        assert hasattr(GrpcTransport, "send")
        # 런타임 프로토콜이 아니므로 시그니처로 확인한다.
        import inspect

        params = list(inspect.signature(GrpcTransport.send).parameters)
        assert params == ["self", "boot_id", "seq", "kind", "payload"]
        assert hasattr(Transport, "send")


class TestAckHandlerRequired:
    """ack을 버리면 WAL 커밋이 영영 전진하지 않고 디스크가 찬다. 조용히 두지 않는다."""

    def _transport(self, **kw):
        import grpc

        return GrpcTransport(
            target="localhost:1",  # 연결하지 않는다 — start() 전에 막히는지만 본다
            credentials=grpc.ssl_channel_credentials(),
            vehicle_id="vehicle-0001",
            **kw,
        )

    def test_start_without_ack_handler_raises(self):
        t = self._transport()
        with pytest.raises(RuntimeError, match="ack 핸들러가 없다"):
            t.start("01JBOOT")

    def test_set_ack_handler_satisfies_it(self):
        seen: list[int] = []
        handler = seen.append  # 바운드 메서드는 접근할 때마다 새 객체다 — 참조를 고정한다
        t = self._transport()
        t.set_ack_handler(handler)
        assert t._on_ack is handler

    def test_constructor_handler_also_works(self):
        seen: list[int] = []
        handler = seen.append
        t = self._transport(on_ack=handler)
        assert t._on_ack is handler

    def test_cannot_swap_handler_after_stream_opens(self):
        # ack이 두 곳으로 갈라지면 커밋 전진이 어느 쪽 기준인지 불명확해진다.
        t = self._transport(on_ack=lambda _: None)
        t._started = True
        with pytest.raises(RuntimeError, match="스트림이 이미 열렸다"):
            t.set_ack_handler(lambda _: None)

    def test_self_initiated_close_is_not_a_transport_error(self):
        """우리가 부른 close()로 생긴 CANCELLED는 `terminal_error`가 아니다.

        호출부가 stall을 감지해 닫았는데 그 close가 `terminal_error`를 채우면,
        보고가 "Channel closed"가 되어 **진짜 원인을 덮는다.** 실제로 그렇게 나왔었다.
        """
        import grpc

        t = self._transport(on_ack=lambda _: None)
        t._closing = True

        class _Cancelled(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.CANCELLED

            def details(self):
                return "Channel closed!"

        def _raising_iter():
            raise _Cancelled()
            yield  # pragma: no cover — 제너레이터로 만들기 위한 것

        t._pump_acks(_raising_iter())
        assert t.stats.terminal_error is None

    def test_unexpected_reader_death_is_reported(self):
        """RpcError가 아닌 예외로 ack 리더가 죽으면 조용히 사라지면 안 된다.

        그냥 두면 호출부는 `terminal_error=None`을 보고 stall로 오해한다.
        """
        t = self._transport(on_ack=lambda _: None)

        def _boom_iter():
            raise ValueError("스텁이 깨졌다")
            yield  # pragma: no cover

        t._pump_acks(_boom_iter())
        assert t.stats.terminal_error is not None
        assert "ack 리더 비정상 종료" in t.stats.terminal_error

    def test_unmapped_kind_raises_rather_than_silently_dropping(self):
        t = self._transport(on_ack=lambda _: None)
        t._started = True  # start()를 우회해 send()의 검사만 본다
        with pytest.raises(ValueError, match="매핑되지 않은 WAL kind"):
            t.send("01JBOOT", 0, 99, b"x")

    def test_new_stream_replays_only_unacknowledged_pending_records(self):
        seen: list[int] = []
        t = self._transport(on_ack=seen.append)
        t._started = True
        t._boot_id = "01JBOOT"
        for seq in range(3):
            t.send("01JBOOT", seq, 0, f"r{seq}".encode())

        t._stream_generation = 1
        first = t._drain_pending(1)
        assert [next(first).seq for _ in range(3)] == [0, 1, 2]

        assert t._pump_acks([SimpleNamespace(ack_seq=1)]) is False
        assert seen == [1]
        assert list(t._pending) == [2]

        t._stream_generation = 2
        replay = t._drain_pending(2)
        assert next(replay).seq == 2
        t.close(timeout=0)

    def test_same_stream_sends_late_lower_sequence_that_closes_a_gap(self):
        t = self._transport(on_ack=lambda _: None)
        t._started = True
        t._boot_id = "01JBOOT"
        for seq in (100, 101, 103):
            t.send("01JBOOT", seq, 0, b"x")

        t._stream_generation = 1
        stream = t._drain_pending(1)
        assert [next(stream).seq for _ in range(3)] == [100, 101, 103]
        t.send("01JBOOT", 102, 0, b"x")
        assert next(stream).seq == 102
        t.close(timeout=0)

    def test_cack_removes_late_lower_sequence_after_a_higher_pending_sequence(self):
        t = self._transport(on_ack=lambda _: None)
        t._started = True
        t._boot_id = "01JBOOT"
        for seq in (100, 103, 102):
            t.send("01JBOOT", seq, 0, b"x")
        t._pump_acks([SimpleNamespace(ack_seq=102)])
        assert list(t._pending) == [103]
        t.close(timeout=0)

    def test_failed_ack_callback_keeps_records_for_replay(self):
        def failed_ack(_seq):
            raise OSError("WAL commit failed")

        t = self._transport(on_ack=failed_ack)
        t._started = True
        t._boot_id = "01JBOOT"
        t.send("01JBOOT", 0, 0, b"x")
        t._pump_acks([SimpleNamespace(ack_seq=0)])
        assert list(t._pending) == [0]
        assert t.stats.last_ack == -1
        assert "WAL commit failed" in t.stats.terminal_error
        t.close(timeout=0)

    def test_queue_size_must_be_positive(self):
        with pytest.raises(ValueError, match="queue_size"):
            self._transport(queue_size=0)

    def test_supervisor_reopens_rpc_and_replays_unacknowledged_record(self, monkeypatch):
        import grpc
        import threading

        acked = threading.Event()
        t = self._transport(on_ack=lambda _: acked.set())
        requests = []
        attempts = []

        class Unavailable(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNAVAILABLE

            def details(self):
                return "connection lost before CACK"

        class Stub:
            def Stream(self, records, **_kwargs):
                attempts.append(True)
                requests.append(next(records).seq)
                if len(attempts) == 1:
                    raise Unavailable()
                yield SimpleNamespace(ack_seq=0)
                t._closed.wait(2)

        t._stub = Stub()
        monkeypatch.setattr(t, "_recreate_channel", lambda: None)
        try:
            t.send("01JBOOT", 0, 0, b"durable-record")
            assert acked.wait(3), "supervisor did not reopen the failed RPC"
        finally:
            t.close(timeout=3)
        assert requests == [0, 0]
        assert t.stats.reconnect_attempts == 1
        assert t.stats.wire_sent == 2
        assert list(t._pending) == []
