package io.fleetsentinel.gateway.grpc;

import io.fleetsentinel.gateway.ack.AckTracker;
import io.fleetsentinel.gateway.config.GatewayProperties;
import io.fleetsentinel.gateway.identity.PeerIdentityInterceptor;
import io.fleetsentinel.gateway.identity.VehicleIdentity;
import io.fleetsentinel.gateway.kafka.RecordPublisher;
import io.fleetsentinel.gateway.proto.Ack;
import io.fleetsentinel.gateway.proto.IngestGrpc;
import io.fleetsentinel.gateway.proto.IngestRecord;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.grpc.server.service.GrpcService;

/**
 * 차량 업링크 스트림 하나를 처리한다.
 *
 * <p>게이트웨이가 stateless라는 말은 <b>차량별 진행 커서를 보유하지 않는다</b>는 뜻이지
 * 스트림 수명 동안 아무것도 안 들고 있다는 뜻이 아니다. 재개 지점은 차량이
 * {@code committed_seq + 1}로 정하고, 게이트웨이는 그 값을 기억하지 않는다 — 그래서 어느
 * 인스턴스에 붙어도 동작이 같고 확장이 인스턴스 추가로 끝난다.
 */
@GrpcService
@Slf4j
public class IngestService extends IngestGrpc.IngestImplBase {

    private final RecordPublisher publisher;
    private final GatewayProperties props;
    private final MeterRegistry meters;
    private final ScheduledExecutorService ackScheduler;
    private final Counter received;
    private final Counter published;
    private final Counter failed;

    public IngestService(RecordPublisher publisher, GatewayProperties props, MeterRegistry meters,
                         ScheduledExecutorService ackScheduler) {
        this.publisher = publisher;
        this.props = props;
        this.meters = meters;
        this.ackScheduler = ackScheduler;
        this.received = meters.counter("fleetsentinel.gateway.records.received");
        this.published = meters.counter("fleetsentinel.gateway.records.published");
        this.failed = meters.counter("fleetsentinel.gateway.records.failed");
    }

    @Override
    public StreamObserver<IngestRecord> stream(StreamObserver<Ack> responses) {
        // 인터셉터가 이미 확정했다. 여기서 못 읽으면 인터셉터가 등록되지 않은 것이다.
        VehicleIdentity identity = PeerIdentityInterceptor.IDENTITY.get();
        String bootId = PeerIdentityInterceptor.BOOT.get();
        if (identity == null || bootId == null) {
            responses.onError(Status.INTERNAL
                    .withDescription("identity interceptor not applied")
                    .asRuntimeException());
            return new NoopObserver();
        }
        return new StreamHandler(identity, bootId, responses);
    }

    /** 스트림 하나의 상태. 인스턴스가 아니라 스트림에 붙는다. */
    private final class StreamHandler implements StreamObserver<IngestRecord> {

        private final VehicleIdentity identity;
        private final String bootId;
        private final ServerCallStreamObserver<Ack> responses;

        /**
         * {@link AckTracker}와 {@code responses.onNext()}를 함께 감싸는 락.
         *
         * <p><b>둘을 같은 임계 구역에 둬야 한다.</b> {@code complete()}는 Kafka 프로듀서 IO
         * 스레드에서, 레코드 수신은 gRPC 스레드에서 온다. 락을 따로 잡으면 ack이 역순으로
         * 나갈 수 있고, 그러면 차량이 커밋을 되돌린다. {@code StreamObserver} 자체도 스레드
         * 안전하지 않다.
         */
        private final Object lock = new Object();

        private AckTracker tracker;
        private long highestSeq = -1;
        private long inflight;
        private boolean closed;

        /**
         * 차량이 half-close 했다. 그러나 <b>아직 끝난 것이 아니다</b> — 마지막에 보낸
         * 레코드들의 Kafka 쓰기가 진행 중일 수 있고, 그것들이 완료되기 전에 스트림을 닫으면
         * 그만큼의 CACK을 못 보낸다. 차량은 다음 연결에서 그 구간을 통째로 재전송한다.
         * 유실은 아니지만(dedup이 흡수한다) 매 스트림 종료마다 최대 max_inflight 만큼을
         * 버리는 셈이라 그냥 두면 안 된다.
         */
        private boolean clientDone;

        /**
         * CACK 주기 플러시. {@code AckTracker.take()}의 시간 조건은 호출될 때만 평가되는데
         * 호출 지점이 publish 완료 콜백뿐이라, 차량이 {@code every-n} 미만을 보내고 조용해지면
         * 그 버스트의 CACK이 영영 안 나간다(정차 중인 차량이 정확히 그 경우다).
         */
        private ScheduledFuture<?> flush;

        StreamHandler(VehicleIdentity identity, String bootId, StreamObserver<Ack> responses) {
            this.identity = identity;
            this.bootId = bootId;
            this.responses = (ServerCallStreamObserver<Ack>) responses;
            this.responses.setOnCancelHandler(() -> {
                synchronized (lock) {
                    closed = true;
                    cancelFlush();
                }
                log.info("스트림 취소: vehicle={} boot={} highestSeq={}",
                        identity.vehicleId(), bootId, highestSeq);
            });

            long periodMs = Math.max(1, props.getAck().getEvery().toMillis());
            this.flush = ackScheduler.scheduleWithFixedDelay(
                    this::flushTick, periodMs, periodMs, TimeUnit.MILLISECONDS);
        }

        /** 주기 깨우기. 보낼 ack이 없으면 아무 일도 하지 않는다. */
        private void flushTick() {
            synchronized (lock) {
                if (closed || tracker == null) {
                    return;
                }
                emit(tracker.take(System.nanoTime()));
            }
        }

        /** 호출자가 {@link #lock}을 들고 있어야 한다. */
        private void cancelFlush() {
            if (flush != null) {
                flush.cancel(false);
                flush = null;
            }
        }

        @Override
        public void onNext(IngestRecord record) {
            received.increment();

            synchronized (lock) {
                if (closed) {
                    return;
                }
                if (tracker == null) {
                    // 첫 레코드의 seq가 이 스트림의 시작점이다. 차량이 재개 지점을 정하므로
                    // 게이트웨이는 그것이 무엇이든 받아들인다 — 이미 Kafka에 들어간 구간이
                    // 다시 와도 하류 dedup이 흡수한다(의도된 at-least-once).
                    tracker = new AckTracker(record.getSeq(),
                            props.getAck().getEveryN(), props.getAck().getEvery(), System.nanoTime());
                }
                if (inflight >= props.getMaxInflight()) {
                    // 차량이 자기 max_inflight를 지키지 않고 있다. 힙이 밀리기 전에 끊는다.
                    fail(Status.RESOURCE_EXHAUSTED.withDescription(
                            "too many unacknowledged records (" + inflight + ")"));
                    return;
                }
                inflight++;
                highestSeq = Math.max(highestSeq, record.getSeq());
            }

            long seq = record.getSeq();
            CompletableFuture<?> pending;
            try {
                pending = publisher.publish(identity.vehicleId(), bootId, seq, record.getKind(),
                        record.getPayload().toByteArray());
            } catch (IllegalArgumentException e) {
                // 라우팅할 토픽이 없다(kind가 UNSPECIFIED이거나 알 수 없는 값).
                // 클라이언트 계약 위반이므로 INVALID_ARGUMENT로 끊는다.
                failed.increment();
                synchronized (lock) {
                    inflight--;
                    fail(Status.INVALID_ARGUMENT.withDescription(e.getMessage()));
                }
                return;
            } catch (RuntimeException e) {
                // KafkaTemplate.send 는 프로듀서 버퍼 고갈·메타데이터 미확보 시
                // max.block.ms 만큼 블록한 뒤 **동기로** 던진다. 이걸 안 잡으면
                // inflight가 새서 finishIfDone()이 영영 못 돌고, 스트림은 우리가 고른
                // 상태가 아니라 새어나간 예외로 끝난다.
                failed.increment();
                synchronized (lock) {
                    inflight--;
                    fail(Status.UNAVAILABLE.withDescription(
                            "kafka publish rejected: " + e.getMessage()));
                }
                log.warn("Kafka 전송이 동기로 거부됐다: vehicle={} seq={}",
                        identity.vehicleId(), seq, e);
                return;
            }

            pending.whenComplete((result, error) -> {
                        if (error != null) {
                            // 완료 표시를 하지 않는다 → ack이 전진하지 않는다 → 차량이
                            // 재전송한다. 이것이 유실을 막는 유일한 장치다.
                            failed.increment();
                            synchronized (lock) {
                                inflight--;
                                // 완료 표시를 하지 않으므로 ack이 이 seq 앞에서 멈춘다.
                                // 그래도 스트림 종료 판정에는 참여해야 한다 — 아니면
                                // 쓰기 하나가 실패했다는 이유로 스트림이 영영 안 닫힌다.
                                finishIfDone();
                            }
                            log.warn("Kafka 쓰기 실패: vehicle={} seq={}", identity.vehicleId(), seq, error);
                            return;
                        }
                        published.increment();
                        synchronized (lock) {
                            inflight--;
                            if (closed) {
                                return;
                            }
                            tracker.complete(seq);
                            emit(tracker.take(System.nanoTime()));
                            finishIfDone();
                        }
                    });
        }

        /** 호출자가 {@link #lock}을 들고 있어야 한다. */
        private void emit(OptionalLong ack) {
            if (ack.isEmpty() || closed) {
                return;
            }
            try {
                responses.onNext(Ack.newBuilder().setAckSeq(ack.getAsLong()).build());
            } catch (StatusRuntimeException e) {
                // 차량이 이미 끊었다. 재전송으로 복구되므로 여기서 할 일은 없다.
                closed = true;
            }
        }

        /** 호출자가 {@link #lock}을 들고 있어야 한다. */
        private void fail(Status status) {
            if (closed) {
                return;
            }
            closed = true;
            cancelFlush();
            responses.onError(status.asRuntimeException());
        }

        @Override
        public void onError(Throwable t) {
            synchronized (lock) {
                closed = true;
                cancelFlush();
            }
            // 셀룰러 단절은 정상이다. 차량이 committed+1부터 재개한다.
            log.info("스트림 오류: vehicle={} boot={} highestSeq={} ({})",
                    identity.vehicleId(), bootId, highestSeq, t.toString());
        }

        @Override
        public void onCompleted() {
            synchronized (lock) {
                clientDone = true;
                finishIfDone();
            }
        }

        /**
         * 차량이 half-close 했고 in-flight 쓰기가 모두 끝났을 때만 스트림을 닫는다.
         *
         * <p>호출자가 {@link #lock}을 들고 있어야 한다.
         */
        private void finishIfDone() {
            if (!clientDone || inflight > 0 || closed) {
                return;
            }
            if (tracker != null) {
                // 주기 조건에 걸려 못 나간 마지막 구간을 흘려보낸다. 여기까지 와야
                // "보낸 것 전부에 대해 CACK을 받았다"가 성립한다.
                emit(tracker.drain());
            }
            if (closed) {
                // emit() 안에서 차량이 이미 끊긴 걸 발견했다. 여기서 onCompleted()를
                // 부르면 IllegalStateException이 나는데, 이 메서드는 Kafka IO 스레드의
                // whenComplete 안에서도 불리므로 CompletableFuture가 그걸 삼킨다 —
                // 아무도 못 보는 실패가 된다.
                cancelFlush();
                return;
            }
            closed = true;
            cancelFlush();
            responses.onCompleted();
            meters.counter("fleetsentinel.gateway.stream.completed").increment();
        }
    }

    private static final class NoopObserver implements StreamObserver<IngestRecord> {
        @Override
        public void onNext(IngestRecord value) {
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
        }
    }
}
