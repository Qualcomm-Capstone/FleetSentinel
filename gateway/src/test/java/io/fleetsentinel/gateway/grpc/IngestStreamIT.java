package io.fleetsentinel.gateway.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.google.protobuf.ByteString;
import io.fleetsentinel.gateway.proto.Ack;
import io.fleetsentinel.gateway.proto.IngestGrpc;
import io.fleetsentinel.gateway.proto.IngestRecord;
import io.fleetsentinel.gateway.proto.RecordKind;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.SslContext;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.annotation.DirtiesContext;

/**
 * 종단 검증 — 실제 mTLS 핸드셰이크 + 실제 Kafka 쓰기.
 *
 * <p>단위 테스트가 확인하지 못하는 것을 확인한다: 인증서가 실제로 요구되는가, SAN에서 뽑은
 * 신원이 실제 파티션 키가 되는가, 그리고 <b>사칭이 실제로 막히는가</b>.
 *
 * <p>인증서는 {@code scripts/gen-certs.sh}가 굽는다 — 테스트 전용 발급 경로를 따로 두면
 * "테스트는 통과하는데 운영에서 안 되는" 차이가 생긴다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(
        partitions = 1,
        topics = {"telemetry.records", "telemetry.dlq"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class IngestStreamIT {

    @Value("${local.grpc.port}")
    private int grpcPort;
    /** 계층 셋이 한 토픽으로 간다. */
    private static final String TOPIC = "telemetry.records";
    private static final Path PKI = TestPki.generate();

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private final List<ManagedChannel> channels = new ArrayList<>();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.grpc.server.port", () -> 0);
        registry.add("spring.grpc.server.ssl.enabled", () -> true);
        registry.add("spring.grpc.server.ssl.bundle", () -> "gateway");
        registry.add("spring.grpc.server.ssl.client-auth", () -> "REQUIRE");
        registry.add("spring.ssl.bundle.pem.gateway.keystore.certificate",
                () -> "file:" + PKI.resolve("server/server.crt"));
        registry.add("spring.ssl.bundle.pem.gateway.keystore.private-key",
                () -> "file:" + PKI.resolve("server/server.key"));
        registry.add("spring.ssl.bundle.pem.gateway.truststore.certificate",
                () -> "file:" + PKI.resolve("ca/ca.crt"));
    }

    @AfterEach
    void closeChannels() throws InterruptedException {
        channels.forEach(ManagedChannel::shutdownNow);
        for (var channel : channels) {
            assertThat(channel.awaitTermination(10, TimeUnit.SECONDS)).as("gRPC channel termination").isTrue();
        }
        channels.clear();
    }

    // ── 정상 경로 ────────────────────────────────────────────────────────

    @Test
    @DisplayName("신호·객체 메타데이터 레코드가 Kafka에 실리고 CACK이 돌아온다")
    void publishesAndAcks() throws Exception {
        // Given
        var stub = stubFor("vehicle-0001", "vehicle-0001", "01JBOOTA");

        var acks = new CopyOnWriteArrayList<Long>();
        var done = new CountDownLatch(1);
        var error = new AtomicReference<Throwable>();

        StreamObserver<IngestRecord> out = stub.stream(new StreamObserver<>() {
            @Override
            public void onNext(Ack ack) {
                acks.add(ack.getAckSeq());
            }

            @Override
            public void onError(Throwable t) {
                error.set(t);
                done.countDown();
            }

            @Override
            public void onCompleted() {
                done.countDown();
            }
        });

        // When
        for (long seq = 0; seq < 200; seq++) {
            out.onNext(IngestRecord.newBuilder()
                    .setSeq(seq)
                    .setKind(seq % 2 == 0 ? RecordKind.RECORD_KIND_SIGNAL : RecordKind.RECORD_KIND_PERCEPTION)
                    .setPayload(ByteString.copyFrom(("rec-" + seq).getBytes(StandardCharsets.UTF_8)))
                    .build());
        }
        out.onCompleted();

        // Then
        assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        assertThat(error.get()).isNull();

        // CACK은 단조 증가하고 마지막이 199다 — 그 이하는 전부 Kafka에 안전하게 들어갔다는 뜻.
        assertThat(acks).isNotEmpty().isSorted();
        assertThat(acks.getLast()).isEqualTo(199L);

        assertThat(drain("RECORD_KIND_SIGNAL", "01JBOOTA")).hasSize(100);
        assertThat(drain("RECORD_KIND_PERCEPTION", "01JBOOTA")).hasSize(100);
    }

    @Test
    @DisplayName("파티션 키는 인증서에서 나온 vehicle_id다 — 페이로드가 아니라")
    void partitionKeyComesFromCertificate() throws Exception {
        // Given
        var stub = stubFor("vehicle-0002", "vehicle-0002", "01JBOOTB");
        // When
        sendOne(stub, 0, RecordKind.RECORD_KIND_SEGMENT_REF, "payload-claims-nothing");

        // Then
        var records = await().atMost(Duration.ofSeconds(30))
                .until(() -> drain("RECORD_KIND_SEGMENT_REF", "01JBOOTB"), r -> !r.isEmpty());

        var record = records.getFirst();
        assertThat(new String(record.key(), StandardCharsets.UTF_8)).isEqualTo("vehicle-0002");
        assertThat(header(record, "vehicle_id")).isEqualTo("vehicle-0002");
        assertThat(header(record, "boot_id")).isEqualTo("01JBOOTB");
        assertThat(header(record, "seq")).isEqualTo("0");
    }

    @Test
    @DisplayName("every-n 미만을 보내고 조용해져도 CACK이 온다 — 정차 중인 차량")
    void idleBurstStillGetsCack() throws Exception {
        // Given
        // every-n=128 이므로 10건은 개수 조건을 만족하지 못한다. 시간 조건(200ms)이
        // 주기 플러시로 깨어나야만 CACK이 나간다. 스트림을 닫지 않고 기다린다 —
        // onCompleted()의 drain에 기대면 이 검사가 무의미해진다.
        var stub = stubFor("vehicle-0001", "vehicle-0001", "01JBOOTG");
        var acks = new CopyOnWriteArrayList<Long>();
        var failure = new AtomicReference<Throwable>();

        StreamObserver<IngestRecord> out = stub.stream(new StreamObserver<>() {
            @Override
            public void onNext(Ack ack) {
                acks.add(ack.getAckSeq());
            }

            @Override
            public void onError(Throwable t) {
                failure.set(t);
            }

            @Override
            public void onCompleted() {
            }
        });

        // When
        for (long seq = 0; seq < 10; seq++) {
            out.onNext(IngestRecord.newBuilder()
                    .setSeq(seq)
                    .setKind(RecordKind.RECORD_KIND_SIGNAL)
                    .setPayload(ByteString.copyFromUtf8("idle-" + seq))
                    .build());
        }
        // 스트림은 열어둔 채로 둔다 — onCompleted()의 drain에 기대면 이 검사가 무의미해진다.
        // 주기 플러시는 200ms마다 그 시점의 연속 최고 seq를 흘리므로 ack이 여러 번
        // 나뉘어 올 수 있다. 중요한 것은 **스트림을 닫지 않고도 9까지 도달**하는 것이다.
        // Then
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> {
                    assertThat(failure.get()).isNull();
                    assertThat(acks)
                            .as("유휴 상태에서 CACK이 9까지 오지 않았다 — 주기 플러시가 없다")
                            .contains(9L);
                });
        assertThat(acks).isSorted();

        out.onCompleted();
    }

    // ── 신원 바인딩 ──────────────────────────────────────────────────────

    @Test
    @DisplayName("사칭 — vehicle-0001 인증서로 vehicle-0002를 주장하면 PERMISSION_DENIED")
    void rejectsImpersonation() {
        // 인증서는 완벽히 유효하다. CA가 서명했고 만료되지 않았다.
        // 막히는 이유는 오직 신원이 일치하지 않기 때문이다 — 이것이 mTLS만으로는
        // 닫히지 않는 층이다(SDD S-9).
        var stub = stubFor("vehicle-0001", "vehicle-0002", "01JBOOTC");

        var status = expectFailure(stub);
        assertThat(status.getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    }

    @Test
    @DisplayName("클라이언트 인증서가 없으면 연결이 서지 않는다")
    void requiresClientCertificate() {
        var channel = NettyChannelBuilder.forAddress("localhost", grpcPort)
                .overrideAuthority("localhost")
                .sslContext(sslContext(null))
                .build();
        channels.add(channel);

        var status = expectFailure(attach(IngestGrpc.newStub(channel), "vehicle-0001", "01JBOOTD"));

        // client-auth=REQUIRE라 TLS 계층에서 끊긴다. 인터셉터까지 오지 않으므로
        // 상태 코드는 전송 계층이 정한다.
        assertThat(status.getCode())
                .isIn(Status.Code.UNAVAILABLE, Status.Code.UNAUTHENTICATED, Status.Code.INTERNAL);
    }

    @Test
    @DisplayName("x-boot-id가 없으면 INVALID_ARGUMENT")
    void requiresBootId() {
        var status = expectFailure(stubFor("vehicle-0001", "vehicle-0001", null));
        assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    @Test
    @DisplayName("주장을 생략하면 인증서 신원으로 진행한다 — 주장은 선택이고 신원은 인증서다")
    void claimIsOptional() throws Exception {
        var stub = stubFor("vehicle-0001", null, "01JBOOTE");
        sendOne(stub, 7, RecordKind.RECORD_KIND_PERCEPTION, "no-claim");

        var records = await().atMost(Duration.ofSeconds(30))
                .until(() -> drain("RECORD_KIND_PERCEPTION", "01JBOOTE"), r -> !r.isEmpty());
        assertThat(new String(records.getFirst().key(), StandardCharsets.UTF_8)).isEqualTo("vehicle-0001");
    }

    // ── 라우팅 ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("kind가 UNSPECIFIED면 INVALID_ARGUMENT — 조용히 한 토픽으로 몰지 않는다")
    void rejectsUnroutableKind() throws InterruptedException {
        var stub = stubFor("vehicle-0001", "vehicle-0001", "01JBOOTF");
        var latch = new CountDownLatch(1);
        var error = new AtomicReference<Throwable>();
        var out = stub.stream(observer(latch, error));

        out.onNext(IngestRecord.newBuilder()
                .setSeq(0)
                .setKind(RecordKind.RECORD_KIND_UNSPECIFIED)
                .setPayload(ByteString.copyFromUtf8("x"))
                .build());
        assertThat(latch.await(30, TimeUnit.SECONDS)).as("gRPC rejection callback").isTrue();

        assertThat(error.get()).isNotNull();
        assertThat(Status.fromThrowable(error.get()).getCode())
                .isEqualTo(Status.Code.INVALID_ARGUMENT);

        assertThat(drain("RECORD_KIND_SIGNAL", "01JBOOTF")).isEmpty();
        assertThat(drain("RECORD_KIND_PERCEPTION", "01JBOOTF")).isEmpty();
        assertThat(drain("RECORD_KIND_SEGMENT_REF", "01JBOOTF")).isEmpty();
    }

    // ── 도우미 ───────────────────────────────────────────────────────────

    private IngestGrpc.IngestStub stubFor(String certVehicle, String claim, String bootId) {
        var channel = NettyChannelBuilder.forAddress("localhost", grpcPort)
                .overrideAuthority("localhost")
                .sslContext(sslContext(certVehicle))
                .build();
        channels.add(channel);
        return attach(IngestGrpc.newStub(channel), claim, bootId);
    }

    private static IngestGrpc.IngestStub attach(IngestGrpc.IngestStub stub, String claim, String bootId) {
        var md = new Metadata();
        if (claim != null) {
            md.put(Metadata.Key.of("x-vehicle-id", Metadata.ASCII_STRING_MARSHALLER), claim);
        }
        if (bootId != null) {
            md.put(Metadata.Key.of("x-boot-id", Metadata.ASCII_STRING_MARSHALLER), bootId);
        }
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md));
    }

    /** @param vehicle null이면 클라이언트 인증서 없이 연결한다 */
    private static SslContext sslContext(String vehicle) {
        try {
            var builder = GrpcSslContexts.forClient()
                    .trustManager(PKI.resolve("ca/ca.crt").toFile());
            if (vehicle != null) {
                File dir = PKI.resolve("vehicles/" + vehicle).toFile();
                builder.keyManager(new File(dir, vehicle + ".crt"), new File(dir, vehicle + ".key"));
            }
            return builder.build();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static void sendOne(IngestGrpc.IngestStub stub, long seq, RecordKind kind, String payload)
            throws Exception {
        var latch = new CountDownLatch(1);
        var error = new AtomicReference<Throwable>();
        var out = stub.stream(observer(latch, error));
        out.onNext(IngestRecord.newBuilder()
                .setSeq(seq)
                .setKind(kind)
                .setPayload(ByteString.copyFromUtf8(payload))
                .build());
        out.onCompleted();
        assertThat(latch.await(30, TimeUnit.SECONDS)).as("gRPC completion callback").isTrue();
        assertThat(error.get()).isNull();
    }

    private static Status expectFailure(IngestGrpc.IngestStub stub) {
        var latch = new CountDownLatch(1);
        var error = new AtomicReference<Throwable>();
        var out = stub.stream(observer(latch, error));
        try {
            out.onNext(IngestRecord.newBuilder()
                    .setSeq(0)
                    .setKind(RecordKind.RECORD_KIND_SIGNAL)
                    .setPayload(ByteString.copyFromUtf8("x"))
                    .build());
            out.onCompleted();
        } catch (StatusRuntimeException ignored) {
            // 스트림이 이미 닫혔다. 상태는 onError로 온다.
        }
        try {
            assertThat(latch.await(30, TimeUnit.SECONDS)).as("gRPC failure callback").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for gRPC failure", e);
        }
        assertThat(error.get()).as("실패해야 한다").isNotNull();
        return Status.fromThrowable(error.get());
    }

    private static StreamObserver<Ack> observer(CountDownLatch latch, AtomicReference<Throwable> error) {
        return new StreamObserver<>() {
            @Override
            public void onNext(Ack value) {
            }

            @Override
            public void onError(Throwable t) {
                error.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };
    }

    /**
     * 이 boot_id · kind 로 쓰인 레코드만 걷는다.
     *
     * <p>토픽은 하나다 — 계층별로 나누면 `seq` 단일 수열이 조각나 dedup 이 깨진다
     * (RecordPublisher.topicFor). 그래서 계층 구분은 {@code kind} 헤더로 한다.
     *
     * <p>boot_id 로도 거르는 이유는 임베디드 브로커를 테스트들이 공유하기 때문이다.
     */
    private List<ConsumerRecord<byte[], byte[]>> drain(String kind, String bootId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        var out = new ArrayList<ConsumerRecord<byte[], byte[]>>();
        try (var consumer = new KafkaConsumer<byte[], byte[]>(props)) {
            var partitions = consumer.partitionsFor(TOPIC).stream()
                    .map(p -> new TopicPartition(TOPIC, p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            var endOffsets = consumer.endOffsets(partitions);
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            // 첫 fetch에 일부만 도착해도 조회 시작 시점의 끝 offset까지 수집한다.
            while (partitions.stream().anyMatch(p -> consumer.position(p) < endOffsets.get(p))
                    && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(250)).records(TOPIC).forEach(r -> {
                    if (bootId.equals(header(r, "boot_id")) && kind.equals(header(r, "kind"))) {
                        out.add(r);
                    }
                });
            }
            for (var partition : partitions) {
                assertThat(consumer.position(partition)).as("Kafka drain reached snapshot end")
                        .isGreaterThanOrEqualTo(endOffsets.get(partition));
            }
        }
        return out;
    }

    private static String header(ConsumerRecord<byte[], byte[]> record, String key) {
        var h = record.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    /** {@code scripts/gen-certs.sh}를 그대로 호출한다. 발급 경로를 테스트가 따로 갖지 않는다. */
    static final class TestPki {
        static Path generate() {
            try {
                Path dir = Files.createTempDirectory("fleetsentinel-pki");
                Path script = Path.of("..", "scripts", "gen-certs.sh").toAbsolutePath().normalize();

                var pb = new ProcessBuilder("bash", script.toString(), "vehicle-0001", "vehicle-0002")
                        .redirectErrorStream(true);
                pb.environment().put("PKI_DIR", dir.toString());
                pb.environment().put("SERVER_DNS", "localhost");

                var proc = pb.start();
                String output = new String(proc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                if (proc.waitFor() != 0) {
                    throw new IllegalStateException("gen-certs.sh 실패:\n" + output);
                }
                return dir;
            } catch (IOException | InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
