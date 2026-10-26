package io.fleetsentinel.gateway.kafka;

import io.fleetsentinel.gateway.config.GatewayProperties;
import lombok.RequiredArgsConstructor;
import io.fleetsentinel.gateway.proto.RecordKind;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * 레코드 하나를 Kafka에 쓴다. 게이트웨이가 상태를 갖지 않는 유일한 이유가 여기 있다 —
 * 쓰기 성공 여부만 알면 되고, 어디까지 봤는지는 기억하지 않는다.
 *
 * <h2>파티션 키</h2>
 *
 * <p>키는 <b>인증서에서 나온</b> {@code vehicleId}다(SDD S-9). 페이로드에서 읽지 않는다.
 * 그리고 한 차량이 여러 파티션에 걸치면 CACK의 전제가 깨진다 —
 * "연속으로 성공한 최고 seq 이하는 전부 안전하다"가 파티션별 콜백 순서에 기대기 때문이다
 * (ack·dedup 설계 A-L5).
 *
 * <h2>페이로드를 열지 않는다</h2>
 *
 * <p>값은 Avro 바이트 그대로 통과시킨다. 게이트웨이가 스키마를 알면 새 채널을 추가할 때마다
 * 게이트웨이를 재배포해야 한다. 검증은 Flink가 한다.
 */
@Component
@RequiredArgsConstructor
public class RecordPublisher {

    private final KafkaTemplate<byte[], byte[]> kafka;
    private final GatewayProperties props;

    /**
     * @return 쓰기 완료 future. <b>완료됐을 때만</b> 해당 seq를 ack해도 된다 —
     *         수신 직후 ack하면 게이트웨이 크래시에서 유실이 결번으로도 안 잡힌다
     */
    public CompletableFuture<SendResult<byte[], byte[]>> publish(
            String vehicleId, String bootId, long seq, RecordKind kind, byte[] payload) {

        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                topicFor(kind),
                null,
                vehicleId.getBytes(StandardCharsets.UTF_8),
                payload);

        // seq·boot_id는 헤더로 간다. 페이로드를 열지 않고도 Flink가 dedup 키를 만들 수 있어야
        // 하고, vehicle_id는 이미 메시지 키다.
        record.headers()
                .add(new RecordHeader("vehicle_id", vehicleId.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("boot_id", bootId.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("seq", Long.toString(seq).getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("kind", kind.name().getBytes(StandardCharsets.UTF_8)));

        return kafka.send(record);
    }

    /**
     * <b>계층을 토픽으로 나누지 않는다.</b> 셋 다 {@code telemetry.records} 로 간다.
     *
     * <p>`seq`는 차량별 단일 수열이고 결번이 곧 유실이다(data-design.md §5.0). 계층별로
     * 토픽을 나누면 그 수열이 조각나서 Flink dedup 이 연속성을 볼 수 없다 — Kafka 는
     * 파티션 안에서만 순서를 보장하기 때문이다. 실제로 분리 상태에서 신호 51,025건이
     * 전부 `too_old` 로 폐기됐다.
     *
     * <p>계층 구분은 {@code kind} 헤더가 한다. 분기는 Flink 이후에서 한다.
     *
     * @throws IllegalArgumentException kind 가 UNSPECIFIED 이거나 알 수 없는 값.
     *         계약 위반이므로 조용히 통과시키지 않는다
     */
    private String topicFor(RecordKind kind) {
        return switch (kind) {
            case RECORD_KIND_SIGNAL, RECORD_KIND_PERCEPTION, RECORD_KIND_SEGMENT_REF ->
                    props.getTopics().getRecords();
            case RECORD_KIND_UNSPECIFIED, UNRECOGNIZED ->
                    throw new IllegalArgumentException("unroutable record kind: " + kind);
        };
    }
}
