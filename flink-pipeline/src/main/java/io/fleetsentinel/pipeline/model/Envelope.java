package io.fleetsentinel.pipeline.model;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Kafka 레코드 하나. 페이로드는 아직 열지 않는다.
 *
 * 신원 3종 (vehicle_id, boot_id, seq)은 게이트웨이가 헤더에 넣은 것이고, 그중 vehicle_id 는
 * 클라이언트 인증서에서 나온 값이다. 페이로드 안에 든 값은 믿지 않는다 — 두 벌이 있으면
 * 어느 쪽을 믿느냐에 따라 구멍이 열린다.
 */
public final class Envelope implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String vehicleId;
    private final String bootId;
    private final long seq;
    private final String kind;
    private final byte[] payload;
    private final String topic;
    private final String streamId;
    private final long reportIntervalMs;
    private final String sourceBootId;
    private Integer sourcePartition;
    private Long sourceOffset;
    private byte[] sourceKey;
    private List<HeaderValue> sourceHeaders = new ArrayList<>();
    private String validationError;

    public static final class HeaderValue implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String key;
        private final byte[] value;
        public HeaderValue(String key, byte[] value) { this.key = key; this.value = value; }
        public String key() { return key; }
        public byte[] value() { return value; }
    }

    public Envelope(String vehicleId, String bootId, long seq, String kind,
                    byte[] payload, String topic) {
        this(vehicleId, bootId, seq, kind, payload, topic, "legacy", 0, bootId);
    }

    private Envelope(String vehicleId, String bootId, long seq, String kind,
                     byte[] payload, String topic, String streamId, long reportIntervalMs, String sourceBootId) {
        this.vehicleId = vehicleId;
        this.bootId = bootId;
        this.seq = seq;
        this.kind = kind;
        this.payload = payload;
        this.topic = topic;
        this.streamId = streamId;
        this.reportIntervalMs = reportIntervalMs;
        this.sourceBootId = sourceBootId;
    }

    /**
     * Kafka 레코드에서 봉투를 만든다.
     *
     * @throws IllegalArgumentException 헤더가 빠졌을 때. 게이트웨이가 항상 넣으므로
     *         없다는 것은 다른 생산자가 토픽에 쓴 것이고, 조용히 통과시키면 안 된다
     */
    public static Envelope of(ConsumerRecord<byte[], byte[]> record) {
        String vehicleId = header(record, "vehicle_id");
        String bootId = header(record, "boot_id");
        String seqRaw = header(record, "seq");
        String kind = header(record, "kind");
        if (vehicleId == null || vehicleId.isBlank() || bootId == null || bootId.isBlank() || seqRaw == null) {
            throw new IllegalArgumentException(
                    "전송 헤더가 빠졌다: vehicle_id/boot_id/seq (topic=" + record.topic() + ")");
        }
        // 키는 게이트웨이가 인증서 신원으로 넣는다. 헤더와 어긋나면 계약 위반이다.
        if (record.key() != null) {
            String key = new String(record.key(), StandardCharsets.UTF_8);
            if (!key.equals(vehicleId)) {
                throw new IllegalArgumentException(
                        "파티션 키와 vehicle_id 헤더가 다르다: key=" + key + " header=" + vehicleId);
            }
        }
        String stream = header(record, "stream_id");
        String sourceBoot = header(record, "source_boot_id");
        long interval = 0;
        if (stream != null) {
            if (sourceBoot == null || sourceBoot.isBlank()) {
                throw new IllegalArgumentException("MQTT source_boot_id is required");
            }
            if (!java.util.Set.of("status", "event", "history").contains(stream)) {
                throw new IllegalArgumentException("unknown stream_id");
            }
            interval = Long.parseLong(header(record, "report_interval_ms"));
            if (interval < 100 || interval > 60_000) throw new IllegalArgumentException("invalid report interval");
        }
        long seq = Long.parseLong(seqRaw);
        if (seq < 0) throw new IllegalArgumentException("negative sequence");
        return new Envelope(vehicleId, bootId, seq, kind,
                record.value(), record.topic(), stream == null ? "legacy" : stream, interval,
                stream == null ? bootId : sourceBoot).withSource(record);
    }

    /** Preserve malformed transport records for DLQ without admitting their identity to dedup. */
    public static Envelope invalid(ConsumerRecord<byte[], byte[]> record, String reason) {
        var env = new Envelope(null, null, -1, null, record.value(), record.topic(), "invalid", 0, null);
        env.validationError = reason;
        return env.withSource(record);
    }

    private Envelope withSource(ConsumerRecord<byte[], byte[]> record) {
        sourcePartition = record.partition();
        sourceOffset = record.offset();
        sourceKey = record.key() == null ? null : record.key().clone();
        for (var h : record.headers()) {
            sourceHeaders.add(new HeaderValue(h.key(), h.value() == null ? null : h.value().clone()));
        }
        return this;
    }

    public Integer sourcePartition() { return sourcePartition; }
    public Long sourceOffset() { return sourceOffset; }
    public byte[] sourceKey() { return sourceKey; }
    public List<HeaderValue> sourceHeaders() { return sourceHeaders == null ? List.of() : sourceHeaders; }
    public String validationError() { return validationError; }

    public String streamId() { return streamId == null ? "legacy" : streamId; }
    public long reportIntervalMs() { return reportIntervalMs; }
    public String sourceBootId() { return sourceBootId == null ? bootId : sourceBootId; }
    public boolean mqtt() { return !"legacy".equals(streamId()); }
    public String dedupKey() {
        if (validationError != null) return "invalid\u0000" + topic + "\u0000" + sourcePartition;
        return mqtt() ? vehicleId + "\u0000" + bootId + "\u0000" + streamId() : vehicleId;
    }

    /** 헤더 하나를 문자열로. 없으면 null. */
    private static String header(ConsumerRecord<byte[], byte[]> r, String key) {
        var h = r.headers().lastHeader(key);
        return h == null || h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    public String vehicleId() {
        return vehicleId;
    }

    public String bootId() {
        return bootId;
    }

    public long seq() {
        return seq;
    }

    public String kind() {
        return kind;
    }

    public byte[] payload() {
        return payload;
    }

    public String topic() {
        return topic;
    }

    @Override
    public String toString() {
        return "Envelope[%s/%s/%d %s %dB]".formatted(vehicleId, bootId, seq, kind,
                payload == null ? 0 : payload.length);
    }
}
