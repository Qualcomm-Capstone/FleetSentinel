package io.fleetsentinel.pipeline.model;

import java.io.Serializable;
import lombok.Getter;
import lombok.experimental.Accessors;

/** 인증된 전송 식별자와 검증된 페이로드. 탐지와 저장에서 함께 사용한다. */
@Getter
@Accessors(fluent = true)
public final class Decoded implements Serializable {
    private static final long serialVersionUID = 1L;
    public enum Kind { SIGNAL, PERCEPTION, SEGMENT }

    private final String vehicleId;
    private final String bootId;
    private final String sourceBootId;
    private final long seq;
    private final String streamId;
    private final long reportIntervalMs;
    private final TelemetryData data;

    public Decoded(Envelope envelope, TelemetryData data) {
        this.vehicleId = envelope.vehicleId();
        this.bootId = envelope.bootId();
        this.sourceBootId = envelope.sourceBootId();
        this.seq = envelope.seq();
        this.streamId = envelope.streamId();
        this.reportIntervalMs = envelope.reportIntervalMs();
        this.data = data;
    }

    public Kind kind() { return data.kind(); }
    public String sourceBootId() { return sourceBootId == null ? bootId : sourceBootId; }
    public Double latitude() { return data.latitude(); }
    public Double longitude() { return data.longitude(); }

    public long eventTimeMillis() {
        if (data.eventTime() == null) throw new IllegalStateException("event time이 없다: " + kind());
        return data.eventTime().toEpochMilli();
    }

    public long freshnessMs(long legacyDefault) {
        return Math.max(legacyDefault, reportIntervalMs * 3);
    }

    /** history는 저장 전용이다. 실시간 관측의 시각에는 차량·서버 간 시계 동기화가 필요하다. */
    public boolean liveAt(long nowMs) {
        if ("history".equals(streamId)) return false;
        if (streamId == null || "legacy".equals(streamId)) return true;
        long age = nowMs - eventTimeMillis();
        return age >= -5_000 && age < freshnessMs(5_000);
    }

    public boolean isEgoPose() {
        return data instanceof TelemetryData.Signal signal && "ego_pose".equals(signal.channel());
    }

    public boolean isVehicleMonitor() {
        return data instanceof TelemetryData.Signal signal && "fleet_status".equals(signal.channel());
    }

    public Double signalNumber(String name) {
        return data instanceof TelemetryData.Signal signal ? signal.numbers().get(name) : null;
    }

    public String signalString(String name) {
        return data instanceof TelemetryData.Signal signal ? signal.strings().get(name) : null;
    }

    public String signalLocation() {
        return signalString("location");
    }

    public int numLidarPoints() {
        if (data instanceof TelemetryData.Perception perception) return perception.numLidarPoints();
        throw new IllegalStateException("객체 메타데이터 레코드가 아니다: " + kind());
    }
}
