package io.fleetsentinel.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

/** 고정 필드는 타입으로 정의하고 채널별 가변 센서 값만 Map으로 보관한다. */
public record SignalSample(
        @JsonProperty("vehicle_id") String vehicleId,
        long seq,
        @JsonProperty("c") String channel,
        @JsonProperty("t") long time,
        @JsonProperty("v") Map<String, Object> values) implements VehicleObservation {
    public record Batch(
            @JsonProperty("vehicle_id") String vehicleId,
            @JsonProperty("t") long time,
            @JsonProperty("n") int count,
            List<SignalSample> records) {
        public static Batch of(List<SignalSample> records) {
            SignalSample first = records.getFirst();
            return new Batch(first.vehicleId(), first.time(), records.size(), records);
        }
    }
}
