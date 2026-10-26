package io.fleetsentinel.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** 결번은 재전송·적재 지연 중에도 발생할 수 있으므로 유실 확정값과 구분한다. */
public record PipelineHealth(
        Counts counts,
        List<Progress> vehicles,
        @JsonProperty("total_missing") long totalMissing,
        @JsonProperty("dup_pressure") List<DuplicatePressure> duplicatePressure) {
    public record Counts(long signals, long perception, long segments) {}
    public record Progress(
            @JsonProperty("vehicle_id") String vehicleId,
            @JsonProperty("boot_id") String bootId,
            long received, long expected, long missing) {}
    public record DuplicatePressure(
            String table,
            @JsonProperty("rows_raw") long rawRows,
            @JsonProperty("pending_dupes") long pendingDuplicates) {}
}
