package io.fleetsentinel.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record VehicleSummary(
        @JsonProperty("vehicle_id") String vehicleId,
        @JsonProperty("scene_id") String sceneId,
        @JsonProperty("scene_name") String sceneName,
        String location,
        @JsonProperty("n_records") long recordCount,
        @JsonProperty("duration_ms") long durationMs,
        List<Double> home) {
    public record Roster(List<VehicleSummary> vehicles) {}
}
