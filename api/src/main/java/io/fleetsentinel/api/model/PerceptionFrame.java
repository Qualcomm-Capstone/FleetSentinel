package io.fleetsentinel.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public record PerceptionFrame(
        @JsonProperty("vehicle_id") String vehicleId,
        @JsonProperty("t") long time,
        @JsonProperty("n_objects") int objectCount,
        @JsonProperty("n_zero_lidar") int zeroLidarCount,
        Map<String, Integer> classes,
        List<Box> boxes) implements VehicleObservation {
    public record Box(
            @JsonProperty("c") List<Double> center,
            @JsonProperty("s") List<Double> size,
            double yaw,
            @JsonProperty("cat") String category,
            @JsonProperty("lp") int lidarPoints,
            @JsonProperty("vis") String visibility) {}
}
