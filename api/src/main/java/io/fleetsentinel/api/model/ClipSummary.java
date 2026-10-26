package io.fleetsentinel.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record ClipSummary(
        @JsonProperty("clip_id") String clipId,
        @JsonProperty("vehicle_id") String vehicleId,
        @JsonProperty("scene_name") String sceneName,
        @JsonProperty("blob_uri") String blobUri,
        @JsonProperty("duration_s") double durationSeconds,
        @JsonProperty("size_bytes") long sizeBytes,
        @JsonProperty("sample_count") int sampleCount,
        String state,
        @JsonProperty("sensor_channels") List<String> sensorChannels) {}
