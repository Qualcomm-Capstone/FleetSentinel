package io.fleetsentinel.gateway.clip;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.fleetsentinel.gateway.config.ObjectStoreProperties;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/** MQTT operation body contract. Explicit names keep it independent of global Jackson configuration. */
public final class ClipApi {
    private static final long MAX_TIMESTAMP_US = java.time.Instant.parse("2300-01-01T00:00:00Z").getEpochSecond() * 1_000_000L;
    private ClipApi() { }

    public record Create(@JsonProperty("request_id") String requestId,
                         @JsonProperty("t_start_us") long tStartUs,
                         @JsonProperty("t_end_us") long tEndUs,
                         @JsonProperty("size_bytes") long sizeBytes,
                         String sha256,
                         @JsonProperty("part_size_bytes") long partSizeBytes,
                         @JsonProperty("scene_id") String sceneId,
                         @JsonProperty("sensor_channels") List<String> sensorChannels,
                         @JsonProperty("sample_count") int sampleCount,
                         Map<String, String> calibration) {
        public void validate(ObjectStoreProperties props) {
            require(requestId != null && !requestId.isBlank() && requestId.length() <= 160,
                    "request_id must contain 1..160 characters");
            require(tStartUs > 0 && tEndUs >= tStartUs && tEndUs < MAX_TIMESTAMP_US,
                    "clip times must be epoch microseconds before 2300-01-01 UTC");
            require(sizeBytes > 0 && sizeBytes <= 5L * 1024 * 1024 * 1024 * 1024,
                    "size_bytes must be within 1..5TiB");
            require(sha256 != null && sha256.matches("[0-9a-f]{64}"), "sha256 must be lowercase hex");
            require(partSizeBytes > 0 && partSizeBytes <= 5L * 1024 * 1024 * 1024,
                    "invalid part_size_bytes");
            require(sizeBytes <= partSizeBytes || partSizeBytes >= props.getMinPartSizeBytes(),
                    "nonfinal parts must be at least 5MiB");
            require(1 + (sizeBytes - 1) / partSizeBytes <= props.getMaxParts(), "too many parts");
            require(sampleCount > 0, "sample_count must be positive");
            require(sceneId == null || sceneId.length() <= 256, "scene_id too long");
            require(sensorChannels != null && sensorChannels.size() <= 256
                            && sensorChannels.stream().allMatch(s -> s != null && !s.isBlank() && s.length() <= 256),
                    "invalid sensor_channels");
            require(calibration != null && calibration.size() <= 256, "invalid calibration");
            int length = 0;
            for (var entry : calibration.entrySet()) {
                require(entry.getKey() != null && entry.getKey().length() <= 256 && entry.getValue() != null,
                        "invalid calibration entry");
                length += entry.getValue().length();
                require(length <= 512 * 1024, "calibration exceeds 512KiB");
                try {
                    require(ClipJson.MAPPER.readTree(entry.getValue()) != null, "empty calibration JSON");
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException("calibration values must be JSON strings", e);
                }
            }
        }
        public int partCount() { return Math.toIntExact(1 + (sizeBytes - 1) / partSizeBytes); }
    }

    public record View(String id, ClipState state,
                       @JsonProperty("upload_id") String uploadId,
                       @JsonProperty("blob_uri") String blobUri,
                       @JsonProperty("size_bytes") long sizeBytes,
                       String sha256,
                       @JsonProperty("part_size_bytes") long partSizeBytes, String error) { }
    public record Part(@JsonProperty("part_number") int partNumber, String etag) { }
    public record Complete(@JsonProperty("upload_id") String uploadId, List<Part> parts) { }
    public record UrlRequest(@JsonProperty("part_numbers") List<Integer> partNumbers) { }
    public record PartUrl(@JsonProperty("part_number") int partNumber, String url) { }
    public record Urls(@JsonProperty("upload_id") String uploadId,
                       @JsonProperty("part_urls") List<PartUrl> partUrls,
                       @JsonProperty("expires_at_us") long expiresAtUs) { }

    public static void validateParts(List<Part> parts, int count) {
        require(parts != null && parts.size() == count, "parts must cover the complete clip");
        var seen = new HashSet<Integer>();
        for (var part : parts) {
            require(part != null && part.partNumber() >= 1 && part.partNumber() <= count
                            && seen.add(part.partNumber()) && part.etag() != null
                            && !part.etag().isBlank() && part.etag().length() <= 256
                            && part.etag().chars().noneMatch(c -> c < 32),
                    "invalid or duplicate part");
        }
    }

    static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }
}
