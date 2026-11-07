package io.fleetsentinel.gateway.clip;

import static org.assertj.core.api.Assertions.*;

import io.fleetsentinel.gateway.config.ObjectStoreProperties;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ClipRequestTest {
    private final ClipJson json = new ClipJson();
    private final ObjectStoreProperties props = new ObjectStoreProperties();

    @ParameterizedTest
    @ValueSource(longs = {Long.MAX_VALUE, 1_800_000_000_000_000_000L, 10_413_792_000_000_000L})
    void rejectsNanosecondsAndTimestampsOutsideCatalogRange(long timestamp) {
        assertThatThrownBy(() -> new ClipApi.Create("time", timestamp, timestamp, 10, "0".repeat(64),
                10, null, List.of(), 1, Map.of()).validate(props)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fingerprintIsIndependentOfMapOrderingButIncludesMetadata() {
        var a = request(Map.of("a", "{}", "b", "[]"));
        var b = request(new java.util.LinkedHashMap<>(Map.of("b", "[]", "a", "{}")));
        assertThat(json.fingerprint(a)).isEqualTo(json.fingerprint(b));
        assertThat(json.fingerprint(a)).isNotEqualTo(json.fingerprint(request(Map.of("a", "[]"))));
    }

    @Test
    void validatesS3AndMetadataLimitsBeforePersisting() {
        request(Map.of()).validate(props);
        assertThatThrownBy(() -> new ClipApi.Create("request", 1, 2, 10, "bad", 10,
                null, List.of(), 1, Map.of()).validate(props)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ClipApi.Create("request", 1, 2, 10, "0".repeat(64), 5,
                null, List.of(), 1, Map.of()).validate(props)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> request(Map.of("lidar", "not json")).validate(props))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void partListMustCoverEveryPartExactlyOnce() {
        ClipApi.validateParts(List.of(new ClipApi.Part(1, "etag")), 1);
        assertThatThrownBy(() -> ClipApi.validateParts(List.of(new ClipApi.Part(2, "etag")), 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ClipApi.validateParts(List.of(new ClipApi.Part(1, "a"),
                new ClipApi.Part(1, "b")), 2)).isInstanceOf(IllegalArgumentException.class);
    }

    private ClipApi.Create request(Map<String, String> calibration) {
        return new ClipApi.Create("request", 1, 2, 10, "0".repeat(64), 10,
                null, List.of("lidar"), 1, calibration);
    }
}
