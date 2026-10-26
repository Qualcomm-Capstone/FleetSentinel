package io.fleetsentinel.pipeline.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

/** 검증된 페이로드. 저장 컬럼 순서와 독립적인 필드 이름·타입을 사용한다. */
public sealed interface TelemetryData extends Serializable {
    Decoded.Kind kind();
    Instant eventTime();
    default Double latitude() { return null; }
    default Double longitude() { return null; }

    @Getter
    @Accessors(fluent = true)
    @RequiredArgsConstructor
    final class Signal implements TelemetryData {
        private static final long serialVersionUID = 1L;
        private final String sceneId;
        private final String channel;
        private final Instant sensorTime;
        private final Instant logTime;
        private final Map<String, Double> numbers;
        private final Map<String, List<Double>> vectors;
        private final Map<String, String> strings;
        private final Double latitude;
        private final Double longitude;

        public Decoded.Kind kind() { return Decoded.Kind.SIGNAL; }
        public Instant eventTime() { return sensorTime; }
    }

    @Getter
    @Accessors(fluent = true)
    @RequiredArgsConstructor
    final class Perception implements TelemetryData {
        private static final long serialVersionUID = 1L;
        private final String sceneId;
        private final String sampleId;
        private final Instant sensorTime;
        private final String trackId;
        private final String category;
        private final String attribute;
        private final double centerX;
        private final double centerY;
        private final double centerZ;
        private final double sizeW;
        private final double sizeL;
        private final double sizeH;
        private final double rotW;
        private final double rotX;
        private final double rotY;
        private final double rotZ;
        private final String visibility;
        private final int numLidarPoints;
        private final int numRadarPoints;
        private final Double latitude;
        private final Double longitude;

        public Decoded.Kind kind() { return Decoded.Kind.PERCEPTION; }
        public Instant eventTime() { return sensorTime; }
    }

    @Getter
    @Accessors(fluent = true)
    @RequiredArgsConstructor
    final class Segment implements TelemetryData {
        private static final long serialVersionUID = 1L;
        private final String segmentId;
        private final String sceneId;
        private final String blobUri;
        private final Instant start;
        private final Instant end;
        private final List<String> sensorChannels;
        private final long sizeBytes;
        private final String checksum;
        private final int sampleCount;
        private final String state;
        private final String dropReason;
        private final Map<String, String> calibration;

        public Decoded.Kind kind() { return Decoded.Kind.SEGMENT; }
        public Instant eventTime() { return start; }
    }
}
