package io.fleetsentinel.pipeline.source;

import io.fleetsentinel.pipeline.model.Decoded;
import io.fleetsentinel.pipeline.model.Envelope;
import io.fleetsentinel.pipeline.model.TelemetryData;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.avro.generic.GenericRecord;

/** Avro Utf8·마이크로초 시각을 도메인 타입으로 변환한다. */
public final class TelemetryRecordMapper {
    private TelemetryRecordMapper() {}

    public static Decoded signal(Envelope envelope, GenericRecord record, Double latitude, Double longitude) {
        return new Decoded(envelope, new TelemetryData.Signal(
                text(record, "scene_id"), text(record, "channel"),
                instant(record, "sensor_time"), instant(record, "log_time"),
                map(record, "values_num", value -> ((Number) value).doubleValue()),
                map(record, "values_vec", TelemetryRecordMapper::vector),
                map(record, "values_str", Object::toString), latitude, longitude));
    }

    public static Decoded perception(Envelope envelope, GenericRecord record, Double latitude, Double longitude) {
        return new Decoded(envelope, new TelemetryData.Perception(
                text(record, "scene_id"), text(record, "sample_id"), instant(record, "sensor_time"),
                text(record, "track_id"), text(record, "category"), text(record, "attribute"),
                number(record, "center_x"), number(record, "center_y"), number(record, "center_z"),
                number(record, "size_w"), number(record, "size_l"), number(record, "size_h"),
                number(record, "rot_w"), number(record, "rot_x"), number(record, "rot_y"), number(record, "rot_z"),
                text(record, "visibility"), ((Number) record.get("num_lidar_pts")).intValue(),
                ((Number) record.get("num_radar_pts")).intValue(), latitude, longitude));
    }

    public static Decoded segment(Envelope envelope, GenericRecord record) {
        List<String> channels = new ArrayList<>();
        for (Object channel : (List<?>) record.get("sensor_channels")) channels.add(channel.toString());
        return new Decoded(envelope, new TelemetryData.Segment(
                text(record, "segment_id"), text(record, "scene_id"), text(record, "blob_uri"),
                instant(record, "t_start"), instant(record, "t_end"), channels,
                ((Number) record.get("size_bytes")).longValue(), text(record, "checksum"),
                ((Number) record.get("sample_count")).intValue(), text(record, "state"),
                text(record, "drop_reason"), map(record, "calibration", Object::toString)));
    }

    private static String text(GenericRecord record, String field) {
        Object value = record.get(field);
        return value == null ? null : value.toString();
    }

    private static double number(GenericRecord record, String field) {
        return ((Number) record.get(field)).doubleValue();
    }

    private static Instant instant(GenericRecord record, String field) {
        Object value = record.get(field);
        if (!(value instanceof Number number)) return null;
        long micros = number.longValue();
        return Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L),
                Math.floorMod(micros, 1_000_000L) * 1000);
    }

    private static <T> Map<String, T> map(GenericRecord record, String field, Function<Object, T> convert) {
        Map<String, T> result = new LinkedHashMap<>();
        if (record.get(field) instanceof Map<?, ?> values) {
            values.forEach((key, value) -> result.put(key.toString(), convert.apply(value)));
        }
        return result;
    }

    private static List<Double> vector(Object value) {
        List<Double> result = new ArrayList<>();
        for (Object component : (List<?>) value) result.add(((Number) component).doubleValue());
        return result;
    }
}
