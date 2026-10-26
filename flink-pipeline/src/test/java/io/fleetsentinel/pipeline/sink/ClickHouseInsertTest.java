package io.fleetsentinel.pipeline.sink;

import static org.assertj.core.api.Assertions.assertThat;

import io.fleetsentinel.pipeline.model.Decoded;
import io.fleetsentinel.pipeline.model.Envelope;
import io.fleetsentinel.pipeline.source.TelemetryRecordMapper;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

/** Avro → 타입 모델 → Flink 직렬화 → INSERT의 모든 컬럼을 대조한다. */
class ClickHouseInsertTest {
    private final Envelope envelope = new Envelope("v1", "boot", 7, "test", new byte[0], "test");
    private static final long MICROS = 1_700_000_000_123456L;
    private static final Timestamp TIME = Timestamp.from(Instant.parse("2023-11-14T22:13:20.123456Z"));

    @Test
    void signalColumnsKeepTypedValuesAndNullCoordinates() throws Exception {
        GenericRecord record = record("vehicle-signal.avsc");
        record.put("channel", new Utf8("ego_pose"));
        record.put("values_num", Map.of(new Utf8("speed"), 3.5));
        record.put("values_vec", Map.of(new Utf8("translation"), List.of(1.0, 2.0, 3.0)));
        record.put("values_str", Map.of(new Utf8("location"), new Utf8("test")));
        Decoded decoded = roundTrip(TelemetryRecordMapper.signal(envelope, record, 0.0, null));
        assertThat(decoded.isEgoPose()).isTrue();
        assertThat(decoded.signalNumber("speed")).isEqualTo(3.5);
        assertThat(decoded.signalLocation()).isEqualTo("test");
        assertColumns(decoded, fields(
                "scene_id", "scene_id", "channel", "ego_pose", "sensor_time", TIME, "log_time", TIME,
                "values_num", Map.of("speed", 3.5), "values_vec", Map.of("translation", List.of(1.0, 2.0, 3.0)),
                "values_str", Map.of("location", "test"), "lat", 0.0, "lon", null));
    }

    @Test
    void perceptionColumnsKeepGeometryAndNullableAttribute() throws Exception {
        GenericRecord record = record("perception-object.avsc");
        record.put("attribute", null);
        Decoded decoded = roundTrip(TelemetryRecordMapper.perception(envelope, record, null, 0.0));
        assertThat(decoded.numLidarPoints()).isEqualTo(17);
        assertColumns(decoded, fields(
                "scene_id", "scene_id", "sample_id", "sample_id", "sensor_time", TIME,
                "track_id", "track_id", "category", "category", "attribute", null,
                "center_x", 6.25, "center_y", 7.25, "center_z", 8.25,
                "size_w", 9.25, "size_l", 10.25, "size_h", 11.25,
                "rot_w", 12.25, "rot_x", 13.25, "rot_y", 14.25, "rot_z", 15.25,
                "visibility", "visibility", "num_lidar_pts", 17, "num_radar_pts", 18,
                "lat", null, "lon", 0.0));
    }

    @Test
    void segmentColumnsKeepTimesAndCalibration() throws Exception {
        GenericRecord record = record("segment-ref.avsc");
        record.put("scene_id", null);
        record.put("drop_reason", null);
        record.put("size_bytes", 1024L);
        record.put("calibration", Map.of(new Utf8("CAM"), new Utf8("{}")));
        Decoded decoded = roundTrip(TelemetryRecordMapper.segment(envelope, record));
        assertColumns(decoded, fields(
                "segment_id", "segment_id", "scene_id", null, "blob_uri", "blob_uri",
                "t_start", TIME, "t_end", TIME, "sensor_channels", List.of("CAM"),
                "size_bytes", 1024L, "checksum", "checksum", "sample_count", 8,
                "state", "UPLOADED", "drop_reason", null, "calibration", Map.of("CAM", "{}")));
    }

    private static Decoded roundTrip(Decoded input) throws Exception {
        var serializer = TypeInformation.of(Decoded.class).createSerializer(new ExecutionConfig().getSerializerConfig());
        var bytes = new DataOutputSerializer(1024);
        serializer.serialize(input, bytes);
        Decoded output = serializer.deserialize(new DataInputDeserializer(bytes.getCopyOfBuffer()));
        assertThat(output.eventTimeMillis()).isEqualTo(TIME.getTime());
        return output;
    }

    private static void assertColumns(Decoded row, Map<String, Object> expected) throws Exception {
        ClickHouseInsert insert = ClickHouseInsert.forKind(row.kind());
        String columns = insert.sql().substring(insert.sql().indexOf('(') + 1, insert.sql().indexOf(')'));
        String[] names = columns.split(",\\s*");
        Map<String, Object> actual = new LinkedHashMap<>();
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                ClickHouseInsertTest.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
                (proxy, method, args) -> {
                    if (method.getName().startsWith("set")) {
                        actual.put(names[(int) args[0] - 1].trim(), args[1]);
                        return null;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        insert.bind(statement, row);
        expected.put("vehicle_id", "v1");
        expected.put("boot_id", "boot");
        expected.put("seq", 7L);
        assertThat(actual).containsExactlyInAnyOrderEntriesOf(expected);
        assertThat(insert.sql().chars().filter(c -> c == '?').count()).isEqualTo(names.length);
    }

    private static GenericRecord record(String filename) throws Exception {
        try (var stream = ClickHouseInsertTest.class.getResourceAsStream("/schemas/" + filename)) {
            Schema schema = new Schema.Parser().parse(stream);
            var record = new GenericData.Record(schema);
            for (var field : schema.getFields()) {
                Schema type = field.schema();
                if (type.getType() == Schema.Type.UNION) type = type.getTypes().get(1);
                Object value = switch (type.getType()) {
                    case STRING -> new Utf8(field.name());
                    case LONG -> MICROS;
                    case INT -> field.pos();
                    case DOUBLE -> field.pos() + 0.25;
                    case MAP -> Map.of();
                    case ARRAY -> List.of(new Utf8("CAM"));
                    case ENUM -> new GenericData.EnumSymbol(type, "UPLOADED");
                    default -> throw new IllegalArgumentException(type.toString());
                };
                record.put(field.name(), value);
            }
            return record;
        }
    }

    private static Map<String, Object> fields(Object... pairs) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) result.put((String) pairs[i], pairs[i + 1]);
        return result;
    }
}
