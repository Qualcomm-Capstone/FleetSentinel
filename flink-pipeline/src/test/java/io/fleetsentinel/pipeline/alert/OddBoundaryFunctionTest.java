package io.fleetsentinel.pipeline.alert;

import io.fleetsentinel.pipeline.source.TelemetryRecordMapper;

import static org.assertj.core.api.Assertions.assertThat;

import io.fleetsentinel.pipeline.alert.AlertEvent.Type;
import io.fleetsentinel.pipeline.model.Decoded;
import io.fleetsentinel.pipeline.model.Envelope;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class OddBoundaryFunctionTest {

    private static final Schema SIGNAL_SCHEMA = loadSchema();

    @Test
    void emitsOneExitThenSustainedAndRecovery() throws Exception {
        var function = new OddBoundaryFunction(
                Map.of("test-map", new GeoBounds(1.0, 2.0, 3.0, 4.0)), 30_000);
        var operator = new KeyedProcessOperator<>(function);

        try (var harness = new KeyedOneInputStreamOperatorTestHarness<>(
                operator, Decoded::vehicleId, Types.STRING)) {
            harness.open();
            harness.setProcessingTime(1_000);
            harness.processElement(new StreamRecord<>(pose(1, 1_000, 1.5, 3.5)));
            harness.processElement(new StreamRecord<>(pose(2, 2_000, 2.1, 3.5)));
            harness.processElement(new StreamRecord<>(pose(3, 3_000, 2.2, 3.5)));

            assertThat(types(harness)).containsExactly(Type.ODD_EXITED);

            for (int i = 4; i <= 32; i++) {
                harness.setProcessingTime(i * 1_000L - 2_000);
                harness.processElement(new StreamRecord<>(pose(i, i * 1_000L, 2.2, 3.5)));
            }
            harness.setProcessingTime(31_000);
            assertThat(types(harness)).containsExactly(Type.ODD_EXITED, Type.ODD_SUSTAINED);

            harness.processElement(new StreamRecord<>(pose(33, 33_000, 1.5, 3.5)));
            assertThat(types(harness)).containsExactly(
                    Type.ODD_EXITED, Type.ODD_SUSTAINED, Type.ODD_RETURNED);
        }
    }

    @Test
    void latePositionCannotReverseCurrentState() throws Exception {
        var function = new OddBoundaryFunction(
                Map.of("test-map", new GeoBounds(1.0, 2.0, 3.0, 4.0)), 30_000);
        var operator = new KeyedProcessOperator<>(function);

        try (var harness = new KeyedOneInputStreamOperatorTestHarness<>(
                operator, Decoded::vehicleId, Types.STRING)) {
            harness.open();
            harness.processElement(new StreamRecord<>(pose(1, 1_000, 1.5, 3.5)));
            harness.processElement(new StreamRecord<>(pose(3, 3_000, 2.1, 3.5)));
            harness.processElement(new StreamRecord<>(pose(2, 2_000, 1.5, 3.5)));

            assertThat(types(harness)).containsExactly(Type.ODD_EXITED);
        }
    }

    @Test void silenceMustNotBeReportedAsSustainedExcursion() throws Exception {
        var operator = new KeyedProcessOperator<>(new OddBoundaryFunction(
                Map.of("test-map", new GeoBounds(1, 2, 3, 4)), 30_000));
        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(operator, Decoded::vehicleId, Types.STRING)) {
            h.open(); h.setProcessingTime(1_000);
            h.processElement(new StreamRecord<>(pose(1, 1_000, 2.1, 3.5)));
            h.setProcessingTime(31_000);
            assertThat(types(h)).containsExactly(Type.ODD_EXITED);
        }
    }

    private static List<Type> types(KeyedOneInputStreamOperatorTestHarness<String, Decoded, AlertEvent> harness) {
        return harness.extractOutputValues().stream().map(AlertEvent::type).toList();
    }

    private static Decoded pose(long seq, long eventTimeMs, double latitude, double longitude) {
        var record = new GenericData.Record(SIGNAL_SCHEMA);
        record.put("scene_id", "scene-1");
        record.put("channel", "ego_pose");
        record.put("sensor_time", eventTimeMs * 1_000);
        record.put("log_time", eventTimeMs * 1_000);
        record.put("values_num", Map.of());
        record.put("values_vec", Map.of());
        record.put("values_str", Map.of("location", "test-map"));
        var envelope = new Envelope("vehicle-1", "boot-1", seq, "signal", new byte[0], "test");
        return TelemetryRecordMapper.signal(envelope, record, latitude, longitude);
    }

    private static Schema loadSchema() {
        try (var input = OddBoundaryFunctionTest.class.getResourceAsStream("/schemas/vehicle-signal.avsc")) {
            if (input == null) {
                throw new IllegalStateException("vehicle-signal.avsc를 찾지 못했다");
            }
            return new Schema.Parser().parse(input);
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
