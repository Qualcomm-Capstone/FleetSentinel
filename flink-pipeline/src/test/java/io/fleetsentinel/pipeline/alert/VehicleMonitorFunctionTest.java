package io.fleetsentinel.pipeline.alert;

import io.fleetsentinel.pipeline.source.TelemetryRecordMapper;

import static org.assertj.core.api.Assertions.assertThat;
import io.fleetsentinel.pipeline.model.Decoded;
import io.fleetsentinel.pipeline.model.Envelope;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class VehicleMonitorFunctionTest {
    private KeyedOneInputStreamOperatorTestHarness<String, Decoded, AlertEvent> harness() throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new VehicleMonitorFunction(3_000, 2_000)),
                Decoded::vehicleId, Types.STRING);
    }

    @Test void stopRequiresContinuousObservationsAndExcludesPlannedStops() throws Exception {
        try (var h = harness()) {
            h.open(); h.setProcessingTime(1_000);
            for (int i = 1; i <= 5; i++) {
                h.processElement(new StreamRecord<>(snapshot("moving", i, i * 1_000, 3, 0, 1)));
                h.processElement(new StreamRecord<>(snapshot("planned", i, i * 1_000, 0, 1, 1)));
                h.processElement(new StreamRecord<>(snapshot("stopped", i, i * 1_000, 0, 0, 1)));
            }
            assertThat(h.extractOutputValues()).hasSize(1);
            assertThat(h.extractOutputValues().get(0).type()).isEqualTo(AlertEvent.Type.UNPLANNED_STOP);
            assertThat(h.extractOutputValues().get(0).vehicleId()).isEqualTo("stopped");
            h.processElement(new StreamRecord<>(snapshot("stopped", 6, 6_000, 2, 0, 1)));
            assertThat(h.extractOutputValues().get(1).type()).isEqualTo(AlertEvent.Type.STOP_CLEARED);
        }
    }

    @Test void staleIsNotAStopAndLateInputDoesNotRecoverIt() throws Exception {
        try (var h = harness()) {
            h.open(); h.setProcessingTime(1_000);
            h.processElement(new StreamRecord<>(snapshot("v1", 1, 1_000, 0, 0, 1)));
            h.setProcessingTime(3_000);
            h.processElement(new StreamRecord<>(snapshot("v1", 2, 900, 0, 0, 1)));
            h.setProcessingTime(6_000);
            assertThat(h.extractOutputValues()).extracting(AlertEvent::type)
                    .containsExactly(AlertEvent.Type.TELEMETRY_STALE);
            h.processElement(new StreamRecord<>(snapshot("v1", 3, 7_000, 0, 0, 1)));
            assertThat(h.extractOutputValues()).extracting(AlertEvent::type)
                    .containsExactly(AlertEvent.Type.TELEMETRY_STALE, AlertEvent.Type.TELEMETRY_RECOVERED);
        }
    }

    @Test void sensorTransitionsAreNotRepeatedAndInvalidInputDoesNotRefreshWatchdog() throws Exception {
        try (var h = harness()) {
            h.open(); h.setProcessingTime(1_000);
            h.processElement(new StreamRecord<>(snapshot("v1", 1, 1_000, 3, 0, 0)));
            h.processElement(new StreamRecord<>(snapshot("v1", 2, 2_000, 3, 0, 0)));
            h.processElement(new StreamRecord<>(snapshot("v1", 3, 3_000, 3, 0, 1)));
            h.setProcessingTime(2_000);
            h.processElement(new StreamRecord<>(snapshot("v1", 4, 4_000, Double.NaN, 0, 1)));
            h.setProcessingTime(3_000);
            assertThat(h.extractOutputValues()).extracting(AlertEvent::type).containsExactly(
                    AlertEvent.Type.SENSOR_FAULT, AlertEvent.Type.SENSOR_RECOVERED, AlertEvent.Type.TELEMETRY_STALE);
        }
    }

    @Test void checkpointRestoresPendingTimerAndEventIdentity() throws Exception {
        try (var original = harness(); var restored = harness()) {
            original.open(); original.setProcessingTime(1_000);
            original.processElement(new StreamRecord<>(snapshot("v1", 1, 1_000, 0, 0, 1)));
            var checkpoint = original.snapshot(1, 1_100);
            restored.initializeState(checkpoint); restored.open();
            original.setProcessingTime(3_000); restored.setProcessingTime(3_000);
            assertThat(restored.extractOutputValues()).isEqualTo(original.extractOutputValues());
            assertThat(restored.extractOutputValues()).hasSize(1);
        }
    }

    @Test void eventTimeGapBreaksStoppedContinuityEvenWhenReplayedInOneBatch() throws Exception {
        try (var h = harness()) {
            h.open(); h.setProcessingTime(1_000);
            h.processElement(new StreamRecord<>(snapshot("v1", 1, 1_000, 0, 0, 1)));
            h.processElement(new StreamRecord<>(snapshot("v1", 2, 10_000, 0, 0, 1)));
            h.processElement(new StreamRecord<>(snapshot("v1", 3, 11_000, 0, 0, 1)));
            assertThat(h.extractOutputValues()).isEmpty();
        }
    }

    @Test void checkpointKeepsStoppedSinceAndDoesNotRepeatAnEmittedStop() throws Exception {
        try (var original = harness(); var restored = harness()) {
            original.open(); original.setProcessingTime(1_000);
            original.processElement(new StreamRecord<>(snapshot("v1", 1, 1_000, 0, 0, 1)));
            original.processElement(new StreamRecord<>(snapshot("v1", 2, 2_000, 0, 0, 1)));
            restored.initializeState(original.snapshot(1, 1_100)); restored.open();
            restored.setProcessingTime(1_200);
            restored.processElement(new StreamRecord<>(snapshot("v1", 3, 3_000, 0, 0, 1)));
            restored.processElement(new StreamRecord<>(snapshot("v1", 4, 4_000, 0, 0, 1)));
            restored.processElement(new StreamRecord<>(snapshot("v1", 5, 5_000, 0, 0, 1)));
            assertThat(restored.extractOutputValues()).extracting(AlertEvent::type)
                    .containsExactly(AlertEvent.Type.UNPLANNED_STOP);
        }
    }

    private static Decoded snapshot(String vehicle, long seq, long time, double speed,
                                    double planned, double health) throws Exception {
        try (var input = VehicleMonitorFunctionTest.class.getResourceAsStream("/schemas/vehicle-signal.avsc")) {
            var record = new GenericData.Record(new Schema.Parser().parse(input));
            record.put("scene_id", "test"); record.put("channel", "fleet_status");
            record.put("sensor_time", time * 1_000); record.put("log_time", time * 1_000);
            record.put("values_num", Map.of("speed_mps", speed, "mission_active", 1.0,
                    "planned_stop", planned, "sensor_healthy", health));
            record.put("values_vec", Map.of()); record.put("values_str", Map.of("mission_id", "mission-1"));
            return TelemetryRecordMapper.signal(new Envelope(vehicle, "boot-1", seq, "signal", new byte[0], "test"), record, null, null);
        }
    }
}
