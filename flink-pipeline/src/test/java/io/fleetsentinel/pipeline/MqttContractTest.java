package io.fleetsentinel.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.fleetsentinel.pipeline.alert.AlertEvent;
import io.fleetsentinel.pipeline.alert.VehicleMonitorFunction;
import io.fleetsentinel.pipeline.dedup.DedupFunction;
import io.fleetsentinel.pipeline.model.Decoded;
import io.fleetsentinel.pipeline.model.Envelope;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class MqttContractTest {
    static Envelope envelope(String stream, String boot, long seq, int interval) {
        return envelope(stream, boot, seq, interval, "source-boot");
    }

    static Envelope envelope(String stream, String boot, long seq, int interval, String sourceBoot) {
        var record = new ConsumerRecord<byte[], byte[]>("test", 0, seq, "v1".getBytes(), new byte[0]);
        Map.of("vehicle_id", "v1", "boot_id", boot, "seq", Long.toString(seq),
                "kind", "RECORD_KIND_SIGNAL", "stream_id", stream,
                "report_interval_ms", Integer.toString(interval), "source_boot_id", sourceBoot)
                .forEach((key, value) -> record.headers().add(key, value.getBytes(StandardCharsets.UTF_8)));
        return Envelope.of(record);
    }

    static Decoded signal(Envelope env, long time) throws Exception {
        return signal(env, time, 3.0);
    }

    static Decoded signal(Envelope env, long time, double speed) throws Exception {
        try (var input = MqttContractTest.class.getResourceAsStream("/schemas/vehicle-signal.avsc")) {
            var r = new GenericData.Record(new Schema.Parser().parse(input));
            r.put("scene_id", "test"); r.put("channel", "fleet_status");
            r.put("sensor_time", time * 1000); r.put("log_time", time * 1000);
            r.put("values_num", Map.of("speed_mps", speed, "mission_active", 1.0,
                    "planned_stop", 0.0, "sensor_healthy", 1.0));
            r.put("values_vec", Map.of()); r.put("values_str", Map.of("mission_id", "m1"));
            return Decoded.signal(env, r, null, null);
        }
    }

    @Test void streamAndBootHaveIndependentDedupNamespaces() throws Exception {
        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new DedupFunction(8)), Envelope::dedupKey, Types.STRING)) {
            h.open();
            var status = envelope("status", "a", 0, 1000);
            var history = envelope("history", "b", 0, 1000);
            h.processElement(new StreamRecord<>(status));
            h.processElement(new StreamRecord<>(history));
            h.processElement(new StreamRecord<>(status));
            h.processElement(new StreamRecord<>(envelope("history", "b", 20, 1000)));
            // Old but previously unseen data must still reach the idempotent storage sink.
            h.processElement(new StreamRecord<>(envelope("history", "b", 1, 1000)));
            assertThat(h.extractOutputValues()).hasSize(4);
        }
    }

    @Test void historyAndStaleReplayAreNotLiveObservations() throws Exception {
        assertThat(signal(envelope("history", "a", 0, 10000), 1000).liveAt(1100)).isFalse();
        var status = signal(envelope("status", "b", 0, 10000), 1000);
        assertThat(status.liveAt(1100)).isTrue();
        assertThat(status.liveAt(40000)).isFalse();
        assertThat(status.liveAt(-10000)).isFalse();
        assertThatThrownBy(() -> envelope("status", "b", 0, 999999)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void watchdogRespectsLowFrequencyReportingAndIgnoresHistory() throws Exception {
        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new VehicleMonitorFunction(3000, 2000)), Decoded::vehicleId, Types.STRING)) {
            h.open(); h.setProcessingTime(1000);
            h.processElement(new StreamRecord<>(signal(envelope("status", "a", 0, 10000), 1000)));
            h.setProcessingTime(6000);
            assertThat(h.extractOutputValues()).isEmpty();
            h.setProcessingTime(30000);
            h.processElement(new StreamRecord<>(signal(envelope("history", "b", 0, 10000), 29000)));
            h.setProcessingTime(31000);
            assertThat(h.extractOutputValues()).extracting(AlertEvent::type)
                    .containsExactly(AlertEvent.Type.TELEMETRY_STALE);
        }
    }

    @Test void streamSwitchPreservesStopContinuityButSourceRestartResetsIt() throws Exception {
        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new VehicleMonitorFunction(3000, 5000)), Decoded::vehicleId, Types.STRING)) {
            h.open(); h.setProcessingTime(1000);
            h.processElement(new StreamRecord<>(signal(envelope("status", "storage-status", 0, 1000), 1000, 0)));
            h.setProcessingTime(4000);
            h.processElement(new StreamRecord<>(signal(envelope("event", "storage-event", 0, 1000), 4000, 0)));
            assertThat(h.extractOutputValues()).extracting(AlertEvent::type)
                    .containsExactly(AlertEvent.Type.UNPLANNED_STOP);
            assertThat(h.extractOutputValues().get(0).bootId()).isEqualTo("storage-event");
            h.setProcessingTime(4500);
            h.processElement(new StreamRecord<>(signal(envelope("status", "storage-status", 1, 1000), 4500, 0)));
            assertThat(h.extractOutputValues()).extracting(AlertEvent::type)
                    .containsExactly(AlertEvent.Type.UNPLANNED_STOP);
            h.setProcessingTime(5000);
            h.processElement(new StreamRecord<>(signal(envelope("event", "new-storage", 0, 1000, "restarted"), 5000, 0)));
            assertThat(h.extractOutputValues()).extracting(AlertEvent::type)
                    .containsExactly(AlertEvent.Type.UNPLANNED_STOP, AlertEvent.Type.STOP_CLEARED);
        }
    }

    @Test void sourceBootContinuitySurvivesCheckpointRestore() throws Exception {
        org.apache.flink.runtime.checkpoint.OperatorSubtaskState snapshot;
        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new VehicleMonitorFunction(3000, 5000)), Decoded::vehicleId, Types.STRING)) {
            h.open(); h.setProcessingTime(1000);
            h.processElement(new StreamRecord<>(signal(envelope("status", "storage-status", 0, 1000), 1000, 0)));
            snapshot = h.snapshot(1, 1000);
        }
        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new VehicleMonitorFunction(3000, 5000)), Decoded::vehicleId, Types.STRING)) {
            h.initializeState(snapshot); h.open(); h.setProcessingTime(4000);
            h.processElement(new StreamRecord<>(signal(envelope("event", "storage-event", 0, 1000), 4000, 0)));
            assertThat(h.extractOutputValues()).extracting(AlertEvent::type)
                    .containsExactly(AlertEvent.Type.UNPLANNED_STOP);
        }
    }
}
