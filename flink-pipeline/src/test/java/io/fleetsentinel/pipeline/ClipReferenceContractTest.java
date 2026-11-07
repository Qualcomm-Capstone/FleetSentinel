package io.fleetsentinel.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.fleetsentinel.pipeline.dedup.DedupFunction;
import io.fleetsentinel.pipeline.model.Envelope;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ClipReferenceContractTest {
    private static Envelope reference(String kind, String boot, long seq) {
        var record = new ConsumerRecord<byte[], byte[]>("telemetry.records", 0, 0,
                "v1".getBytes(StandardCharsets.UTF_8), new byte[0]);
        Map.of("vehicle_id", "v1", "boot_id", boot, "seq", Long.toString(seq),
                "kind", kind, "producer_origin", "clip-upload")
                .forEach((key, value) -> record.headers().add(key, value.getBytes(StandardCharsets.UTF_8)));
        return Envelope.of(record);
    }

    @Test
    void referenceRetriesDoNotResetOrConsumeSensorSequenceState() throws Exception {
        // Given an existing sensor sequence window.
        try (var harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new DedupFunction(8)), Envelope::dedupKey, Types.STRING)) {
            harness.open();
            var sensor = new Envelope("v1", "sensor-boot", 100, "RECORD_KIND_SIGNAL", new byte[0], "telemetry.records");
            var clip = reference("RECORD_KIND_SEGMENT_REF", "clip-one", 0);
            var secondClip = reference("RECORD_KIND_SEGMENT_REF", "clip-two", 0);
            harness.processElement(new StreamRecord<>(sensor));

            // When a clip and a delayed retry interleave with the same sensor record.
            harness.processElement(new StreamRecord<>(clip));
            harness.processElement(new StreamRecord<>(sensor));
            harness.processElement(new StreamRecord<>(secondClip));
            harness.processElement(new StreamRecord<>(clip));
            harness.processElement(new StreamRecord<>(sensor));

            // Then every clip reaches idempotent storage; the sensor is still emitted only once.
            assertThat(harness.extractOutputValues()).extracting(Envelope::bootId)
                    .containsExactly("sensor-boot", "clip-one", "clip-two", "clip-one");
            assertThat(harness.extractOutputValues().stream()
                    .filter(value -> value.kind().equals("RECORD_KIND_SIGNAL"))).hasSize(1);
        }
    }

    @ParameterizedTest
    @CsvSource({"RECORD_KIND_SIGNAL,clip-one,0", "RECORD_KIND_SEGMENT_REF,other,0", "RECORD_KIND_SEGMENT_REF,clip-one,1"})
    void invalidServerReferenceIdentityIsRejected(String kind, String boot, long seq) {
        assertThatThrownBy(() -> reference(kind, boot, seq)).isInstanceOf(IllegalArgumentException.class);
    }
}
