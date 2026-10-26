package io.fleetsentinel.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import io.fleetsentinel.pipeline.dedup.DedupFunction;
import io.fleetsentinel.pipeline.model.DlqRecord;
import io.fleetsentinel.pipeline.model.Envelope;
import io.fleetsentinel.pipeline.transform.DecodeFunction;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class DlqSerializationTest {
    static ConsumerRecord<byte[], byte[]> input(byte[] payload) {
        var record = new ConsumerRecord<byte[], byte[]>("telemetry.records", 3, 42,
                "v1".getBytes(StandardCharsets.UTF_8), payload);
        Map.of("vehicle_id", "v1", "boot_id", "storage-boot", "source_boot_id", "vehicle-boot",
                "seq", "7", "stream_id", "event", "report_interval_ms", "1000",
                "kind", "RECORD_KIND_SIGNAL").forEach((k, v) -> record.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
        record.headers().add("binary", new byte[]{0, -1, 5});
        record.headers().add("binary", null);
        return record;
    }

    static GenericRecord read(DlqRecord failure) throws Exception {
        byte[] bytes = new DlqSerializationSchema().serialize(failure);
        try (var reader = new DataFileStream<GenericRecord>(new ByteArrayInputStream(bytes), new GenericDatumReader<>())) {
            assertThat(reader.hasNext()).isTrue();
            var result = reader.next();
            assertThat(reader.hasNext()).isFalse();
            return result;
        }
    }

    @Test void malformedAvroReachesDlqWithReplayIdentityAndOriginalBytes() throws Exception {
        byte[] payload = {(byte) 0xff};
        var input = input(payload);
        try (var h = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(new DecodeFunction()))) {
            h.open();
            h.processElement(new StreamRecord<>(Envelope.of(input)));
            assertThat(h.extractOutputValues()).isEmpty();
            var failures = h.getSideOutput(PipelineSupport.DLQ);
            assertThat(failures).hasSize(1);
            var failure = failures.peek().getValue();
            var wire = read(failure);
            assertThat(wire.get("format_version")).isEqualTo(2);
            assertThat(wire.get("original_payload")).isEqualTo(ByteBuffer.wrap(payload));
            assertThat(wire.get("source_key")).isEqualTo(ByteBuffer.wrap(input.key()));
            assertThat(wire.get("source_partition")).isEqualTo(3);
            assertThat(wire.get("source_offset")).isEqualTo(42L);
            assertThat(wire.get("source_subscription").toString()).isEqualTo("telemetry.records");
            assertThat(wire.get("vehicle_id").toString()).isEqualTo("v1");
            assertThat(wire.get("boot_id").toString()).isEqualTo("storage-boot");
            assertThat(wire.get("source_boot_id").toString()).isEqualTo("vehicle-boot");
            assertThat(wire.get("stream_id").toString()).isEqualTo("event");
            assertThat(wire.get("seq")).isEqualTo(7L);
            assertThat(wire.get("report_interval_ms")).isEqualTo(1000L);
            assertThat(wire.get("kind").toString()).isEqualTo("RECORD_KIND_SIGNAL");
            assertThat(wire.get("error_class").toString()).isEqualTo("PARSE_FAILURE");
            assertThat(wire.get("processing_time")).isEqualTo(failure.processingTime() * 1000);

            var headers = (List<?>) wire.get("source_headers");
            assertThat(headers).hasSize(input.headers().toArray().length);
            for (int i = 0; i < headers.size(); i++) {
                var header = (GenericRecord) headers.get(i);
                var original = input.headers().toArray()[i];
                assertThat(header.get("key").toString()).isEqualTo(original.key());
                assertThat(header.get("value")).isEqualTo(original.value() == null ? null : ByteBuffer.wrap(original.value()));
            }
        }
    }

    @Test void invalidHeadersAreQuarantinedWithoutDedupDroppingThem() throws Exception {
        var input = input(null);
        input.headers().remove("source_boot_id");
        input.headers().add("seq", null);
        List<Envelope> envelopes = new ArrayList<>();
        new EnvelopeDeserializer().deserialize(input, new Collector<>() {
            public void collect(Envelope value) { envelopes.add(value); }
            public void close() {}
        });
        assertThat(envelopes).hasSize(1);
        try (var dedup = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new DedupFunction(8)), Envelope::dedupKey, Types.STRING);
             var decode = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(new DecodeFunction()))) {
            dedup.open(); decode.open();
            dedup.processElement(new StreamRecord<>(envelopes.get(0)));
            dedup.processElement(new StreamRecord<>(envelopes.get(0)));
            assertThat(dedup.extractOutputValues()).hasSize(2);
            for (var envelope : dedup.extractOutputValues()) decode.processElement(new StreamRecord<>(envelope));
            assertThat(decode.extractOutputValues()).isEmpty();
            var failures = decode.getSideOutput(PipelineSupport.DLQ);
            assertThat(failures).hasSize(2);
            var wire = read(failures.peek().getValue());
            assertThat(wire.get("original_payload")).isNull();
            assertThat(wire.get("source_offset")).isEqualTo(42L);
            assertThat(wire.get("pipeline_step").toString()).isEqualTo("envelope");
            assertThat((List<?>) wire.get("source_headers")).hasSize(input.headers().toArray().length);
        }
    }

    @Test void legacyEnvelopeKeepsItsBootAsTheSourceBoot() throws Exception {
        var env = new Envelope("v", "legacy", 9, "signal", new byte[]{1}, "old-topic");
        assertThat(env.sourceBootId()).isEqualTo("legacy");
        var wire = read(DlqRecord.of(env, DlqRecord.ErrorClass.PARSE_FAILURE, "decode", "bad"));
        assertThat(wire.get("source_boot_id").toString()).isEqualTo("legacy");
        assertThat(wire.get("source_partition")).isNull();
        assertThat(wire.get("source_offset")).isNull();
    }

}
