package io.fleetsentinel.pipeline;

import io.fleetsentinel.pipeline.model.DlqRecord;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;

/** One self-describing Avro record per Kafka value; serialization failure fails the sink. */
public final class DlqSerializationSchema implements SerializationSchema<DlqRecord> {
    private static final long serialVersionUID = 1L;
    private transient Schema schema;

    @Override public byte[] serialize(DlqRecord failure) {
        try {
            if (schema == null) {
                try (var input = getClass().getResourceAsStream("/schemas/dlq-envelope.avsc")) {
                    if (input == null) throw new IOException("missing DLQ schema");
                    schema = new Schema.Parser().parse(input);
                }
            }
            var record = new GenericData.Record(schema);
            record.put("original_payload", bytes(failure.originalPayload()));
            record.put("error_class", new GenericData.EnumSymbol(schema.getField("error_class").schema(), failure.errorClass().name()));
            record.put("error_detail", failure.errorDetail());
            record.put("source_subscription", failure.sourceSubscription());
            record.put("pipeline_step", failure.pipelineStep());
            record.put("processing_time", Math.multiplyExact(failure.processingTime(), 1000L));
            record.put("attempt", failure.attempt());
            record.put("format_version", 2);
            var headers = new ArrayList<GenericRecord>();
            var source = failure.source();
            if (source != null) {
                record.put("source_partition", source.sourcePartition());
                record.put("source_offset", source.sourceOffset());
                record.put("source_key", bytes(source.sourceKey()));
                for (var h : source.sourceHeaders()) {
                    var header = new GenericData.Record(schema.getField("source_headers").schema().getElementType());
                    header.put("key", h.key());
                    header.put("value", bytes(h.value()));
                    headers.add(header);
                }
                if (source.validationError() == null) {
                    record.put("vehicle_id", source.vehicleId());
                    record.put("boot_id", source.bootId());
                    record.put("source_boot_id", source.sourceBootId());
                    record.put("seq", source.seq());
                    record.put("kind", source.kind());
                    record.put("stream_id", source.streamId());
                    record.put("report_interval_ms", source.reportIntervalMs());
                }
            }
            record.put("source_headers", headers);
            var output = new ByteArrayOutputStream();
            try (var writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema))) {
                writer.create(schema, output);
                writer.append(record);
            }
            return output.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("DLQ serialization failed", e);
        }
    }

    private static ByteBuffer bytes(byte[] value) {
        return value == null ? null : ByteBuffer.wrap(value);
    }
}
