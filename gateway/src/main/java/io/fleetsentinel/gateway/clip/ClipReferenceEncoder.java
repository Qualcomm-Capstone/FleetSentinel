package io.fleetsentinel.gateway.clip;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "fleetsentinel.clips.enabled", havingValue = "true", matchIfMissing = true)
public class ClipReferenceEncoder {
    private final Schema schema;

    public ClipReferenceEncoder() {
        try (var input = getClass().getResourceAsStream("/schemas/segment-ref.avsc")) {
            if (input == null) throw new IllegalStateException("missing segment-ref Avro schema");
            schema = new Schema.Parser().parse(input);
        } catch (IOException e) { throw new IllegalStateException(e); }
    }

    byte[] encode(ClipJob job, ClipApi.Create request, String digest) {
        var record = new GenericData.Record(schema);
        record.put("segment_id", job.id);
        record.put("scene_id", request.sceneId());
        record.put("blob_uri", "s3://" + job.bucket + "/" + job.objectKey);
        record.put("t_start", request.tStartUs());
        record.put("t_end", request.tEndUs());
        record.put("sensor_channels", request.sensorChannels());
        record.put("size_bytes", request.sizeBytes());
        record.put("checksum", digest);
        record.put("sample_count", request.sampleCount());
        record.put("state", new GenericData.EnumSymbol(schema.getField("state").schema(), "UPLOADED"));
        record.put("drop_reason", null);
        record.put("calibration", new java.util.TreeMap<>(request.calibration()));
        try {
            var output = new ByteArrayOutputStream();
            var encoder = EncoderFactory.get().binaryEncoder(output, null);
            new GenericDatumWriter<GenericData.Record>(schema).write(record, encoder);
            encoder.flush();
            return output.toByteArray();
        } catch (IOException e) { throw new IllegalStateException("cannot encode clip reference", e); }
    }
}
