package io.fleetsentinel.pipeline;

import io.fleetsentinel.pipeline.dedup.DedupFunction;
import io.fleetsentinel.pipeline.dedup.SeqWindow;
import io.fleetsentinel.pipeline.model.Decoded;
import io.fleetsentinel.pipeline.model.DlqRecord;
import io.fleetsentinel.pipeline.model.Envelope;
import io.fleetsentinel.pipeline.transform.DecodeFunction;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.ParameterTool;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/** Shared, deterministic topology construction for the independently deployed jobs. */
public final class PipelineSupport {

    public static final OutputTag<DlqRecord> DLQ =
            new OutputTag<>("dlq", TypeInformation.of(DlqRecord.class));

    private PipelineSupport() {
    }

    public static void configureCheckpoints(
            StreamExecutionEnvironment env, ParameterTool parameters) {
        long checkpointMs = parameters.getLong("checkpoint-ms", 10_000);
        env.enableCheckpointing(checkpointMs);
        CheckpointConfig checkpoint = env.getCheckpointConfig();
        checkpoint.setMinPauseBetweenCheckpoints(checkpointMs / 2);
        checkpoint.setCheckpointTimeout(Duration.ofMinutes(5).toMillis());
        checkpoint.setTolerableCheckpointFailureNumber(3);
        checkpoint.setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    }

    public static SingleOutputStreamOperator<Decoded> decoded(
            StreamExecutionEnvironment env,
            ParameterTool parameters,
            String consumerGroup,
            String sourceName) {
        String bootstrap = parameters.get("bootstrap", "localhost:29092");
        int window = parameters.getInt("dedup-window", SeqWindow.DEFAULT_WINDOW);

        KafkaSource<Envelope> source = KafkaSource.<Envelope>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(parameters.get("topic", "telemetry.records"))
                .setGroupId(consumerGroup)
                // Resume the independent consumer group after redeploy. Only a brand-new group
                // falls back to the retained log's earliest offset.
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new EnvelopeDeserializer())
                .build();

        DataStream<Envelope> raw = env.fromSource(
                        source, WatermarkStrategy.noWatermarks(), sourceName)
                .uid("kafka-source");

        DataStream<Envelope> deduped = raw
                .keyBy(Envelope::vehicleId)
                .process(new DedupFunction(window))
                .name("dedup")
                .uid("dedup");

        return deduped
                .process(new DecodeFunction())
                .name("decode-validate-derive")
                .uid("decode");
    }

    public static void addDlqSink(
            SingleOutputStreamOperator<Decoded> decoded, ParameterTool parameters) {
        decoded.getSideOutput(DLQ)
                .map(d -> "%s|%s|%s|%s".formatted(
                        d.errorClass(), d.pipelineStep(), d.sourceSubscription(), d.errorDetail()))
                .sinkTo(KafkaSink.<String>builder()
                        .setBootstrapServers(parameters.get("bootstrap", "localhost:29092"))
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(parameters.get("dlq-topic", "telemetry.dlq"))
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build())
                .name("dlq")
                .uid("dlq");
    }
}
