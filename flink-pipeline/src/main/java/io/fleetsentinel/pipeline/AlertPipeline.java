package io.fleetsentinel.pipeline;

import io.fleetsentinel.pipeline.alert.AlertEvent;
import io.fleetsentinel.pipeline.alert.AlertJsonSerializationSchema;
import io.fleetsentinel.pipeline.alert.GeoBounds;
import io.fleetsentinel.pipeline.alert.OddBoundaryFunction;
import io.fleetsentinel.pipeline.alert.VehicleMonitorFunction;
import io.fleetsentinel.pipeline.model.Decoded;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

/** Latency-sensitive alert job. It has no ClickHouse dependency or shared checkpoint. */
public final class AlertPipeline {

    public static final String JOB_NAME = "fleetsentinel-alerts";
    public static final String DEFAULT_GROUP = "fleetsentinel-alerts";

    private AlertPipeline() {
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env, parameters);
        env.execute(JOB_NAME);
    }

    static void build(StreamExecutionEnvironment env, ParameterTool parameters) {
        PipelineSupport.configureCheckpoints(env, parameters);
        String bootstrap = parameters.get("bootstrap", "localhost:29092");
        String alertTopic = parameters.get("alert-topic", "fleet.alerts");
        String group = parameters.get("group", DEFAULT_GROUP);
        Map<String, GeoBounds> oddBounds =
                GeoBounds.parseAll(parameters.getRequired("odd-bounds"));

        SingleOutputStreamOperator<Decoded> decoded = PipelineSupport.decoded(
                env, parameters, group, "kafka-alert-input");

        DataStream<AlertEvent> oddAlerts = decoded
                .filter(Decoded::isEgoPose)
                .name("odd-input")
                .keyBy(Decoded::vehicleId)
                .process(new OddBoundaryFunction(
                        oddBounds, parameters.getLong("odd-sustained-ms", 30_000)))
                .name("odd-boundary")
                .uid("odd-boundary");

        DataStream<AlertEvent> monitorAlerts = decoded
                .filter(Decoded::isVehicleMonitor)
                .name("monitor-input")
                .keyBy(Decoded::vehicleId)
                .process(new VehicleMonitorFunction(
                        parameters.getLong("stop-after-ms", 30_000),
                        parameters.getLong("monitor-stale-ms", 5_000)))
                .name("vehicle-monitor")
                .uid("vehicle-monitor-v1");

        oddAlerts.union(monitorAlerts)
                .sinkTo(KafkaSink.<AlertEvent>builder()
                        .setBootstrapServers(bootstrap)
                        .setRecordSerializer(KafkaRecordSerializationSchema.<AlertEvent>builder()
                                .setTopic(alertTopic)
                                .setKeySerializationSchema(event ->
                                        event.vehicleId().getBytes(StandardCharsets.UTF_8))
                                .setValueSerializationSchema(new AlertJsonSerializationSchema())
                                .build())
                        // Checkpoint 완료까지 경보를 숨기지 않는다. 안정적인 eventId로 중복을 제거한다.
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build())
                .name("alerts-kafka")
                .uid("alerts-kafka");
    }
}
