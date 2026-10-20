package io.fleetsentinel.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;
import org.junit.jupiter.api.Test;

class PipelineIsolationTest {

    private static final ParameterTool ALERT_PARAMETERS = ParameterTool.fromMap(Map.of(
            "bootstrap", "unused:9092",
            "topic", "telemetry.records",
            "alert-topic", "fleet.alerts",
            "odd-bounds", "test=0.0,1.0,0.0,1.0"));

    @Test
    void alertGraphHasNoClickHouseSink() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        AlertPipeline.build(env, ALERT_PARAMETERS);

        String graph = env.getStreamGraph().getStreamingPlanAsJSON();
        assertThat(graph)
                .contains("kafka-alert-input", "alerts-kafka")
                .doesNotContain("clickhouse", "kafka-storage-input");
    }

    @Test
    void storageGraphHasNoAlertOperators() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StoragePipeline.build(env, ParameterTool.fromMap(Map.of(
                "bootstrap", "unused:9092",
                "topic", "telemetry.records",
                "clickhouse", "jdbc:ch://unused:8123/fleet")));

        String graph = env.getStreamGraph().getStreamingPlanAsJSON();
        assertThat(graph)
                .contains("kafka-storage-input", "clickhouse")
                .doesNotContain("alerts-kafka", "odd-boundary", "vehicle-monitor");
    }

    @Test
    void jobsUseIndependentConsumerGroupsByDefault() {
        assertThat(AlertPipeline.DEFAULT_GROUP)
                .isNotEqualTo(StoragePipeline.DEFAULT_GROUP);
    }
}
