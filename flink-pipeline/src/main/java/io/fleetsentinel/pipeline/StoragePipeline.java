package io.fleetsentinel.pipeline;

import io.fleetsentinel.pipeline.model.Decoded;
import io.fleetsentinel.pipeline.sink.ClickHouseSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

/** Throughput-oriented ClickHouse storage job, isolated from the alert SLO. */
public final class StoragePipeline {

    public static final String JOB_NAME = "fleetsentinel-storage";
    public static final String DEFAULT_GROUP = "fleetsentinel-storage";

    private StoragePipeline() {
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env, parameters);
        env.execute(JOB_NAME);
    }

    static void build(StreamExecutionEnvironment env, ParameterTool parameters) {
        PipelineSupport.configureCheckpoints(env, parameters);
        String group = parameters.get("group", DEFAULT_GROUP);
        SingleOutputStreamOperator<Decoded> decoded = PipelineSupport.decoded(
                env, parameters, group, "kafka-storage-input");

        decoded.addSink(new ClickHouseSink(
                        parameters.get("clickhouse", "jdbc:ch://localhost:8124/fleet"),
                        parameters.get("clickhouse-user", "fleet"),
                        parameters.get("clickhouse-password", "fleet"),
                        parameters.getInt("batch-size", 1000)))
                .name("clickhouse")
                .uid("clickhouse");

        // 검증 실패는 storage 경로가 한 번만 기록한다. 두 job에서 쓰면 DLQ가 중복된다.
        PipelineSupport.addDlqSink(decoded, parameters);
    }
}
