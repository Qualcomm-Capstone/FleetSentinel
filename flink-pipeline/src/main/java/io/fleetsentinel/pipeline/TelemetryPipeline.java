package io.fleetsentinel.pipeline;

import org.apache.flink.util.ParameterTool;

/**
 * Backward-compatible command dispatcher.
 *
 * <p>The alert and storage graphs must be submitted as separate Flink jobs. A combined graph would
 * reintroduce the checkpoint/backpressure coupling this split is designed to remove.
 */
public final class TelemetryPipeline {

    private TelemetryPipeline() {
    }

    public static void main(String[] args) throws Exception {
        String job = ParameterTool.fromArgs(args).getRequired("job");
        switch (job) {
            case "alert" -> AlertPipeline.main(args);
            case "storage" -> StoragePipeline.main(args);
            default -> throw new IllegalArgumentException(
                    "--job must be 'alert' or 'storage'; submit each value as a separate Flink job");
        }
    }
}
