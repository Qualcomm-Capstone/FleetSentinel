package io.fleetsentinel.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.fleetsentinel.api.model.*;
import io.fleetsentinel.api.query.TelemetryQueries;
import io.fleetsentinel.api.web.QueryController;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

/** DTO 전환 후에도 HTTP·SSE의 필드명, 값, 배열 구조를 유지한다. */
class ContractTest {
    private final TelemetryQueries queries = mock(TelemetryQueries.class);
    private final QueryController controller = new QueryController(queries);
    private final ObjectMapper json = new ObjectMapper();

    @Test
    void vehiclesShape() {
        when(queries.vehicles()).thenReturn(List.of(new VehicleSummary(
                "v1", "scene", "scene", "singapore", 12, 19500, List.of(0.0, 103.0))));
        assertJson(controller.vehicles(), """
                {"vehicles":[{"vehicle_id":"v1","scene_id":"scene","scene_name":"scene",
                "location":"singapore","n_records":12,"duration_ms":19500,"home":[0.0,103.0]}]}
                """);
    }

    @Test
    void clipsShape() {
        when(queries.clips(100)).thenReturn(List.of(new ClipSummary(
                "clip", "v1", null, "s3://bucket/clip", 19.2, 1024, 3, "UPLOADED", List.of("CAM"))));
        assertJson(controller.clips(100), """
                [{"clip_id":"clip","vehicle_id":"v1","scene_name":null,"blob_uri":"s3://bucket/clip",
                "duration_s":19.2,"size_bytes":1024,"sample_count":3,"state":"UPLOADED","sensor_channels":["CAM"]}]
                """);
    }

    @Test
    void healthShape() {
        when(queries.health()).thenReturn(new PipelineHealth(
                new PipelineHealth.Counts(10, 2, 1),
                List.of(new PipelineHealth.Progress("v1", "boot", 10, 12, 2)), 2,
                List.of(new PipelineHealth.DuplicatePressure("signals", 11, 1))));
        assertJson(controller.health(), """
                {"counts":{"signals":10,"perception":2,"segments":1},
                "vehicles":[{"vehicle_id":"v1","boot_id":"boot","received":10,"expected":12,"missing":2}],
                "total_missing":2,"dup_pressure":[{"table":"signals","rows_raw":11,"pending_dupes":1}]}
                """);
    }

    @Test
    void signalBatchShape() {
        var sample = new SignalSample("v1", 7, "ego_pose", 1234, Map.of("lat", 0.0, "speed", 2.5));
        assertJson(SignalSample.Batch.of(List.of(sample)), """
                {"vehicle_id":"v1","t":1234,"n":1,"records":[
                {"vehicle_id":"v1","seq":7,"c":"ego_pose","t":1234,"v":{"lat":0.0,"speed":2.5}}]}
                """);
    }

    @Test
    void perceptionShape() {
        var box = new PerceptionFrame.Box(List.of(1.0, 2.0), List.of(3.0, 4.0), 0.0, "car", 0, "v0-40");
        assertJson(new PerceptionFrame("v1", 1234, 1, 1, Map.of("car", 1), List.of(box)), """
                {"vehicle_id":"v1","t":1234,"n_objects":1,"n_zero_lidar":1,"classes":{"car":1},
                "boxes":[{"c":[1.0,2.0],"s":[3.0,4.0],"yaw":0.0,"cat":"car","lp":0,"vis":"v0-40"}]}
                """);
    }

    @Test
    void clipsLimitIsCapped() {
        controller.clips(999_999);
        verify(queries).clips(1000);
    }

    private void assertJson(Object actual, String expected) {
        var tree = json.readTree(json.writeValueAsString(actual));
        assertThat(tree).isEqualTo(json.readTree(expected));
    }
}
