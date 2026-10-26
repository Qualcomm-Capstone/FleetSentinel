package io.fleetsentinel.api.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/** 001-schema.sql을 적용한 일회용 ClickHouse에서만 실행한다. 기존 데이터가 있으면 중단한다. */
class TelemetryQueriesIT {
    private static GenericContainer<?> database;
    private static TelemetryQueries queries;
    private static final Instant START = Instant.parse("2026-10-27T00:00:00.123456Z");

    @BeforeAll
    static void seedEmptyDatabase() throws Exception {
        // Given: each run owns a fresh container unless an explicit disposable URL is supplied.
        String url = System.getProperty("clickhouse.test.url", "");
        if (url.isBlank()) {
            database = new GenericContainer<>(DockerImageName.parse("clickhouse/clickhouse-server:26.3"))
                    .withEnv("CLICKHOUSE_SKIP_USER_SETUP", "1")
                    .withExposedPorts(8123)
                    .withCopyFileToContainer(MountableFile.forHostPath(System.getProperty("clickhouse.test.schema")),
                            "/tmp/fleetsentinel-schema.sql")
                    .waitingFor(Wait.forHttp("/ping").forPort(8123));
            database.start();
            var schema = database.execInContainer("clickhouse-client", "--multiquery",
                    "--queries-file", "/tmp/fleetsentinel-schema.sql");
            assertThat(schema.getExitCode()).as("ClickHouse schema: %s", schema.getStderr()).isZero();
            url = "jdbc:ch://" + database.getHost() + ":" + database.getMappedPort(8123) + "/fleet";
        }
        var jdbc = JdbcClient.create(new DriverManagerDataSource(
                url, "default", ""));
        queries = new TelemetryQueries(jdbc);
        assertThat(queries.health().counts()).isEqualTo(new io.fleetsentinel.api.model.PipelineHealth.Counts(0, 0, 0));
        assertThat(queries.timeRange()).isNull();
        assertThat(queries.nextRecordAfter(START)).isNull();
        jdbc.sql("""
                INSERT INTO fleet.signals_raw
                    (vehicle_id, boot_id, seq, scene_id, channel, sensor_time, log_time,
                     values_num, values_vec, values_str, lat, lon)
                VALUES
                    ('v1','boot',1,'scene','ego_pose','2026-10-27 00:00:00.123456','2026-10-27 00:00:00.123456',
                     {'speed':1.5},{'translation':[1.0,2.0,3.0]},{'location':'test'},0.0,103.0),
                    ('v1','boot',3,'scene','ego_pose','2026-10-27 00:00:01.123456','2026-10-27 00:00:01.123456',
                     {'speed':2.5},{'translation':[4.0,5.0,6.0]},{'location':'test'},1.0,104.0),
                    ('v1','boot',3,'scene','ego_pose','2026-10-27 00:00:01.123456','2026-10-27 00:00:01.123456',
                     {'speed':2.5},{'translation':[4.0,5.0,6.0]},{'location':'test'},1.0,104.0),
                    ('v1','boot',5,'scene','ego_pose','2026-10-27 00:00:00.623456','2026-10-27 00:00:02.123456',
                     {},{},{'location':'test'},2.0,105.0)
                """).update();
        jdbc.sql("""
                INSERT INTO fleet.perception_raw
                    (vehicle_id, boot_id, seq, scene_id, sample_id, sensor_time, category,
                     center_x, center_y, size_w, size_l, rot_z, visibility, num_lidar_pts)
                VALUES ('v1','boot',10,'scene','sample','2026-10-27 00:00:00.123456','vehicle.car',
                        1.23456,2.34567,3.0,4.0,0.0,'v0-40',0)
                """).update();
        jdbc.sql("""
                INSERT INTO fleet.segments_raw
                    (vehicle_id, boot_id, seq, segment_id, blob_uri, t_start, t_end,
                     sensor_channels, size_bytes, sample_count, state)
                VALUES ('v1','boot',11,'clip','s3://test/clip',
                        '2026-10-27 00:00:00.123456','2026-10-27 00:00:02.123456',['CAM','LIDAR'],1024,2,'UPLOADED')
                """).update();
    }

    @AfterAll
    static void stopDatabase() {
        if (database != null) database.stop();
    }

    @Test
    void latestPositionUsesObservationTimeAndFinalView() {
        // When
        var vehicles = queries.vehicles();
        // Then
        assertThat(vehicles).hasSize(1);
        assertThat(vehicles.getFirst().recordCount()).isEqualTo(3);
        assertThat(vehicles.getFirst().home()).containsExactly(1.0, 104.0);
        assertThat(vehicles.getFirst().durationMs()).isEqualTo(1000);
    }

    @Test
    void signalWindowPreservesUtcBoundariesAndLimits() {
        // When
        var samples = queries.signalsBetween(START, START.plusSeconds(1), 10);
        // Then
        assertThat(samples).extracting(sample -> sample.seq()).containsExactly(5L, 3L);
        assertThat(samples.getLast().values()).containsEntry("speed", 2.5);
        assertThat(samples.getLast().time()).isEqualTo(START.plusSeconds(1).toEpochMilli());
        assertThat(queries.signalsBetween(START, START.plusSeconds(1), 1)).hasSize(1);
        assertThat(queries.timeRange().start()).isEqualTo(START);
        assertThat(queries.timeRange().end()).isEqualTo(START.plusSeconds(1));
        assertThat(queries.nextRecordAfter(START)).isEqualTo(START.plusMillis(500));
        assertThat(queries.nextRecordAfter(START.plusSeconds(1))).isNull();
    }

    @Test
    void perceptionAndClipArraysMapToTypedResponses() {
        // When
        var frame = queries.perceptionBetween(START.minusNanos(1000), START, 10).getFirst();
        var clip = queries.clips(10).getFirst();
        // Then
        assertThat(frame.classes()).containsEntry("car", 1);
        assertThat(frame.zeroLidarCount()).isEqualTo(1);
        assertThat(frame.boxes().getFirst().center()).containsExactly(1.2346, 2.3457);
        assertThat(frame.boxes().getFirst().category()).isEqualTo("car");
        assertThat(clip.sensorChannels()).containsExactly("CAM", "LIDAR");
        assertThat(clip.durationSeconds()).isEqualTo(2.0);
    }

    @Test
    void healthCountsUniqueRowsAndSequenceGaps() {
        // When
        var health = queries.health();
        // Then
        assertThat(health.counts()).isEqualTo(new io.fleetsentinel.api.model.PipelineHealth.Counts(3, 1, 1));
        assertThat(health.totalMissing()).isEqualTo(2);
        assertThat(health.duplicatePressure()).hasSize(3);
    }
}
