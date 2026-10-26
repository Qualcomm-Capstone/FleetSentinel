package io.fleetsentinel.api.query;

import io.fleetsentinel.api.model.*;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** ClickHouse 관제 조회. SQL과 행 변환은 각각 TelemetrySql, TelemetryRowMappers에 정의한다. */
@Repository
@RequiredArgsConstructor
public class TelemetryQueries {
    private final JdbcClient jdbc;

    public List<VehicleSummary> vehicles() {
        return jdbc.sql(TelemetrySql.VEHICLES).query(TelemetryRowMappers.VEHICLE).list();
    }

    public List<SignalSample> signalsBetween(Instant from, Instant to, int limit) {
        return window(TelemetrySql.SIGNALS, from, to, limit)
                .query(TelemetryRowMappers.SIGNAL).list();
    }

    public List<PerceptionFrame> perceptionBetween(Instant from, Instant to, int limit) {
        return window(TelemetrySql.PERCEPTION, from, to, limit)
                .query(TelemetryRowMappers.PERCEPTION).list();
    }

    public List<ClipSummary> clips(int limit) {
        return jdbc.sql(TelemetrySql.CLIPS).param("limit", limit)
                .query(TelemetryRowMappers.CLIP).list();
    }

    public PipelineHealth health() {
        var counts = jdbc.sql(TelemetrySql.COUNTS).query(TelemetryRowMappers.COUNTS).single();
        var progress = jdbc.sql(TelemetrySql.PROGRESS).query(TelemetryRowMappers.PROGRESS).list();
        var duplicates = jdbc.sql(TelemetrySql.DUPLICATE_PRESSURE)
                .query(TelemetryRowMappers.DUPLICATE_PRESSURE).list();
        long missing = progress.stream().mapToLong(PipelineHealth.Progress::missing).sum();
        return new PipelineHealth(counts, progress, missing, duplicates);
    }

    public Instant nextRecordAfter(Instant after) {
        return jdbc.sql(TelemetrySql.NEXT_RECORD).param("after", ClickHouseValues.timestamp(after))
                .query((rs, row) -> ClickHouseValues.instant(rs, "t")).optional().orElse(null);
    }

    public TimeRange timeRange() {
        return jdbc.sql(TelemetrySql.TIME_RANGE).query(TelemetryRowMappers.TIME_RANGE).optional().orElse(null);
    }

    private JdbcClient.StatementSpec window(String sql, Instant from, Instant to, int limit) {
        return jdbc.sql(sql)
                .param("from", ClickHouseValues.timestamp(from))
                .param("to", ClickHouseValues.timestamp(to))
                .param("limit", limit);
    }
}
