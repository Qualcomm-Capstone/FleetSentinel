package io.fleetsentinel.api.query;

import static io.fleetsentinel.api.query.ClickHouseValues.*;

import io.fleetsentinel.api.model.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.RowMapper;

/** 조회 결과를 대시보드 응답 타입으로 변환한다. SQL 실행은 TelemetryQueries가 담당한다. */
final class TelemetryRowMappers {
    private TelemetryRowMappers() {}

    static final RowMapper<VehicleSummary> VEHICLE = (rs, row) -> {
        Double latitude = nullableDouble(rs, "last_lat");
        Double longitude = nullableDouble(rs, "last_lon");
        return new VehicleSummary(
                rs.getString("vehicle_id"), rs.getString("scene_id"), rs.getString("scene_id"),
                rs.getString("location"), rs.getLong("n_records"), rs.getLong("recorded_ms"),
                latitude == null || longitude == null ? null : List.of(latitude, longitude));
    };

    static final RowMapper<SignalSample> SIGNAL = (rs, row) -> {
        Map<String, Object> values = new LinkedHashMap<>();
        for (String column : List.of("values_num", "values_vec", "values_str")) {
            map(rs, column).forEach((key, value) -> values.put(key.toString(), value));
        }
        Double latitude = nullableDouble(rs, "lat");
        Double longitude = nullableDouble(rs, "lon");
        if (latitude != null && longitude != null) {
            values.put("lat", latitude);
            values.put("lon", longitude);
        }
        return new SignalSample(rs.getString("vehicle_id"), rs.getLong("seq"),
                rs.getString("channel"), instant(rs, "sensor_time").toEpochMilli(), values);
    };

    static final RowMapper<PerceptionFrame> PERCEPTION = (rs, row) -> perception(rs);

    static final RowMapper<ClipSummary> CLIP = (rs, row) -> {
        var start = instant(rs, "t_start");
        var end = instant(rs, "t_end");
        double duration = start == null || end == null ? 0 : (end.toEpochMilli() - start.toEpochMilli()) / 1000.0;
        return new ClipSummary(rs.getString("segment_id"), rs.getString("vehicle_id"),
                rs.getString("scene_id"), rs.getString("blob_uri"), duration,
                rs.getLong("size_bytes"), rs.getInt("sample_count"), rs.getString("state"),
                strings(rs, "sensor_channels"));
    };

    static final RowMapper<PipelineHealth.Counts> COUNTS = (rs, row) ->
            new PipelineHealth.Counts(rs.getLong("signals"), rs.getLong("perception"), rs.getLong("segments"));

    static final RowMapper<PipelineHealth.Progress> PROGRESS = (rs, row) ->
            new PipelineHealth.Progress(rs.getString("vehicle_id"), rs.getString("boot_id"),
                    rs.getLong("received"), rs.getLong("expected"), rs.getLong("missing"));

    static final RowMapper<PipelineHealth.DuplicatePressure> DUPLICATE_PRESSURE = (rs, row) ->
            new PipelineHealth.DuplicatePressure(rs.getString("table"),
                    rs.getLong("rows_raw"), rs.getLong("pending_dupes"));

    static final RowMapper<TimeRange> TIME_RANGE = (rs, row) -> {
        var start = instant(rs, "lo");
        var end = instant(rs, "hi");
        return start == null || end == null ? null : new TimeRange(start, end);
    };

    private static PerceptionFrame perception(ResultSet rs) throws SQLException {
        List<String> categories = strings(rs, "categories");
        Map<String, Integer> classes = new LinkedHashMap<>();
        for (String category : categories) classes.merge(shortCategory(category), 1, Integer::sum);

        List<Double> centerX = doubles(rs, "cx");
        List<Double> centerY = doubles(rs, "cy");
        List<Double> width = doubles(rs, "sw");
        List<Double> length = doubles(rs, "sl");
        List<Double> rotationZ = doubles(rs, "rz");
        List<String> visibility = strings(rs, "vis");
        List<Integer> lidarPoints = ints(rs, "lp");
        List<PerceptionFrame.Box> boxes = new ArrayList<>(centerX.size());
        for (int i = 0; i < centerX.size(); i++) {
            // 기존 화면 계약: 지면과 평행한 박스의 yaw를 rot_z로 근사한다.
            double yaw = round(2 * Math.asin(Math.max(-1, Math.min(1, rotationZ.get(i)))));
            boxes.add(new PerceptionFrame.Box(
                    List.of(round(centerX.get(i)), round(centerY.get(i))),
                    List.of(round(width.get(i)), round(length.get(i))), yaw,
                    i < categories.size() ? shortCategory(categories.get(i)) : "unknown",
                    i < lidarPoints.size() ? lidarPoints.get(i) : 0,
                    i < visibility.size() ? visibility.get(i) : ""));
        }
        return new PerceptionFrame(rs.getString("vehicle_id"), instant(rs, "sensor_time").toEpochMilli(),
                rs.getInt("n_objects"), rs.getInt("n_zero_lidar"), classes, boxes);
    }

    private static String shortCategory(String category) {
        return category.substring(category.lastIndexOf('.') + 1);
    }

    private static double round(double value) {
        return Math.round(value * 10_000.0) / 10_000.0;
    }
}
