package io.fleetsentinel.pipeline.sink;

import io.fleetsentinel.pipeline.model.Decoded;
import io.fleetsentinel.pipeline.model.TelemetryData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

/** 테이블별 INSERT와 필드 바인딩. JDBC 위치 인덱스는 이 경계 안에서만 사용한다. */
enum ClickHouseInsert {
    SIGNAL("""
            INSERT INTO fleet.signals_raw
              (vehicle_id, boot_id, seq, scene_id, channel, sensor_time,
               log_time, values_num, values_vec, values_str, lat, lon)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""") {
        @Override
        void bindPayload(PreparedStatement statement, TelemetryData data) throws SQLException {
            var row = (TelemetryData.Signal) data;
            statement.setObject(4, row.sceneId());
            statement.setObject(5, row.channel());
            statement.setObject(6, timestamp(row.sensorTime()));
            statement.setObject(7, timestamp(row.logTime()));
            statement.setObject(8, row.numbers());
            statement.setObject(9, row.vectors());
            statement.setObject(10, row.strings());
            statement.setObject(11, row.latitude());
            statement.setObject(12, row.longitude());
        }
    },

    PERCEPTION("""
            INSERT INTO fleet.perception_raw
              (vehicle_id, boot_id, seq, scene_id, sample_id, sensor_time,
               track_id, category, attribute,
               center_x, center_y, center_z, size_w, size_l, size_h,
               rot_w, rot_x, rot_y, rot_z,
               visibility, num_lidar_pts, num_radar_pts, lat, lon)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""") {
        @Override
        void bindPayload(PreparedStatement statement, TelemetryData data) throws SQLException {
            var row = (TelemetryData.Perception) data;
            statement.setObject(4, row.sceneId());
            statement.setObject(5, row.sampleId());
            statement.setObject(6, timestamp(row.sensorTime()));
            statement.setObject(7, row.trackId());
            statement.setObject(8, row.category());
            statement.setObject(9, row.attribute());
            statement.setObject(10, row.centerX());
            statement.setObject(11, row.centerY());
            statement.setObject(12, row.centerZ());
            statement.setObject(13, row.sizeW());
            statement.setObject(14, row.sizeL());
            statement.setObject(15, row.sizeH());
            statement.setObject(16, row.rotW());
            statement.setObject(17, row.rotX());
            statement.setObject(18, row.rotY());
            statement.setObject(19, row.rotZ());
            statement.setObject(20, row.visibility());
            statement.setObject(21, row.numLidarPoints());
            statement.setObject(22, row.numRadarPoints());
            statement.setObject(23, row.latitude());
            statement.setObject(24, row.longitude());
        }
    },

    SEGMENT("""
            INSERT INTO fleet.segments_raw
              (vehicle_id, boot_id, seq, segment_id, scene_id, blob_uri,
               t_start, t_end, sensor_channels, size_bytes, checksum,
               sample_count, state, drop_reason, calibration)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""") {
        @Override
        void bindPayload(PreparedStatement statement, TelemetryData data) throws SQLException {
            var row = (TelemetryData.Segment) data;
            statement.setObject(4, row.segmentId());
            statement.setObject(5, row.sceneId());
            statement.setObject(6, row.blobUri());
            statement.setObject(7, timestamp(row.start()));
            statement.setObject(8, timestamp(row.end()));
            statement.setObject(9, row.sensorChannels());
            statement.setObject(10, row.sizeBytes());
            statement.setObject(11, row.checksum());
            statement.setObject(12, row.sampleCount());
            statement.setObject(13, row.state());
            statement.setObject(14, row.dropReason());
            statement.setObject(15, row.calibration());
        }
    };

    private final String sql;

    ClickHouseInsert(String sql) { this.sql = sql; }

    static ClickHouseInsert forKind(Decoded.Kind kind) {
        return switch (kind) {
            case SIGNAL -> SIGNAL;
            case PERCEPTION -> PERCEPTION;
            case SEGMENT -> SEGMENT;
        };
    }

    String sql() { return sql; }

    void bind(PreparedStatement statement, Decoded row) throws SQLException {
        statement.setString(1, row.vehicleId());
        statement.setString(2, row.bootId());
        statement.setLong(3, row.seq());
        bindPayload(statement, row.data());
    }

    abstract void bindPayload(PreparedStatement statement, TelemetryData data) throws SQLException;

    private static Timestamp timestamp(Instant time) {
        return time == null ? null : Timestamp.from(time);
    }
}
