package io.fleetsentinel.api.query;

/** 데이터 조회는 FINAL 뷰를 사용한다. 중복 적재량은 dup_pressure 뷰에서 별도로 조회한다. */
final class TelemetrySql {
    private TelemetrySql() {}

    // 장면별 기록 시간을 합산하고, 도착 순서 대신 sensor_time으로 최신 위치를 선택한다.
    static final String VEHICLES = """
            SELECT
                vehicle_id,
                argMax(scene_id, scene_time) AS scene_id,
                tupleElement(argMax(position, position_time), 3) AS location,
                sum(n)                        AS n_records,
                sum(scene_ms)                 AS recorded_ms,
                tupleElement(argMax(position, position_time), 1) AS last_lat,
                tupleElement(argMax(position, position_time), 2) AS last_lon
            FROM (
                SELECT
                    vehicle_id,
                    scene_id,
                    count()                                       AS n,
                    max(sensor_time)                              AS scene_time,
                    dateDiff('millisecond', min(sensor_time),
                                            max(sensor_time))     AS scene_ms,
                    argMaxIf(tuple(lat, lon, values_str['location']), tuple(sensor_time, boot_id, seq),
                             lat IS NOT NULL AND lon IS NOT NULL) AS position,
                    maxIf(sensor_time, lat IS NOT NULL AND lon IS NOT NULL) AS position_time
                FROM fleet.signals
                GROUP BY vehicle_id, scene_id
            )
            GROUP BY vehicle_id
            ORDER BY vehicle_id
            """;

    static final String SIGNALS = """
            SELECT vehicle_id, seq, channel, sensor_time,
                   values_num, values_vec, values_str, lat, lon
            FROM fleet.signals
            WHERE sensor_time >  toDateTime64(:from, 6, 'UTC')
              AND sensor_time <= toDateTime64(:to, 6, 'UTC')
            ORDER BY sensor_time, seq
            LIMIT :limit
            """;

    static final String PERCEPTION = """
            SELECT vehicle_id, sample_id, sensor_time,
                   count()                          AS n_objects,
                   countIf(num_lidar_pts = 0)       AS n_zero_lidar,
                   groupArray(category)             AS categories,
                   groupArray(center_x)             AS cx,
                   groupArray(center_y)             AS cy,
                   groupArray(size_w)               AS sw,
                   groupArray(size_l)               AS sl,
                   groupArray(rot_z)                AS rz,
                   groupArray(visibility)           AS vis,
                   groupArray(num_lidar_pts)        AS lp
            FROM fleet.perception
            WHERE sensor_time >  toDateTime64(:from, 6, 'UTC')
              AND sensor_time <= toDateTime64(:to, 6, 'UTC')
            GROUP BY vehicle_id, sample_id, sensor_time
            ORDER BY sensor_time
            LIMIT :limit
            """;

    static final String CLIPS = """
            SELECT s.segment_id, s.vehicle_id, s.scene_id, s.blob_uri,
                   s.t_start, s.t_end, s.size_bytes, s.sample_count,
                   s.state, s.sensor_channels
            FROM fleet.segments AS s
            ORDER BY s.t_start DESC
            LIMIT :limit
            """;

    static final String COUNTS = """
            SELECT
                (SELECT count() FROM fleet.signals)     AS signals,
                (SELECT count() FROM fleet.perception)  AS perception,
                (SELECT count() FROM fleet.segments)    AS segments
            """;

    static final String PROGRESS = """
            SELECT vehicle_id, boot_id, first_seq, last_seq, received, expected, missing
            FROM fleet.vehicle_progress
            ORDER BY vehicle_id
            """;

    // 빈 집계가 1970년을 반환하지 않도록 OrNull을 사용한다.
    static final String NEXT_RECORD = """
            SELECT minOrNull(sensor_time) AS t FROM fleet.signals
            WHERE sensor_time > toDateTime64(:after, 6, 'UTC')
            """;

    static final String TIME_RANGE = """
            SELECT minOrNull(sensor_time) AS lo, maxOrNull(sensor_time) AS hi FROM fleet.signals
            """;

    static final String DUPLICATE_PRESSURE = """
            SELECT table, rows_raw, rows_distinct, pending_dupes FROM fleet.dup_pressure
            """;
}
