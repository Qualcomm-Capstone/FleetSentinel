package io.fleetsentinel.api.query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * ClickHouse 질의. <b>{@code FINAL} 뷰만 읽는다.</b>
 *
 * <p>Flink→ClickHouse 는 at-least-once 라 머지 전에는 중복이 보인다. 스키마가
 * {@code fleet.signals} 같은 뷰에 {@code FINAL} 을 박아뒀으므로 여기서는 그 이름만 쓰면
 * 정확한 결과가 나온다 — {@code _raw} 를 직접 쓰면 집계가 조용히 부풀어오른다(SDD L-14).
 *
 * <p>이 클래스에 {@code _raw} 가 나오는 곳은 <b>운영 지표를 볼 때뿐</b>이다(머지 대기
 * 중복량). 그건 중복 자체가 관측 대상이라 뷰를 쓰면 안 된다.
 */
@Repository
public class TelemetryQueries {

    /**
     * <b>타임스탬프는 반드시 UTC 달력으로 읽고 쓴다.</b>
     *
     * <p>ClickHouse 컬럼은 {@code DateTime64(6, 'UTC')} 인데 JDBC 의
     * {@code getTimestamp(col)} 은 <b>JVM 기본 시간대</b>로 해석한다. KST 에서 돌리면
     * 9시간이 밀려 조회 구간이 데이터 공백에 떨어진다 — 예외도 안 나고 결과만 빈다.
     */
    private static final Calendar UTC =
            Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    private static Calendar utc() {
        // Calendar 는 스레드 안전하지 않다. 매번 복제해서 쓴다.
        return (Calendar) UTC.clone();
    }

    /**
     * 바인딩은 <b>문자열</b>로 한다. {@code setTimestamp(i, ts, utcCalendar)} 는 드라이버가
     * Calendar 를 무시해 여전히 JVM 시간대로 보냈다 — 예외 없이 빈 결과만 나온다.
     * SQL 쪽에서 {@code toDateTime64(?, 6, 'UTC')} 로 받으면 모호성이 사라진다.
     */
    private static final java.time.format.DateTimeFormatter CH_TS =
            java.time.format.DateTimeFormatter
                    .ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                    .withZone(java.time.ZoneOffset.UTC);

    private static String ts(Instant t) {
        return CH_TS.format(t);
    }

    private final JdbcTemplate jdbc;

    public TelemetryQueries(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 차량 로스터. 대시보드가 지도를 그리기 전에 차량 메타를 심어둔다.
     *
     * <p>{@code ego_pose} 채널만 좌표를 가지므로 거기서 마지막 위치를 뽑는다.
     */
    public List<Map<String, Object>> vehicles() {
        return jdbc.query("""
                -- 2단계 집계다. 윈도우 함수를 중첩하면 ClickHouse 가 거절한다
                -- (ILLEGAL_AGGREGATION) — 장면별로 먼저 접고 차량별로 다시 접는다.
                SELECT
                    vehicle_id,
                    argMax(scene_id, scene_time) AS scene_id,
                    tupleElement(argMax(position, position_time), 3) AS location,
                    sum(n)                        AS n_records,
                    -- 장면 사이 공백이 며칠이라(data-design.md §3.2) 전체 스팬은 주행시간이
                    -- 아니다. **장면별 스팬의 합**이 실제 녹화 길이다.
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
                        -- MQTT history may arrive after live status. Arrival order is not freshness.
                        -- Keep the coordinate pair/location from one observation; zero is valid.
                        argMaxIf(tuple(lat, lon, values_str['location']), tuple(sensor_time, boot_id, seq),
                                 lat IS NOT NULL AND lon IS NOT NULL) AS position,
                        maxIf(sensor_time, lat IS NOT NULL AND lon IS NOT NULL) AS position_time
                    FROM fleet.signals
                    GROUP BY vehicle_id, scene_id
                )
                GROUP BY vehicle_id
                ORDER BY vehicle_id
                """, (rs, i) -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("vehicle_id", rs.getString("vehicle_id"));
            m.put("scene_id", rs.getString("scene_id"));
            // 프론트는 scene_name 을 기대한다. 재생기에서는 scene_id 가 곧 장면 이름이다.
            m.put("scene_name", rs.getString("scene_id"));
            m.put("location", rs.getString("location"));
            m.put("n_records", rs.getLong("n_records"));
            m.put("duration_ms", rs.getLong("recorded_ms"));
            double lat = rs.getDouble("last_lat");
            double lon = rs.getDouble("last_lon");
            // home = 지도 초기 중심. 좌표가 없으면 null 로 두고 프론트가 판단한다.
            m.put("home", rs.wasNull() ? null : List.of(lat, lon));
            return m;
        });
    }

    /**
     * {@code sensor_time} 이 주어진 구간에 드는 신호를 채널 네이티브 그대로 준다.
     *
     * <p><b>다운샘플하지 않는다</b> — `zoesensors` 943.8 Hz 를 깎으면 98.5% 가 사라진다
     * (data-design.md §4.2). 대신 {@code limit} 로 한 번에 보내는 양만 자른다.
     */
    public List<Map<String, Object>> signalsBetween(Instant from, Instant to, int limit) {
        return jdbc.query("""
                SELECT vehicle_id, seq, channel, sensor_time,
                       values_num, values_vec, values_str, lat, lon
                FROM fleet.signals
                WHERE sensor_time >  toDateTime64(?, 6, 'UTC')
                  AND sensor_time <= toDateTime64(?, 6, 'UTC')
                ORDER BY sensor_time, seq
                LIMIT ?
                """,
                ps -> {
                    ps.setString(1, ts(from));
                    ps.setString(2, ts(to));
                    ps.setInt(3, limit);
                },
                (rs, i) -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("vehicle_id", rs.getString("vehicle_id"));
                    m.put("seq", rs.getLong("seq"));
                    m.put("c", rs.getString("channel"));          // 프론트가 쓰는 짧은 키
                    m.put("t", rs.getTimestamp("sensor_time", utc()).getTime());
                    Map<String, Object> v = new LinkedHashMap<>();
                    v.putAll(mapOf(rs, "values_num"));
                    v.putAll(mapOf(rs, "values_vec"));
                    v.putAll(mapOf(rs, "values_str"));
                    double lat = rs.getDouble("lat");
                    if (!rs.wasNull()) {
                        v.put("lat", lat);
                        v.put("lon", rs.getDouble("lon"));
                    }
                    m.put("v", v);
                    return m;
                });
    }

    /** 객체 메타데이터를 키프레임 단위로 묶어 준다. 프론트가 박스를 지도에 투영한다. */
    public List<Map<String, Object>> perceptionBetween(Instant from, Instant to, int limit) {
        List<Map<String, Object>> rows = jdbc.query("""
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
                WHERE sensor_time >  toDateTime64(?, 6, 'UTC')
                  AND sensor_time <= toDateTime64(?, 6, 'UTC')
                GROUP BY vehicle_id, sample_id, sensor_time
                ORDER BY sensor_time
                LIMIT ?
                """,
                ps -> {
                    ps.setString(1, ts(from));
                    ps.setString(2, ts(to));
                    ps.setInt(3, limit);
                },
                (rs, i) -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("vehicle_id", rs.getString("vehicle_id"));
                    m.put("t", rs.getTimestamp("sensor_time", utc()).getTime());
                    m.put("n_objects", rs.getInt("n_objects"));
                    m.put("n_zero_lidar", rs.getInt("n_zero_lidar"));

                    List<String> cats = strings(rs, "categories");
                    // 프론트는 짧은 이름을 쓴다 — vehicle.car → car
                    Map<String, Integer> classes = new LinkedHashMap<>();
                    for (String c : cats) {
                        String shortName = c.substring(c.lastIndexOf('.') + 1);
                        classes.merge(shortName, 1, Integer::sum);
                    }
                    m.put("classes", classes);

                    List<Double> cx = doubles(rs, "cx");
                    List<Double> cy = doubles(rs, "cy");
                    List<Double> sw = doubles(rs, "sw");
                    List<Double> sl = doubles(rs, "sl");
                    List<Double> rz = doubles(rs, "rz");
                    List<String> vs = strings(rs, "vis");
                    List<Integer> lp = ints(rs, "lp");

                    List<Map<String, Object>> boxes = new ArrayList<>(cx.size());
                    for (int b = 0; b < cx.size(); b++) {
                        Map<String, Object> box = new LinkedHashMap<>();
                        box.put("c", List.of(round(cx.get(b)), round(cy.get(b))));
                        box.put("s", List.of(round(sw.get(b)), round(sl.get(b))));
                        // rot_z 만으로 yaw 를 근사한다 — 박스가 지면에 평행하다는 전제다.
                        box.put("yaw", round(2 * Math.asin(Math.max(-1, Math.min(1, rz.get(b))))));
                        box.put("cat", b < cats.size()
                                ? cats.get(b).substring(cats.get(b).lastIndexOf('.') + 1) : "unknown");
                        box.put("lp", b < lp.size() ? lp.get(b) : 0);
                        box.put("vis", b < vs.size() ? vs.get(b) : "");
                        boxes.add(box);
                    }
                    m.put("boxes", boxes);
                    return m;
                });
        return rows;
    }

    /**
     * 클립 카탈로그. 중량 경로가 올린 세그먼트를 객체 메타데이터 요약과 함께 보여준다.
     *
     * <p>파일 자체는 오브젝트 스토리지에 있고 여기엔 {@code blob_uri} 참조만 있다
     * (Claim-Check, SDD S-1).
     */
    public List<Map<String, Object>> clips(int limit) {
        return jdbc.query("""
                SELECT s.segment_id, s.vehicle_id, s.scene_id, s.blob_uri,
                       s.t_start, s.t_end, s.size_bytes, s.sample_count,
                       s.state, s.sensor_channels
                FROM fleet.segments AS s
                ORDER BY s.t_start DESC
                LIMIT ?
                """,
                ps -> ps.setInt(1, limit),
                (rs, i) -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("clip_id", rs.getString("segment_id"));
                    m.put("vehicle_id", rs.getString("vehicle_id"));
                    m.put("scene_name", rs.getString("scene_id"));
                    m.put("blob_uri", rs.getString("blob_uri"));
                    m.put("duration_s", millisBetween(rs, "t_start", "t_end") / 1000.0);
                    m.put("size_bytes", rs.getLong("size_bytes"));
                    m.put("sample_count", rs.getInt("sample_count"));
                    m.put("state", rs.getString("state"));
                    m.put("sensor_channels", strings(rs, "sensor_channels"));
                    return m;
                });
    }

    /**
     * 파이프라인 건강도.
     *
     * <p>{@code vehicle_progress} 뷰가 결번을 계산한다 — <b>결번이 곧 유실</b>이므로
     * 이 값이 0 이 아니면 유실이 있다는 뜻이다(data-design.md §5.0).
     */
    public Map<String, Object> health() {
        Map<String, Object> out = new LinkedHashMap<>();

        Map<String, Object> counts = jdbc.queryForMap("""
                SELECT
                    (SELECT count() FROM fleet.signals)     AS signals,
                    (SELECT count() FROM fleet.perception)  AS perception,
                    (SELECT count() FROM fleet.segments)    AS segments
                """);
        out.put("counts", counts);

        // 결번 = 유실. 차량별로 본다.
        List<Map<String, Object>> progress = jdbc.query("""
                SELECT vehicle_id, boot_id, first_seq, last_seq, received, expected, missing
                FROM fleet.vehicle_progress
                ORDER BY vehicle_id
                """, (rs, i) -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("vehicle_id", rs.getString("vehicle_id"));
            m.put("boot_id", rs.getString("boot_id"));
            m.put("received", rs.getLong("received"));
            m.put("expected", rs.getLong("expected"));
            m.put("missing", rs.getLong("missing"));
            return m;
        });
        out.put("vehicles", progress);
        out.put("total_missing", progress.stream()
                .mapToLong(m -> (Long) m.get("missing")).sum());

        // 머지 대기 중복. **여기만 _raw 를 본다** — 중복 자체가 관측 대상이라
        // FINAL 뷰를 쓰면 항상 0 이 나와 아무것도 알 수 없다(SDD L-14).
        List<Map<String, Object>> dupes = jdbc.query(
                "SELECT table, rows_raw, rows_distinct, pending_dupes FROM fleet.dup_pressure",
                (rs, i) -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("table", rs.getString("table"));
                    m.put("rows_raw", rs.getLong("rows_raw"));
                    m.put("pending_dupes", rs.getLong("pending_dupes"));
                    return m;
                });
        out.put("dup_pressure", dupes);
        return out;
    }

    /**
     * {@code after} 이후 첫 레코드의 시각. <b>공백을 건너뛰는 데 쓴다.</b>
     *
     * <p>장면 사이 간격이 최소 120초이고 다른 날짜면 며칠이다(data-design.md §3.2).
     * 커서를 250ms 씩 밀면 그 공백을 지나는 데 벽시계로 수십 년이 걸린다 — 빈 창을
     * 만나면 여기로 점프한다.
     */
    public Instant nextRecordAfter(Instant after) {
        return jdbc.query("""
                SELECT min(sensor_time) AS t FROM fleet.signals
                WHERE sensor_time > toDateTime64(?, 6, 'UTC')
                """,
                ps -> ps.setString(1, ts(after)),
                (ResultSet rs) -> {
                    if (!rs.next()) {
                        return null;
                    }
                    var ts = rs.getTimestamp("t", utc());
                    return ts == null ? null : ts.toInstant();
                });
    }

    /** 데이터의 시간 범위. SSE 가 어디서부터 재생할지 정하는 데 쓴다. */
    public Instant[] timeRange() {
        return jdbc.queryForObject("""
                SELECT min(sensor_time) AS lo, max(sensor_time) AS hi FROM fleet.signals
                """, (rs, i) -> {
            var lo = rs.getTimestamp("lo", utc());
            var hi = rs.getTimestamp("hi", utc());
            return lo == null || hi == null
                    ? null : new Instant[]{lo.toInstant(), hi.toInstant()};
        });
    }

    // ── ClickHouse 타입 변환 ────────────────────────────────────────────

    private static long millisBetween(ResultSet rs, String a, String b) throws SQLException {
        var lo = rs.getTimestamp(a, utc());
        var hi = rs.getTimestamp(b, utc());
        return lo == null || hi == null ? 0 : hi.getTime() - lo.getTime();
    }

    /** ClickHouse {@code Map(...)} 는 드라이버가 {@link Map} 으로 준다. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> mapOf(ResultSet rs, String col) throws SQLException {
        Object v = rs.getObject(col);
        return v instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
    }

    private static List<String> strings(ResultSet rs, String col) throws SQLException {
        List<String> out = new ArrayList<>();
        for (Object o : arrayOf(rs, col)) {
            out.add(o == null ? null : o.toString());
        }
        return out;
    }

    private static List<Double> doubles(ResultSet rs, String col) throws SQLException {
        List<Double> out = new ArrayList<>();
        for (Object o : arrayOf(rs, col)) {
            out.add(o instanceof Number n ? n.doubleValue() : 0.0);
        }
        return out;
    }

    private static List<Integer> ints(ResultSet rs, String col) throws SQLException {
        List<Integer> out = new ArrayList<>();
        for (Object o : arrayOf(rs, col)) {
            out.add(o instanceof Number n ? n.intValue() : 0);
        }
        return out;
    }

    private static Object[] arrayOf(ResultSet rs, String col) throws SQLException {
        Object v = rs.getObject(col);
        if (v instanceof java.sql.Array a) {
            Object arr = a.getArray();
            return arr instanceof Object[] o ? o : new Object[0];
        }
        if (v instanceof Object[] o) {
            return o;
        }
        if (v instanceof List<?> l) {
            return l.toArray();
        }
        return new Object[0];
    }

    /** 좌표를 소수 4자리로. 프론트가 그리는 데 그 이상은 필요 없고 전송량만 는다. */
    private static double round(double d) {
        return Math.round(d * 10_000.0) / 10_000.0;
    }
}
