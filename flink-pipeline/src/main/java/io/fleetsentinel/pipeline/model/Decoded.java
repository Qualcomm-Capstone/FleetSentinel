package io.fleetsentinel.pipeline.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;

/**
 * 검증·파생까지 끝난 레코드. 싱크가 이걸 그대로 INSERT 한다.
 *
 * 신원 3종은 각자 필드로 두고, 나머지 컬럼은 columns() 리스트에 순서대로 담는다.
 * 그 순서가 싱크의 VALUES (?,?,...) 물음표 순서와 그대로 1:1이다 — 순서를 바꾸면
 * 컬럼이 밀려 들어간다.
 */
public final class Decoded implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Kind { SIGNAL, PERCEPTION, SEGMENT }

    private final Kind kind;
    private final String vehicleId;
    private final String bootId;
    private final String sourceBootId;
    private final long seq;
    private final List<Object> columns;
    private final String streamId;
    private final long reportIntervalMs;

    private Decoded(Kind kind, Envelope envelope, List<Object> columns) {
        this.kind = kind;
        this.vehicleId = envelope.vehicleId();
        this.bootId = envelope.bootId();
        this.sourceBootId = envelope.sourceBootId();
        this.seq = envelope.seq();
        this.columns = columns;
        this.streamId = envelope.streamId();
        this.reportIntervalMs = envelope.reportIntervalMs();
    }

    public long freshnessMs(long legacyDefault) {
        return Math.max(legacyDefault, reportIntervalMs * 3);
    }

    /** Historical backfill is storage-only. MQTT live observations require synchronized clocks. */
    public boolean liveAt(long nowMs) {
        if ("history".equals(streamId)) return false;
        if (streamId == null || "legacy".equals(streamId)) return true;
        long age = nowMs - eventTimeMillis();
        return age >= -5_000 && age < freshnessMs(5_000);
    }

    public Kind kind() {
        return kind;
    }

    public String vehicleId() {
        return vehicleId;
    }

    public String bootId() {
        return bootId;
    }

    public String sourceBootId() {
        return sourceBootId == null ? bootId : sourceBootId;
    }

    public long seq() {
        return seq;
    }

    public List<Object> columns() {
        return columns;
    }

    public long eventTimeMillis() {
        int index = switch (kind) {
            case SIGNAL, PERCEPTION -> 2;
            case SEGMENT -> 3;
        };
        Object value = columns.get(index);
        if (value instanceof java.sql.Timestamp timestamp) {
            return timestamp.toInstant().toEpochMilli();
        }
        throw new IllegalStateException("event time이 없다: " + kind);
    }

    public boolean isEgoPose() {
        return kind == Kind.SIGNAL && "ego_pose".equals(columns.get(1));
    }

    public boolean isVehicleMonitor() {
        return kind == Kind.SIGNAL && "fleet_status".equals(columns.get(1));
    }

    public Double signalNumber(String name) {
        return kind == Kind.SIGNAL && columns.get(4) instanceof Map<?, ?> values
                ? number(values.get(name)) : null;
    }

    public String signalString(String name) {
        if (kind != Kind.SIGNAL || !(columns.get(6) instanceof Map<?, ?> values)) return null;
        Object value = values.get(name);
        return value == null ? null : value.toString();
    }

    public String signalLocation() {
        if (kind != Kind.SIGNAL) {
            return null;
        }
        Object values = columns.get(6);
        if (!(values instanceof Map<?, ?> map)) {
            return null;
        }
        Object location = map.get("location");
        return location == null ? null : location.toString();
    }

    public Double latitude() {
        int index = switch (kind) {
            case SIGNAL -> 7;
            case PERCEPTION -> 19;
            case SEGMENT -> -1;
        };
        return index < 0 ? null : number(columns.get(index));
    }

    public Double longitude() {
        int index = switch (kind) {
            case SIGNAL -> 8;
            case PERCEPTION -> 20;
            case SEGMENT -> -1;
        };
        return index < 0 ? null : number(columns.get(index));
    }

    public int numLidarPoints() {
        if (kind != Kind.PERCEPTION) {
            throw new IllegalStateException("객체 메타데이터 레코드가 아니다: " + kind);
        }
        Object value = columns.get(17);
        if (!(value instanceof Number number)) {
            throw new IllegalStateException("num_lidar_pts가 없다");
        }
        return number.intValue();
    }

    /** 신호. lat/lon 은 Flink 가 파생한 값이라 실패하면 null 이고, 그래도 행은 살린다. */
    public static Decoded signal(Envelope env, GenericRecord r, Double lat, Double lon) {
        List<Object> c = new ArrayList<>();
        c.add(str(r, "scene_id"));
        c.add(str(r, "channel"));
        c.add(micros(r, "sensor_time"));
        c.add(micros(r, "log_time"));
        c.add(map(r, "values_num"));
        c.add(map(r, "values_vec"));
        c.add(map(r, "values_str"));
        c.add(lat);
        c.add(lon);
        return new Decoded(Kind.SIGNAL, env, c);
    }

    /** ② 객체 메타데이터. 좌표가 글로벌 프레임이라 ego 지역을 알아야 파생할 수 있다. */
    public static Decoded perception(Envelope env, GenericRecord r, Double lat, Double lon) {
        List<Object> c = new ArrayList<>();
        c.add(str(r, "scene_id"));
        c.add(str(r, "sample_id"));
        c.add(micros(r, "sensor_time"));
        c.add(str(r, "track_id"));
        c.add(str(r, "category"));
        c.add(str(r, "attribute"));
        for (String f : List.of("center_x", "center_y", "center_z",
                "size_w", "size_l", "size_h",
                "rot_w", "rot_x", "rot_y", "rot_z")) {
            c.add(r.get(f));
        }
        c.add(str(r, "visibility"));
        c.add(r.get("num_lidar_pts"));
        c.add(r.get("num_radar_pts"));
        c.add(lat);
        c.add(lon);
        return new Decoded(Kind.PERCEPTION, env, c);
    }

    /** ③ 클립 참조. 파일은 오브젝트 스토리지에 있고 이건 보관증이다. */
    public static Decoded segment(Envelope env, GenericRecord r) {
        List<Object> c = new ArrayList<>();
        c.add(str(r, "segment_id"));
        c.add(str(r, "scene_id"));
        c.add(str(r, "blob_uri"));
        c.add(micros(r, "t_start"));
        c.add(micros(r, "t_end"));
        c.add(strList(r, "sensor_channels"));
        c.add(r.get("size_bytes"));
        c.add(str(r, "checksum"));
        c.add(r.get("sample_count"));
        c.add(str(r, "state"));
        c.add(str(r, "drop_reason"));
        c.add(map(r, "calibration"));
        return new Decoded(Kind.SEGMENT, env, c);
    }

    // ── Avro → JDBC 변환 ────────────────────────────────────────────────

    /** Avro 문자열은 String 이 아니라 Utf8 이라, 그대로 넘기면 JDBC 드라이버가 못 알아본다. */
    private static String str(GenericRecord r, String field) {
        Object v = r.get(field);
        return v == null ? null : v.toString();
    }

    /** 마이크로초 정수 → Timestamp. ClickHouse DateTime64(6) 에 맞춘다. */
    private static java.sql.Timestamp micros(GenericRecord r, String field) {
        Object v = r.get(field);
        if (!(v instanceof Number n)) {
            return null;
        }
        long us = n.longValue();
        // Timestamp 생성자는 밀리초를 받는다. 그래서 밀리초까지만 먼저 넣고,
        // 잘려 나간 마이크로초 자리를 setNanos 로 따로 채운다(1 µs = 1000 ns).
        var ts = new java.sql.Timestamp(us / 1000);
        ts.setNanos((int) (Math.floorMod(us, 1_000_000L) * 1000L));
        return ts;
    }

    private static Map<String, Object> map(GenericRecord r, String field) {
        Object v = r.get(field);
        if (!(v instanceof Map<?, ?> m)) {
            return Map.of();
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : m.entrySet()) {
            Object val = e.getValue();
            if (val instanceof List<?> list) {
                // 벡터. ClickHouse 의 Array(Float64) 로 들어간다.
                List<Double> nums = new ArrayList<>(list.size());
                for (Object o : list) {
                    nums.add(o instanceof Number n ? n.doubleValue() : null);
                }
                out.put(e.getKey().toString(), nums);
            } else if (val instanceof CharSequence cs) {
                out.put(e.getKey().toString(), cs.toString());
            } else {
                out.put(e.getKey().toString(), val);
            }
        }
        return out;
    }

    private static List<String> strList(GenericRecord r, String field) {
        Object v = r.get(field);
        if (!(v instanceof List<?> list)) {
            return List.of();
        }
        List<String> out = new ArrayList<>(list.size());
        for (Object o : list) {
            out.add(o == null ? null : o.toString());
        }
        return out;
    }

    private static Double number(Object value) {
        return value instanceof Number number ? number.doubleValue() : null;
    }
}
