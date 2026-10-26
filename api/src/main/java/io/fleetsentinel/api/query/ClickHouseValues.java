package io.fleetsentinel.api.query;

import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;

/** ClickHouse JDBC의 시각·Nullable·Array 변환을 한곳에서 처리한다. */
final class ClickHouseValues {
    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);

    private ClickHouseValues() {}

    // 드라이버의 JVM 기본 시간대 해석을 피하도록 UTC 문자열로 바인딩한다.
    static String timestamp(Instant time) {
        return TIMESTAMP.format(time);
    }

    static Instant instant(ResultSet rs, String column) throws SQLException {
        var value = rs.getTimestamp(column, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
        return value == null ? null : value.toInstant();
    }

    static Double nullableDouble(ResultSet rs, String column) throws SQLException {
        double value = rs.getDouble(column);
        return rs.wasNull() ? null : value;
    }

    static Map<?, ?> map(ResultSet rs, String column) throws SQLException {
        Object value = rs.getObject(column);
        return value instanceof Map<?, ?> map ? map : Map.of();
    }

    static <T> List<T> array(ResultSet rs, String column, Function<Object, T> convert)
            throws SQLException {
        Object value = rs.getObject(column);
        java.sql.Array sqlArray = value instanceof java.sql.Array array ? array : null;
        try {
            if (sqlArray != null) value = sqlArray.getArray();
            List<T> result = new ArrayList<>();
            if (value instanceof List<?> list) {
                for (Object item : list) result.add(convert.apply(item));
            } else if (value != null && value.getClass().isArray()) {
                // JDBC 구현에 따라 Object[] 또는 primitive 배열을 반환한다.
                for (int i = 0; i < Array.getLength(value); i++) result.add(convert.apply(Array.get(value, i)));
            }
            return result;
        } finally {
            if (sqlArray != null) sqlArray.free();
        }
    }

    static List<String> strings(ResultSet rs, String column) throws SQLException {
        return array(rs, column, value -> value == null ? null : value.toString());
    }

    static List<Double> doubles(ResultSet rs, String column) throws SQLException {
        return array(rs, column, value -> value instanceof Number number ? number.doubleValue() : 0.0);
    }

    static List<Integer> ints(ResultSet rs, String column) throws SQLException {
        return array(rs, column, value -> value instanceof Number number ? number.intValue() : 0);
    }
}
