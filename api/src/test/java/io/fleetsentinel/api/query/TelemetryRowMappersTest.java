package io.fleetsentinel.api.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TelemetryRowMappersTest {
    private final ResultSet rs = mock(ResultSet.class);

    @Test
    void coordinatesDistinguishZeroFromMissing() throws Exception {
        when(rs.getDouble("last_lat")).thenReturn(0.0);
        when(rs.getDouble("last_lon")).thenReturn(103.0);
        when(rs.wasNull()).thenReturn(false, false, true, false, false, true);

        assertThat(TelemetryRowMappers.VEHICLE.mapRow(rs, 0).home()).containsExactly(0.0, 103.0);
        assertThat(TelemetryRowMappers.VEHICLE.mapRow(rs, 0).home()).isNull();
        assertThat(TelemetryRowMappers.VEHICLE.mapRow(rs, 0).home()).isNull();
    }

    @Test
    void timestampBindingAndReadingAreUtcAndKeepMicroseconds() throws Exception {
        Instant time = Instant.parse("2026-10-27T00:00:00.123456Z");
        when(rs.getTimestamp(eq("time"), any(Calendar.class))).thenAnswer(call -> {
            Calendar calendar = call.getArgument(1);
            assertThat(calendar.getTimeZone().getID()).isEqualTo("UTC");
            return Timestamp.from(time);
        });
        assertThat(ClickHouseValues.timestamp(time)).isEqualTo("2026-10-27 00:00:00.123456");
        assertThat(ClickHouseValues.instant(rs, "time")).isEqualTo(time);
    }

    @Test
    void arraysHandleJdbcPrimitiveAndListForms() throws Exception {
        java.sql.Array array = mock(java.sql.Array.class);
        when(array.getArray()).thenReturn(new double[]{1.25, 2.5});
        when(rs.getObject("jdbc")).thenReturn(array);
        when(rs.getObject("list")).thenReturn(List.of("CAM", "LIDAR"));
        when(rs.getObject("objects")).thenReturn(new Integer[]{1, 2});
        assertThat(ClickHouseValues.doubles(rs, "jdbc")).containsExactly(1.25, 2.5);
        assertThat(ClickHouseValues.strings(rs, "list")).containsExactly("CAM", "LIDAR");
        assertThat(ClickHouseValues.ints(rs, "objects")).containsExactly(1, 2);
        verify(array).free();
    }

    @Test
    void signalCombinesDynamicChannelsWithoutInventingCoordinates() throws Exception {
        when(rs.getString("vehicle_id")).thenReturn("v1");
        when(rs.getString("channel")).thenReturn("ego_pose");
        when(rs.getObject("values_num")).thenReturn(Map.of("speed", 3.5));
        when(rs.getObject("values_vec")).thenReturn(Map.of("translation", List.of(1.0, 2.0)));
        when(rs.getObject("values_str")).thenReturn(Map.of("location", "test"));
        when(rs.getTimestamp(eq("sensor_time"), any(Calendar.class)))
                .thenReturn(Timestamp.from(Instant.ofEpochMilli(1234)));
        when(rs.wasNull()).thenReturn(true, false);
        var sample = TelemetryRowMappers.SIGNAL.mapRow(rs, 0);
        assertThat(sample.time()).isEqualTo(1234);
        assertThat(sample.values()).containsEntry("speed", 3.5)
                .containsEntry("translation", List.of(1.0, 2.0))
                .containsEntry("location", "test").doesNotContainKeys("lat", "lon");
    }
}
