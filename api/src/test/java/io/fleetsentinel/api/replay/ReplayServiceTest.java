package io.fleetsentinel.api.replay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.fleetsentinel.api.config.ApiProperties;
import io.fleetsentinel.api.model.SignalSample;
import io.fleetsentinel.api.model.TimeRange;
import io.fleetsentinel.api.query.TelemetryQueries;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.DataAccessResourceFailureException;

class ReplayServiceTest {
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant BEFORE_START = START.minusNanos(1000);
    private final TelemetryQueries queries = mock(TelemetryQueries.class);
    private final ApiProperties properties = new ApiProperties();
    private final ReplayService replay = new ReplayService(queries, properties);

    enum FailureStage { RANGE, SIGNAL, PERCEPTION, GAP }

    @ParameterizedTest
    @EnumSource(FailureStage.class)
    void retriesSameWindowAfterQueryFailure(FailureStage stage) {
        // Given
        var range = new TimeRange(START, START.plusSeconds(10));
        var failure = new DataAccessResourceFailureException("temporary database failure");
        when(queries.timeRange()).thenReturn(range);
        switch (stage) {
            case RANGE -> when(queries.timeRange()).thenThrow(failure).thenReturn(range);
            case SIGNAL -> when(queries.signalsBetween(any(), any(), anyInt()))
                    .thenThrow(failure).thenReturn(List.of());
            case PERCEPTION -> when(queries.perceptionBetween(any(), any(), anyInt()))
                    .thenThrow(failure).thenReturn(List.of());
            case GAP -> when(queries.nextRecordAfter(any())).thenThrow(failure).thenReturn(null);
        }
        // When
        assertThatThrownBy(replay::poll).isSameAs(failure);
        replay.poll();
        // Then: failure never commits a new replay position.
        verify(queries, times(stage == FailureStage.RANGE ? 1 : 2))
                .signalsBetween(BEFORE_START, BEFORE_START.plusMillis(250), 200);
    }

    @Test
    void waitsForDataWithoutCachingEmptyRange() {
        // Given
        when(queries.timeRange()).thenReturn(null, new TimeRange(START, START));
        // When
        var empty = replay.poll();
        var first = replay.poll();
        // Then
        assertThat(empty.wrapped()).isFalse();
        assertThat(empty.signals()).isEmpty();
        assertThat(first.wrapped()).isTrue();
        verify(queries).signalsBetween(BEFORE_START, START, 200);
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 100, 250})
    void wrapsAtEndAndIncludesFirstTimestampOnNextCycle(long durationMs) {
        // Given
        properties.setPushIntervalMs(251);
        Instant end = START.plusMillis(durationMs);
        when(queries.timeRange()).thenReturn(new TimeRange(START, end));
        // When
        var first = replay.poll();
        var second = replay.poll();
        // Then
        assertThat(first.wrapped()).isTrue();
        assertThat(second.wrapped()).isTrue();
        verify(queries, times(2)).signalsBetween(BEFORE_START, end, 200);
    }

    @Test
    void jumpsToNextObservationAcrossGap() {
        // Given
        when(queries.timeRange()).thenReturn(new TimeRange(START, START.plusSeconds(3600)));
        when(queries.nextRecordAfter(any())).thenReturn(START.plusSeconds(1800));
        // When
        var gap = replay.poll();
        replay.poll();
        // Then
        assertThat(gap.wrapped()).isFalse();
        var next = START.plusSeconds(1800).minusNanos(1000);
        verify(queries).signalsBetween(next, next.plusMillis(250), 200);
    }

    @Test
    void forwardsConfiguredPreviewLimitAndAdvancesAfterSuccess() {
        // Given
        properties.setMaxRecordsPerPush(1);
        when(queries.timeRange()).thenReturn(new TimeRange(START, START.plusSeconds(10)));
        var sample = new SignalSample("v1", 1, "speed", START.toEpochMilli(), Map.of("speed", 1.0));
        when(queries.signalsBetween(any(), any(), anyInt())).thenReturn(List.of(sample));
        // When
        var first = replay.poll();
        replay.poll();
        // Then
        assertThat(first.signals()).containsExactly(sample);
        verify(queries).signalsBetween(BEFORE_START.plusMillis(250), BEFORE_START.plusMillis(500), 1);
    }
}
