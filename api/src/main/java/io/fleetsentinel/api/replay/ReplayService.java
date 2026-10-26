package io.fleetsentinel.api.replay;

import io.fleetsentinel.api.config.ApiProperties;
import io.fleetsentinel.api.model.PerceptionFrame;
import io.fleetsentinel.api.model.SignalSample;
import io.fleetsentinel.api.model.TimeRange;
import io.fleetsentinel.api.query.TelemetryQueries;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

/** 저장된 관측의 시간 창, 공백 이동, 반복 재생을 관리한다. */
@Service
@RequiredArgsConstructor
@EnableConfigurationProperties(ApiProperties.class)
public class ReplayService {
    private final TelemetryQueries queries;
    private final ApiProperties properties;
    private TimeRange range;
    private Instant cursor;

    /** 조회가 모두 성공한 뒤 커서를 전진시킨다. 조회 실패 시 같은 시간 창을 재시도한다. */
    public synchronized Frame poll() {
        if (range == null) {
            range = queries.timeRange();
            if (range == null) return Frame.empty();
            cursor = startCursor();
        }

        Instant to = cursor.plusMillis(properties.getPushIntervalMs());
        boolean wrapped = !to.isBefore(range.end());
        if (wrapped) to = range.end();

        // 대시보드 미리보기는 주기당 건수 상한을 유지한다.
        var signals = queries.signalsBetween(cursor, to, properties.getMaxRecordsPerPush());
        var perception = queries.perceptionBetween(cursor, to, 50);
        Instant nextCursor = wrapped ? startCursor() : to;
        if (signals.isEmpty() && perception.isEmpty() && !wrapped) {
            Instant next = queries.nextRecordAfter(to);
            if (next == null) {
                nextCursor = startCursor();
                wrapped = true;
            } else {
                nextCursor = next.minusNanos(1000);
            }
        }
        cursor = nextCursor;
        return new Frame(signals, perception, wrapped);
    }

    private Instant startCursor() {
        // SQL 하한이 exclusive이며 ClickHouse 관측 시각은 마이크로초 정밀도다.
        return range.start().minusNanos(1000);
    }

    public record Frame(List<SignalSample> signals, List<PerceptionFrame> perception, boolean wrapped) {
        static Frame empty() { return new Frame(List.of(), List.of(), false); }
    }
}
