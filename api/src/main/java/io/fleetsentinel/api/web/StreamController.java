package io.fleetsentinel.api.web;

import io.fleetsentinel.api.config.ApiProperties;
import io.fleetsentinel.api.model.PerceptionFrame;
import io.fleetsentinel.api.model.SignalSample;
import io.fleetsentinel.api.model.TimeRange;
import io.fleetsentinel.api.model.VehicleObservation;
import io.fleetsentinel.api.query.TelemetryQueries;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * 실시간 푸시. 대시보드의 목업 스트림을 대체한다.
 *
 * <h2>WebSocket 이 아니라 SSE 인 이유</h2>
 *
 * <p>서버→클라이언트 단방향이면 충분하고, <b>재연결과 재개가 프로토콜에 있다</b>
 * ({@code Last-Event-ID}). WebSocket 은 그걸 직접 만들어야 한다.
 *
 * <h2>저장된 데이터를 "실시간"으로 만드는 방법</h2>
 *
 * <p>이 프로젝트는 라이브 관제가 아니다(SDD L-1). ClickHouse 에 있는 것은 2018년
 * nuScenes 기록이므로, <b>재생 커서</b>를 두고 벽시계 진행만큼 데이터 시간축을 밀어
 * 그 구간을 내보낸다. 화면·주기·형식은 실제와 같고 시각만 과거다.
 *
 * <p>끝에 닿으면 처음으로 되감고 {@code epoch} 이벤트를 보낸다 — 프론트가 궤적을
 * 지우고 다시 그리라는 신호다(목업 스트림과 같은 규약).
 */
@RestController
@RequestMapping("/api")
@EnableConfigurationProperties(ApiProperties.class)
@Slf4j
public class StreamController {

    /** 연결 유지 상한. 브라우저가 끊기면 EventSource 가 알아서 재연결한다. */
    private static final long TIMEOUT_MS = Duration.ofHours(2).toMillis();

    private final TelemetryQueries queries;
    private final ApiProperties props;

    /** 열려 있는 구독자. 푸시는 스케줄러 스레드 하나에서만 일어난다. */
    private final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();

    /** 데이터 시간축의 재생 커서. 벽시계 진행만큼 함께 전진한다. */
    private volatile Instant cursor;
    private volatile Instant lo;
    private volatile Instant hi;

    public StreamController(TelemetryQueries queries, ApiProperties props) {
        this.queries = queries;
        this.props = props;
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(@RequestParam(required = false) String vehicle) {
        SseEmitter emitter = new SseEmitter(TIMEOUT_MS);
        Subscriber sub = new Subscriber(emitter, vehicle);

        emitter.onCompletion(() -> subscribers.remove(sub));
        emitter.onTimeout(() -> subscribers.remove(sub));
        emitter.onError(e -> subscribers.remove(sub));
        subscribers.add(sub);

        log.info("SSE 연결: vehicle={} (총 {}명)", vehicle, subscribers.size());
        return emitter;
    }

    /**
     * 주기 푸시. 커서를 벽시계 진행만큼 밀고 그 구간을 내보낸다.
     *
     * <p>구독자가 없으면 질의도 하지 않는다 — 아무도 안 보는데 ClickHouse 를 때릴 이유가 없다.
     */
    @Scheduled(fixedDelayString = "${fleetsentinel.api.push-interval-ms:250}")
    public void push() {
        if (subscribers.isEmpty()) {
            return;
        }
        if (!ensureRange()) {
            return;   // 데이터가 아직 없다
        }

        Instant from = cursor;
        Instant to = from.plusMillis(props.getPushIntervalMs());
        boolean wrapped = false;
        if (to.isAfter(hi)) {
            // 끝에 닿았다. 되감고 프론트에 알린다.
            to = hi;
            wrapped = true;
        }
        cursor = wrapped ? lo : to;

        List<SignalSample> signals;
        List<PerceptionFrame> perception;
        try {
            signals = queries.signalsBetween(from, to, props.getMaxRecordsPerPush());
            perception = queries.perceptionBetween(from, to, 50);
        } catch (RuntimeException e) {
            log.warn("질의 실패 — 이번 주기는 건너뛴다: {}", e.toString());
            return;
        }

        log.debug("푸시 창 {} ~ {} : 신호 {}건 · 객체 메타데이터 {}건",
                from, to, signals.size(), perception.size());

        // 창이 비었다 = 데이터 공백이다. 장면 사이 간격이 최소 120초이고 다른 날짜면
        // 며칠이므로(data-design.md §3.2) 250ms 씩 기어가면 벽시계로 수십 년이 걸린다.
        // 다음 레코드로 점프한다.
        if (signals.isEmpty() && perception.isEmpty() && !wrapped) {
            Instant next = queries.nextRecordAfter(to);
            if (next == null) {
                cursor = lo;          // 뒤에 아무것도 없다 — 되감는다
                broadcastEpoch();
            } else if (next.isAfter(to)) {
                // 창 하나만큼 앞에 두고 점프한다. 다음 주기에 그 구간이 잡힌다.
                cursor = next.minusMillis(props.getPushIntervalMs());
                log.debug("공백 건너뜀: {} → {}", to, cursor);
            }
            return;
        }

        for (Subscriber sub : subscribers) {
            try {
                sendBatch(sub, signals);
                sendPerception(sub, perception);
                if (wrapped) {
                    sub.emitter.send(SseEmitter.event()
                            .name("epoch")
                            .data(new Epoch(System.currentTimeMillis())));
                }
            } catch (IOException | IllegalStateException e) {
                // 브라우저가 끊었다. EventSource 가 알아서 재연결하므로 조용히 정리한다.
                subscribers.remove(sub);
                sub.emitter.completeWithError(e);
            }
        }
    }

    /**
     * 신호를 배치로 묶어 보낸다.
     *
     * <p>레코드마다 SSE 프레임을 만들면 초당 1,308개가 되고 브라우저가 먼저 무너진다.
     * <b>전송 단위만 배치이고 계약은 레코드 단위</b>다 — 목업 스트림과 같은 형태를 준다.
     */
    private void sendBatch(Subscriber sub, List<SignalSample> rows) throws IOException {
        List<SignalSample> mine = sub.filter(rows);
        if (mine.isEmpty()) {
            return;
        }
        sub.emitter.send(SseEmitter.event()
                .name("signal")
                .data(SignalSample.Batch.of(mine)));
    }

    private void sendPerception(Subscriber sub, List<PerceptionFrame> rows)
            throws IOException {
        for (PerceptionFrame row : sub.filter(rows)) {
            sub.emitter.send(SseEmitter.event().name("perception").data(row));
        }
    }

    /** 재생이 처음으로 되감겼다고 알린다. 프론트는 궤적을 지우고 다시 그린다. */
    private void broadcastEpoch() {
        for (Subscriber sub : subscribers) {
            try {
                sub.emitter.send(SseEmitter.event()
                        .name("epoch").data(new Epoch(System.currentTimeMillis())));
            } catch (IOException | IllegalStateException e) {
                subscribers.remove(sub);
                sub.emitter.completeWithError(e);
            }
        }
    }

    /** 데이터 시간 범위를 한 번 잡아둔다. 없으면 false. */
    private boolean ensureRange() {
        if (cursor != null) {
            return true;
        }
        TimeRange range = queries.timeRange();
        if (range == null) {
            return false;
        }
        lo = range.start();
        hi = range.end();
        cursor = lo;
        log.info("재생 범위: {} ~ {} ({}초)", lo, hi, Duration.between(lo, hi).toSeconds());
        return true;
    }

    /** 구독자 하나. {@code vehicle} 을 주면 그 차량만 받는다. */
    private record Subscriber(SseEmitter emitter, String vehicle) {

        <T extends VehicleObservation> List<T> filter(List<T> rows) {
            if (vehicle == null || vehicle.isBlank()) {
                return rows;
            }
            return rows.stream()
                    .filter(r -> vehicle.equals(r.vehicleId()))
                    .toList();
        }
    }

    private record Epoch(long at) {}
}
