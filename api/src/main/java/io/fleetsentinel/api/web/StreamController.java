package io.fleetsentinel.api.web;

import io.fleetsentinel.api.model.PerceptionFrame;
import io.fleetsentinel.api.model.SignalSample;
import io.fleetsentinel.api.model.VehicleObservation;
import io.fleetsentinel.api.replay.ReplayService;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/** 저장 데이터 재생 결과를 SSE로 전송하고 구독 연결을 관리한다. */
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Slf4j
public class StreamController {

    /** 연결 유지 상한. 브라우저가 끊기면 EventSource 가 알아서 재연결한다. */
    private static final long TIMEOUT_MS = Duration.ofHours(2).toMillis();

    private final ReplayService replay;

    /** 열려 있는 구독자. 푸시는 스케줄러 스레드 하나에서만 일어난다. */
    private final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();

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

    /** 구독자가 있을 때만 다음 재생 창을 조회한다. */
    @Scheduled(fixedDelayString = "${fleetsentinel.api.push-interval-ms:250}")
    public void push() {
        if (subscribers.isEmpty()) {
            return;
        }
        ReplayService.Frame frame;
        try {
            frame = replay.poll();
        } catch (DataAccessException e) {
            log.warn("재생 조회 실패: 다음 주기에 같은 구간 재시도", e);
            return;
        }

        for (Subscriber sub : subscribers) {
            try {
                sendBatch(sub, frame.signals());
                sendPerception(sub, frame.perception());
                if (frame.wrapped()) {
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

    /** 같은 차량의 신호를 하나의 SSE 배치로 전송한다. */
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
