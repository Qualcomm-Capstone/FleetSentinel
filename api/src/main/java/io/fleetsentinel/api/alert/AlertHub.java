package io.fleetsentinel.api.alert;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/** Kafka 알림을 연결된 SSE 구독자에게 전달하고 짧은 재연결 구간을 보관한다. */
@Component
@Slf4j
public class AlertHub {

    private static final long TIMEOUT_MS = Duration.ofHours(2).toMillis();

    private final int replayCapacity;
    private final Deque<AlertMessage> recent = new ArrayDeque<>();
    private final Set<String> recentIds = new HashSet<>();
    private final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();

    public AlertHub(@Value("${fleetsentinel.alerts.replay-capacity:1000}") int replayCapacity) {
        if (replayCapacity <= 0) {
            throw new IllegalArgumentException("alert replay capacity는 0보다 커야 한다");
        }
        this.replayCapacity = replayCapacity;
    }

    /** 같은 eventId는 Flink의 at-least-once 재전송으로 보고 한 번만 전달한다. */
    public synchronized void publish(AlertMessage alert) {
        if (!recentIds.add(alert.eventId())) {
            return;
        }
        recent.addLast(alert);
        while (recent.size() > replayCapacity) {
            recentIds.remove(recent.removeFirst().eventId());
        }

        for (Subscriber subscriber : subscribers) {
            if (subscriber.accepts(alert)) {
                send(subscriber, alert);
            }
        }
    }

    /** Last-Event-ID 뒤의 이벤트를 먼저 재생한 다음 라이브 전달에 참여시킨다. */
    public synchronized SseEmitter subscribe(String vehicle, String lastEventId) {
        SseEmitter emitter = new SseEmitter(TIMEOUT_MS);
        Subscriber subscriber = new Subscriber(emitter, normalize(vehicle));
        emitter.onCompletion(() -> subscribers.remove(subscriber));
        emitter.onTimeout(() -> subscribers.remove(subscriber));
        emitter.onError(error -> subscribers.remove(subscriber));

        for (AlertMessage alert : replayAfter(lastEventId, subscriber.vehicle())) {
            if (!send(subscriber, alert)) {
                return emitter;
            }
        }
        subscribers.add(subscriber);
        log.info("알림 SSE 연결: vehicle={} (총 {}명)", subscriber.vehicle(), subscribers.size());
        return emitter;
    }

    @Scheduled(fixedDelayString = "${fleetsentinel.alerts.heartbeat-ms:15000}")
    public synchronized void heartbeat() {
        for (Subscriber subscriber : subscribers) {
            try {
                subscriber.emitter().send(SseEmitter.event().comment("keepalive"));
            } catch (IOException | IllegalStateException error) {
                subscribers.remove(subscriber);
                subscriber.emitter().completeWithError(error);
            }
        }
    }

    synchronized List<AlertMessage> recent(String vehicle) {
        return recent.stream().filter(alert -> normalize(vehicle) == null
                        || normalize(vehicle).equals(alert.vehicleId()))
                .toList();
    }

    private List<AlertMessage> replayAfter(String lastEventId, String vehicle) {
        boolean replay = lastEventId == null || lastEventId.isBlank();
        List<AlertMessage> result = new ArrayList<>();
        for (AlertMessage alert : recent) {
            if (!replay && alert.eventId().equals(lastEventId)) {
                replay = true;
                continue;
            }
            if (replay && (vehicle == null || vehicle.equals(alert.vehicleId()))) {
                result.add(alert);
            }
        }
        // 브라우저가 보낸 ID가 현재 버퍼보다 오래됐으면 보관 중인 범위부터 다시 준다.
        if (!replay) {
            return recent.stream()
                    .filter(alert -> vehicle == null || vehicle.equals(alert.vehicleId()))
                    .toList();
        }
        return result;
    }

    private boolean send(Subscriber subscriber, AlertMessage alert) {
        try {
            subscriber.emitter().send(SseEmitter.event()
                    .id(alert.eventId())
                    .name("alert")
                    .data(alert));
            return true;
        } catch (IOException | IllegalStateException error) {
            subscribers.remove(subscriber);
            subscriber.emitter().completeWithError(error);
            return false;
        }
    }

    private static String normalize(String vehicle) {
        return vehicle == null || vehicle.isBlank() ? null : vehicle;
    }

    private record Subscriber(SseEmitter emitter, String vehicle) {
        boolean accepts(AlertMessage alert) {
            return vehicle == null || vehicle.equals(alert.vehicleId());
        }
    }
}
