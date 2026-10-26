package io.fleetsentinel.pipeline.alert;

import io.fleetsentinel.pipeline.alert.AlertEvent.DecodedPosition;
import io.fleetsentinel.pipeline.alert.AlertEvent.Severity;
import io.fleetsentinel.pipeline.alert.AlertEvent.Type;
import io.fleetsentinel.pipeline.model.Decoded;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** 차량별 ODD 진입·이탈 전이와 지속 시간을 판정한다. */
public class OddBoundaryFunction extends KeyedProcessFunction<String, Decoded, AlertEvent> {

    private static final long serialVersionUID = 1L;

    private final Map<String, GeoBounds> boundsByLocation;
    private final long sustainedAfterMs;
    private final long freshForMs;
    private transient ValueState<Long> lastReceivedAt;
    private transient ValueState<Long> lastFreshnessMs;

    private transient ValueState<Boolean> outside;
    private transient ValueState<String> location;
    private transient ValueState<Long> pendingTimer;
    private transient ValueState<String> exitBootId;
    private transient ValueState<Long> exitSeq;
    private transient ValueState<Long> exitEventTime;
    private transient ValueState<Double> lastLatitude;
    private transient ValueState<Double> lastLongitude;
    private transient ValueState<Long> lastEventTime;
    private transient Counter missingBounds;
    private transient Counter latePositions;

    public OddBoundaryFunction(Map<String, GeoBounds> boundsByLocation, long sustainedAfterMs) {
        this(boundsByLocation, sustainedAfterMs, 5_000);
    }

    public OddBoundaryFunction(Map<String, GeoBounds> boundsByLocation, long sustainedAfterMs, long freshForMs) {
        if (boundsByLocation == null || boundsByLocation.isEmpty()) {
            throw new IllegalArgumentException("ODD 경계가 비어 있다");
        }
        if (sustainedAfterMs <= 0) {
            throw new IllegalArgumentException("ODD 지속 시간은 0보다 커야 한다");
        }
        this.boundsByLocation = new LinkedHashMap<>(boundsByLocation);
        this.sustainedAfterMs = sustainedAfterMs;
        if (freshForMs <= 0) throw new IllegalArgumentException("positive freshness required");
        this.freshForMs = freshForMs;
    }

    @Override
    public void open(OpenContext openContext) {
        lastReceivedAt = getRuntimeContext().getState(new ValueStateDescriptor<>("odd-last-received", Long.class));
        lastFreshnessMs = getRuntimeContext().getState(new ValueStateDescriptor<>("odd-freshness", Long.class));
        outside = getRuntimeContext().getState(new ValueStateDescriptor<>("odd-outside", Boolean.class));
        location = getRuntimeContext().getState(new ValueStateDescriptor<>("odd-location", String.class));
        pendingTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("odd-timer", Long.class));
        exitBootId = getRuntimeContext().getState(new ValueStateDescriptor<>("odd-exit-boot", String.class));
        exitSeq = getRuntimeContext().getState(new ValueStateDescriptor<>("odd-exit-seq", Long.class));
        exitEventTime = getRuntimeContext().getState(new ValueStateDescriptor<>("odd-exit-time", Long.class));
        lastLatitude = getRuntimeContext().getState(new ValueStateDescriptor<>("odd-last-lat", Double.class));
        lastLongitude = getRuntimeContext().getState(new ValueStateDescriptor<>("odd-last-lon", Double.class));
        lastEventTime = getRuntimeContext().getState(new ValueStateDescriptor<>("odd-last-event-time", Long.class));
        missingBounds = getRuntimeContext().getMetricGroup().addGroup("alerts").counter("odd_missing_bounds");
        latePositions = getRuntimeContext().getMetricGroup().addGroup("alerts").counter("odd_late_positions");
    }

    @Override
    public void processElement(Decoded value, Context ctx, Collector<AlertEvent> out) throws Exception {
        if (!value.liveAt(ctx.timerService().currentProcessingTime())) return;
        String currentLocation = value.signalLocation();
        Double latitude = value.latitude();
        Double longitude = value.longitude();
        GeoBounds bounds = boundsByLocation.get(currentLocation);
        if (bounds == null || latitude == null || longitude == null) {
            missingBounds.inc();
            return;
        }

        long eventTime = value.eventTimeMillis();
        Long previousEventTime = lastEventTime.value();
        if (previousEventTime != null && eventTime <= previousEventTime) {
            latePositions.inc();
            return;
        }
        lastEventTime.update(eventTime);
        Long received = lastReceivedAt.value();
        long now = ctx.timerService().currentProcessingTime();
        long previousFreshness = lastFreshnessMs.value() == null ? freshForMs : lastFreshnessMs.value();
        if (received != null && now - received >= previousFreshness) {
            clearExcursion(ctx);
            outside.clear();
        }
        lastReceivedAt.update(now);
        lastFreshnessMs.update(value.freshnessMs(freshForMs));

        String previousLocation = location.value();
        if (previousLocation != null && !Objects.equals(previousLocation, currentLocation)) {
            clearExcursion(ctx);
            outside.clear();
        }
        location.update(currentLocation);
        lastLatitude.update(latitude);
        lastLongitude.update(longitude);

        boolean nowOutside = !bounds.contains(latitude, longitude);
        Boolean wasOutside = outside.value();
        long detectedAt = ctx.timerService().currentProcessingTime();
        DecodedPosition position = position(value, currentLocation, latitude, longitude);

        if (wasOutside == null) {
            outside.update(nowOutside);
            if (nowOutside) {
                beginExcursion(value, ctx);
                out.collect(AlertEvent.odd(Type.ODD_EXITED, Severity.WARNING, position,
                        detectedAt, transitionId(value), "첫 위치가 ODD 경계 밖에서 관측됐다"));
            }
            return;
        }

        if (!wasOutside && nowOutside) {
            outside.update(true);
            beginExcursion(value, ctx);
            out.collect(AlertEvent.odd(Type.ODD_EXITED, Severity.WARNING, position,
                    detectedAt, transitionId(value), "차량이 ODD 경계를 벗어났다"));
        } else if (wasOutside && !nowOutside) {
            outside.update(false);
            clearExcursion(ctx);
            out.collect(AlertEvent.odd(Type.ODD_RETURNED, Severity.INFO, position,
                    detectedAt, transitionId(value), "차량이 ODD 경계 안으로 복귀했다"));
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AlertEvent> out) throws Exception {
        Long expected = pendingTimer.value();
        if (expected == null || expected != timestamp || !Boolean.TRUE.equals(outside.value())) {
            return;
        }
        Long received = lastReceivedAt.value();
        long freshness = lastFreshnessMs.value() == null ? freshForMs : lastFreshnessMs.value();
        if (received == null || ctx.timerService().currentProcessingTime() - received >= freshness) {
            pendingTimer.clear();
            return;
        }
        String currentLocation = location.value();
        Double latitude = lastLatitude.value();
        Double longitude = lastLongitude.value();
        String bootId = exitBootId.value();
        Long seq = exitSeq.value();
        Long eventTime = exitEventTime.value();
        if (currentLocation == null || latitude == null || longitude == null
                || bootId == null || seq == null || eventTime == null) {
            pendingTimer.clear();
            return;
        }

        DecodedPosition position = new DecodedPosition(
                ctx.getCurrentKey(), bootId, seq, eventTime, currentLocation, latitude, longitude);
        out.collect(AlertEvent.odd(Type.ODD_SUSTAINED, Severity.CRITICAL, position,
                timestamp, bootId + ":" + seq,
                "ODD 이탈 상태가 %d초 이상 이어졌다".formatted(sustainedAfterMs / 1000)));
        pendingTimer.clear();
    }

    private void beginExcursion(Decoded value, Context ctx) throws Exception {
        long timer = ctx.timerService().currentProcessingTime() + sustainedAfterMs;
        ctx.timerService().registerProcessingTimeTimer(timer);
        pendingTimer.update(timer);
        exitBootId.update(value.bootId());
        exitSeq.update(value.seq());
        exitEventTime.update(value.eventTimeMillis());
    }

    private void clearExcursion(Context ctx) throws Exception {
        Long timer = pendingTimer.value();
        if (timer != null) {
            ctx.timerService().deleteProcessingTimeTimer(timer);
        }
        pendingTimer.clear();
        exitBootId.clear();
        exitSeq.clear();
        exitEventTime.clear();
    }

    private static DecodedPosition position(
            Decoded value, String location, double latitude, double longitude) {
        return new DecodedPosition(value.vehicleId(), value.bootId(), value.seq(),
                value.eventTimeMillis(), location, latitude, longitude);
    }

    private static String transitionId(Decoded value) {
        return value.bootId() + ":" + value.seq();
    }
}
