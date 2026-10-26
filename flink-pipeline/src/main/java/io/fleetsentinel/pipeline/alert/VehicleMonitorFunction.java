package io.fleetsentinel.pipeline.alert;

import io.fleetsentinel.pipeline.model.Decoded;
import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Co-timed monitor snapshots; never infers motion or health through a missing observation. */
public final class VehicleMonitorFunction extends KeyedProcessFunction<String, Decoded, AlertEvent> {
    private final long stopAfterMs;
    private final long staleAfterMs;
    private transient ValueState<State> state;
    private transient Counter invalid;
    private transient Counter late;
    private transient Counter emitted;

    public VehicleMonitorFunction(long stopAfterMs, long staleAfterMs) {
        if (stopAfterMs <= 0 || staleAfterMs <= 0) throw new IllegalArgumentException("positive durations required");
        this.stopAfterMs = stopAfterMs;
        this.staleAfterMs = staleAfterMs;
    }

    public static class State implements Serializable {
        public String boot;
        public String sourceBoot;
        public String mission;
        public long seq;
        public long eventTime;
        public long deadline;
        public long freshnessMs;
        public Long stoppedSince;
        public boolean stopAlert;
        public Boolean healthy;
        public boolean stale;
        public State() {}
    }

    @Override public void open(OpenContext context) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("vehicle-monitor-v1", State.class));
        var metrics = getRuntimeContext().getMetricGroup().addGroup("vehicle_monitor");
        invalid = metrics.counter("invalid");
        late = metrics.counter("late");
        emitted = metrics.counter("alerts");
    }

    @Override public void processElement(Decoded value, Context ctx, Collector<AlertEvent> out) throws Exception {
        if (!value.isVehicleMonitor() || !value.liveAt(ctx.timerService().currentProcessingTime())) return;
        Double speed = value.signalNumber("speed_mps");
        Double active = value.signalNumber("mission_active");
        Double planned = value.signalNumber("planned_stop");
        Double health = value.signalNumber("sensor_healthy");
        String mission = value.signalString("mission_id");
        if (speed == null || !Double.isFinite(speed) || speed < 0 || !bool(active)
                || !bool(planned) || !bool(health) || mission == null || mission.isBlank()
                || value.eventTimeMillis() <= 0) {
            invalid.inc();
            return;
        }
        State s = state.value();
        long eventTime = value.eventTimeMillis();
        // A new UUID is not a generation number. Older event times never reset live state.
        if (s != null && eventTime <= s.eventTime) { late.inc(); return; }
        long now = ctx.timerService().currentProcessingTime();
        if (s == null) s = new State();
        if (s.deadline > 0) ctx.timerService().deleteProcessingTimeTimer(s.deadline);
        long freshness = value.freshnessMs(staleAfterMs);
        boolean brokenContinuity = s.stale || (s.eventTime > 0 && eventTime - s.eventTime >
                Math.max(staleAfterMs, s.freshnessMs))
                || !Objects.equals(s.sourceBoot, value.sourceBootId()) || !Objects.equals(s.mission, mission);
        boolean wasStale = s.stale;
        s.boot = value.bootId();
        s.sourceBoot = value.sourceBootId();
        s.seq = value.seq();
        s.eventTime = eventTime;
        s.mission = mission;
        s.stale = false;
        if (wasStale) emit(s, ctx.getCurrentKey(), AlertEvent.Type.TELEMETRY_RECOVERED,
                AlertEvent.Severity.INFO, now, "유효한 차량 상태 수신 재개", out);
        if (brokenContinuity) {
            s.stoppedSince = null;
            if (s.stopAlert) emit(s, ctx.getCurrentKey(), AlertEvent.Type.STOP_CLEARED,
                    AlertEvent.Severity.INFO, now, "관측 연속성 또는 임무 변경으로 정지 판정 종료", out);
            s.stopAlert = false;
        }
        boolean stopped = active == 1 && planned == 0 && speed <= 0.1;
        if (stopped) {
            if (s.stoppedSince == null) s.stoppedSince = eventTime;
            if (!s.stopAlert && eventTime - s.stoppedSince >= stopAfterMs) {
                s.stopAlert = true;
                emit(s, ctx.getCurrentKey(), AlertEvent.Type.UNPLANNED_STOP, AlertEvent.Severity.WARNING,
                        now, "활성 임무 중 비계획 정지 관측이 " + stopAfterMs / 1000 + "초 이상 이어짐", out);
            }
        } else {
            s.stoppedSince = null;
            if (s.stopAlert) emit(s, ctx.getCurrentKey(), AlertEvent.Type.STOP_CLEARED,
                    AlertEvent.Severity.INFO, now, "비계획 정지 조건 해소", out);
            s.stopAlert = false;
        }
        boolean healthy = health == 1;
        if (!healthy && !Boolean.FALSE.equals(s.healthy)) emit(s, ctx.getCurrentKey(),
                AlertEvent.Type.SENSOR_FAULT, AlertEvent.Severity.CRITICAL, now, "필수 센서 오류 보고", out);
        if (healthy && Boolean.FALSE.equals(s.healthy)) emit(s, ctx.getCurrentKey(),
                AlertEvent.Type.SENSOR_RECOVERED, AlertEvent.Severity.INFO, now, "필수 센서 정상 복귀 보고", out);
        s.healthy = healthy;
        s.freshnessMs = freshness;
        s.deadline = now + freshness;
        ctx.timerService().registerProcessingTimeTimer(s.deadline);
        state.update(s);
    }

    @Override public void onTimer(long timestamp, OnTimerContext ctx, Collector<AlertEvent> out) throws Exception {
        State s = state.value();
        if (s == null || s.deadline != timestamp || s.stale) return;
        s.stale = true;
        s.stoppedSince = null;
        if (s.stopAlert) emit(s, ctx.getCurrentKey(), AlertEvent.Type.STOP_CLEARED,
                AlertEvent.Severity.INFO, timestamp, "정보 미수신으로 정지 판정 종료; 운행 재개를 뜻하지 않음", out);
        s.stopAlert = false;
        emit(s, ctx.getCurrentKey(), AlertEvent.Type.TELEMETRY_STALE, AlertEvent.Severity.WARNING,
                timestamp, "유효한 차량 상태 미수신; 현재 상태 확인 필요", out);
        state.update(s);
    }

    private void emit(State s, String vehicle, AlertEvent.Type type, AlertEvent.Severity severity,
                      long detected, String message, Collector<AlertEvent> out) {
        out.collect(new AlertEvent(vehicle + ":" + s.boot + ":" + type + ":" + s.seq,
                type, severity, vehicle, s.boot, s.seq, s.eventTime, detected,
                null, null, null, message));
        emitted.inc();
    }

    private static boolean bool(Double value) { return value != null && (value == 0 || value == 1); }
}
