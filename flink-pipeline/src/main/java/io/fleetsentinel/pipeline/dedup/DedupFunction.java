package io.fleetsentinel.pipeline.dedup;

import io.fleetsentinel.pipeline.dedup.SeqWindow.Verdict;
import io.fleetsentinel.pipeline.model.Envelope;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 전송 identity별 시퀀스 윈도우로 중복을 제거한다.
 *
 * 앞의 keyBy는 legacy의 vehicle_id, MQTT의 vehicle_id/boot_id/stream_id를 사용한다.
 * 상태·이력의 독립 시퀀스가 서로를 중복으로 오인하지 않도록 ValueState를 분리한다.
 *
 * 게이트웨이가 아니라 여기인 이유는, 게이트웨이에 두면 인스턴스가 죽을 때 상태가 같이
 * 사라져 중복이 저장 계층까지 그대로 들어가기 때문이다. Flink 상태는 체크포인트로 살아난다.
 *
 * 못 막는 것도 있다. Flink→ClickHouse 구간의 중복은 여기서 안 잡힌다 — 체크포인트로
 * 복구하면 이 상태도 같이 되감겨서, 다시 흘러온 레코드를 처음 보는 것으로 통과시킨다.
 * 그 구간은 ReplacingMergeTree 가 흡수한다. 두 장치가 서로 다른 구멍을 막는다.
 */
public class DedupFunction extends KeyedProcessFunction<String, Envelope, Envelope> {

    private static final long serialVersionUID = 1L;

    private final int window;

    private transient ValueState<SeqWindow> state;
    private transient Counter accepted;
    private transient Counter duplicate;
    private transient Counter late;
    private transient Counter tooOld;
    private transient Counter bootReset;

    public DedupFunction(int window) {
        this.window = window;
    }

    @Override
    public void open(OpenContext openContext) {
        // 키(차량) 하나마다 값 하나를 들고 있는 상태. 이름("seq-window")으로 식별하므로
        // 재배포해도 같은 이름이면 예전 값을 이어받는다.
        state = getRuntimeContext().getState(
                new ValueStateDescriptor<>("seq-window", SeqWindow.class));

        var group = getRuntimeContext().getMetricGroup().addGroup("dedup");
        accepted = group.counter("accepted");
        duplicate = group.counter("duplicate");
        late = group.counter("late");
        // too_old 는 유실일 수 있다 — 윈도우 밖으로 밀려난 뒤에 도착한 것이라서다.
        // 0이 아니면 윈도우를 키우거나 지연 원인을 봐야 한다.
        tooOld = group.counter("too_old");
        bootReset = group.counter("boot_reset");
    }

    @Override
    public void processElement(Envelope env, Context ctx, Collector<Envelope> out)
            throws Exception {
        if (env.validationError() != null) {
            out.collect(env);
            return;
        }
        // 이 차량의 비트맵을 꺼낸다. 처음 보는 차량이면 null 이라 새로 만든다.
        SeqWindow w = state.value();
        if (w == null) {
            w = new SeqWindow(window);
        }

        Verdict verdict = w.accept(env.bootId(), env.seq());
        // accept 가 w 를 바꿨으므로 다시 넣어 줘야 저장된다.
        state.update(w);

        switch (verdict) {
            case ACCEPT -> accepted.inc();
            case LATE -> {
                late.inc();
                accepted.inc();
            }
            case BOOT_RESET -> {
                bootReset.inc();
                accepted.inc();
            }
            case DUPLICATE -> duplicate.inc();
            case TOO_OLD -> tooOld.inc();
        }

        // MQTT retries can arrive outside the bitmap after bridge failover. Dropping
        // an unseen late record would violate durable upload; ClickHouse's stable key
        // handles duplicate inserts. Live alert operators separately reject stale time.
        if (verdict.pass() || (env.mqtt() && verdict == Verdict.TOO_OLD)) {
            out.collect(env);
        }
    }
}
