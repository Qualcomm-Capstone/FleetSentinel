package io.fleetsentinel.pipeline.sink;

import io.fleetsentinel.pipeline.model.Decoded;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import lombok.extern.slf4j.Slf4j;

/**
 * ClickHouse 배치 INSERT 싱크.
 *
 * 이 싱크는 exactly-once 가 아니다. ClickHouse 는 체크포인트에 걸친 트랜잭션을 지원하지
 * 않아 2PC 를 못 한다. 복구하면 마지막 체크포인트 이후에 쓴 행이 다시 쓰인다.
 *
 * 그 중복은 ReplacingMergeTree 가 (vehicle_id, boot_id, seq) 기준으로 흡수한다. 다만
 * 머지가 돌 때 지우므로 중복이 사라지는 시점이 쓰기가 아니라 읽기다. 그래서 질의는
 * FINAL 이 박힌 뷰를 봐야 한다.
 *
 * 앞단의 dedup 이 이걸 못 막는 이유는, 복구할 때 dedup 상태도 같이 되감겨서 다시 흘러온
 * 레코드를 처음 보는 것으로 통과시키기 때문이다. 두 장치가 서로 다른 구멍을 막는다.
 *
 * 배치로 넣는 이유는 ClickHouse 가 작은 INSERT 를 싫어해서다 — 파트가 폭증해 머지가
 * 못 따라간다. 배치가 차거나 체크포인트가 올 때만 flush 한다.
 */
@Slf4j
public class ClickHouseSink extends RichSinkFunction<Decoded> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private final String url;
    private final String user;
    private final String password;
    private final int batchSize;

    private transient Connection conn;
    private transient List<Decoded> buffer;

    public ClickHouseSink(String url, String user, String password, int batchSize) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.batchSize = batchSize;
    }

    @Override
    public void open(OpenContext ctx) throws Exception {
        buffer = new ArrayList<>(batchSize);
        // Each Flink job has its own classloader; DriverManager's global registry can
        // retain a driver from a previous deployment and hide it from this job.
        var properties = new java.util.Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        conn = new com.clickhouse.jdbc.ClickHouseDriver().connect(url, properties);
        if (conn == null) throw new SQLException("Unsupported ClickHouse JDBC URL: " + url);
        conn.setAutoCommit(true);
        log.info("ClickHouse 연결: {}", url);
    }

    @Override
    public void invoke(Decoded value, Context context) throws Exception {
        buffer.add(value);
        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    /** 체크포인트를 찍기 직전에 불린다. */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 여기서 비운다. 안 비우면 버퍼에 남은 것이 다음 체크포인트로 밀리고,
        // 그만큼 복구할 때 다시 흘려야 할 구간이 커진다.
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        // 상태를 들지 않는다 — 버퍼는 체크포인트마다 비워지고, 재생은 소스가 담당한다.
    }

    @Override
    public void close() throws Exception {
        try {
            flush();
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    private void flush() throws SQLException {
        if (buffer.isEmpty()) {
            return;
        }
        // 한 스트림에 신호·객체 메타데이터·세그먼트가 섞여 오므로 종류별로 갈라 각 테이블에 넣는다.
        for (Decoded.Kind kind : Decoded.Kind.values()) {
            List<Decoded> rows = buffer.stream().filter(d -> d.kind() == kind).toList();
            if (!rows.isEmpty()) {
                insert(kind, rows);
            }
        }
        buffer.clear();
    }

    private void insert(Decoded.Kind kind, List<Decoded> rows) throws SQLException {
        ClickHouseInsert insert = ClickHouseInsert.forKind(kind);
        try (PreparedStatement statement = conn.prepareStatement(insert.sql())) {
            for (Decoded row : rows) {
                insert.bind(statement, row);
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }
}
