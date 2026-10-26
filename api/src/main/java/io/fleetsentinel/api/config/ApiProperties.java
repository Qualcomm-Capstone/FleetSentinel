package io.fleetsentinel.api.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "fleetsentinel.api")
public class ApiProperties {

    /**
     * SSE 푸시 주기(ms). 기본값은 250ms(4Hz).
     */
    private long pushIntervalMs = 250;

    /**
     * 한 번에 밀어 넣을 신호 레코드 상한.
     *
     * <p>차량당 1,308 rec/s 가 들어온다. 무제한으로 밀면 <b>브라우저가 먼저 무너진다</b> —
     * 서버가 아니라 클라이언트가 병목이다.
     */
    private int maxRecordsPerPush = 200;

    public long getPushIntervalMs() {
        return pushIntervalMs;
    }

    public void setPushIntervalMs(long pushIntervalMs) {
        this.pushIntervalMs = pushIntervalMs;
    }

    public int getMaxRecordsPerPush() {
        return maxRecordsPerPush;
    }

    public void setMaxRecordsPerPush(int maxRecordsPerPush) {
        this.maxRecordsPerPush = maxRecordsPerPush;
    }
}
