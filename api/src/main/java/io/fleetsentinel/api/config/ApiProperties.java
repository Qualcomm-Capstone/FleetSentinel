package io.fleetsentinel.api.config;

import lombok.Getter;
import lombok.Setter;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Getter
@Setter
@ConfigurationProperties(prefix = "fleetsentinel.api")
@Validated
public class ApiProperties {

    /**
     * SSE 푸시 주기(ms). 기본값은 250ms(4Hz).
     */
    @Positive
    private long pushIntervalMs = 250;

    /**
     * 재생 주기당 전송하는 신호 레코드 상한.
     */
    @Positive
    private int maxRecordsPerPush = 200;
}
