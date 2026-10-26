package io.fleetsentinel.gateway.config;

import java.time.Duration;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/** 게이트웨이 고유 설정. gRPC 서버·TLS는 {@code spring.grpc.*}가 담당한다. */
@Getter
@Setter
@ConfigurationProperties(prefix = "fleetsentinel.gateway")
@Validated
public class GatewayProperties {

    @Valid
    private final Topics topics = new Topics();
    @Valid
    private final Ack ack = new Ack();

    /**
     * ack 없이 받아들일 수 있는 최대 미완료 레코드 수. 넘으면 스트림을 끊는다.
     *
     * <p>차량 쪽에도 같은 이름의 상한이 있지만(WalShipper.max_inflight), 그건 차량이 지키기로
     * 한 약속이지 게이트웨이가 강제한 값이 아니다. 지키지 않는 클라이언트가 힙을 밀어버릴 수
     * 있으므로 서버가 자기 한도를 따로 갖는다.
     */
    @Positive
    private int maxInflight = 4096;

    @Getter
    @Setter
    public static class Topics {
        /**
         * 계층 셋이 한 토픽으로 간다. 나누면 `seq` 단일 수열이 조각나
         * dedup 이 연속성을 볼 수 없다(RecordPublisher.topicFor 참조).
         */
        @NotBlank
        private String records = "telemetry.records";
        @NotBlank
        private String dlq = "telemetry.dlq";
    }

    @Getter
    @Setter
    public static class Ack {
        /** 이만큼 전진하면 CACK을 방출한다. */
        @Positive
        private int everyN = 128;

        /** 전진량이 적어도 이 시간이 지나면 방출한다. 저부하에서 커밋이 멈추지 않게 한다. */
        private Duration every = Duration.ofMillis(200);

        @AssertTrue(message = "ack.every must be at least 1ms")
        public boolean isEveryValid() {
            return every != null && every.compareTo(Duration.ofMillis(1)) >= 0;
        }
    }
}
