package io.fleetsentinel.api.alert;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/** 알림 토픽을 읽어 API 인스턴스에 연결된 SSE 구독자에게 broadcast한다. */
@Component
@Slf4j
@RequiredArgsConstructor
public class KafkaAlertConsumer {

    private final ObjectMapper json;
    private final AlertHub hub;

    @KafkaListener(
            topics = "${fleetsentinel.alerts.topic:fleet.alerts}",
            groupId = "${fleetsentinel.alerts.consumer-group}")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            AlertMessage alert = json.readValue(record.value(), AlertMessage.class);
            if (record.key() != null && !record.key().equals(alert.vehicleId())) {
                throw new IllegalArgumentException("Kafka key와 vehicleId가 다르다");
            }
            hub.publish(alert);
        } catch (JacksonException | IllegalArgumentException error) {
            // 잘못된 한 건이 같은 offset에서 무한 재시도되어 전체 알림을 막지 않게 건너뛴다.
            log.error("잘못된 알림을 건너뜀: topic={} partition={} offset={} reason={}",
                    record.topic(), record.partition(), record.offset(), error.getMessage());
        }
    }
}
