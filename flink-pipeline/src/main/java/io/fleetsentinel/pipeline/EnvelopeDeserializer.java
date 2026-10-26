package io.fleetsentinel.pipeline;

import io.fleetsentinel.pipeline.model.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;
import lombok.extern.slf4j.Slf4j;

/**
 * Kafka 레코드를 Envelope 으로 감싼다. 페이로드는 아직 열지 않는다.
 *
 * 잘못된 헤더도 원본과 Kafka 위치를 보존한다. 검증 실패 봉투는 dedup을 우회하고
 * DecodeFunction에서 DLQ로 격리한다.
 */
@Slf4j
public class EnvelopeDeserializer implements KafkaRecordDeserializationSchema<Envelope> {

    private static final long serialVersionUID = 1L;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Envelope> out) {
        try {
            out.collect(Envelope.of(record));
        } catch (IllegalArgumentException e) {
            log.warn("전송 헤더 검증 실패, DLQ로 격리: topic={} offset={} ({})",
                    record.topic(), record.offset(), e.getMessage());
            out.collect(Envelope.invalid(record, e.getMessage()));
        }
    }

    @Override
    public TypeInformation<Envelope> getProducedType() {
        return TypeInformation.of(Envelope.class);
    }
}
