package io.fleetsentinel.pipeline.model;

import java.io.Serializable;

/**
 * 실패 격리 봉투. schemas/dlq-envelope.avsc 에 대응한다.
 *
 * 원본 바이트를 그대로 보존한다. 파싱 실패를 격리하는 자리에서 파싱된 값을 저장하면
 * 앞뒤가 안 맞는다.
 */
public final class DlqRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 실패를 네 가지로 나눈다. 어디서 깨졌는지에 따라 대응이 다르기 때문이다. */
    public enum ErrorClass {
        /** 바이트를 스키마로 못 읽었다 */
        PARSE_FAILURE,
        /** 읽었지만 필드가 계약에 안 맞다 */
        SCHEMA_VALIDATION_FAILURE,
        /** 필드는 맞지만 값이 물리적으로 불가능하다 */
        BUSINESS_RULE_FAILURE,
        /** 싱크 쓰기가 끝내 실패했다 */
        SINK_WRITE_FAILURE
    }

    private final byte[] originalPayload;
    private final ErrorClass errorClass;
    private final String errorDetail;
    private final String sourceSubscription;
    private final String pipelineStep;
    private final long processingTime;
    private final int attempt;
    private final Envelope source;

    public DlqRecord(byte[] originalPayload, ErrorClass errorClass, String errorDetail,
                     String sourceSubscription, String pipelineStep,
                     long processingTime, int attempt) {
        this(originalPayload, errorClass, errorDetail, sourceSubscription, pipelineStep,
                processingTime, attempt, null);
    }

    private DlqRecord(byte[] originalPayload, ErrorClass errorClass, String errorDetail,
                      String sourceSubscription, String pipelineStep, long processingTime,
                      int attempt, Envelope source) {
        this.originalPayload = originalPayload;
        this.errorClass = errorClass;
        this.errorDetail = errorDetail;
        this.sourceSubscription = sourceSubscription;
        this.pipelineStep = pipelineStep;
        this.processingTime = processingTime;
        this.attempt = attempt;
        this.source = source;
    }

    public static DlqRecord of(Envelope env, ErrorClass cls, String step, String detail) {
        return new DlqRecord(env.payload(), cls, detail, env.topic(), step,
                System.currentTimeMillis(), 1, env);
    }

    public Envelope source() { return source; }

    public byte[] originalPayload() {
        return originalPayload;
    }

    public ErrorClass errorClass() {
        return errorClass;
    }

    public String errorDetail() {
        return errorDetail;
    }

    public String sourceSubscription() {
        return sourceSubscription;
    }

    public String pipelineStep() {
        return pipelineStep;
    }

    public long processingTime() {
        return processingTime;
    }

    public int attempt() {
        return attempt;
    }
}
