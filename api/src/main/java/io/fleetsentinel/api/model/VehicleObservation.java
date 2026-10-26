package io.fleetsentinel.api.model;

/** 차량별 SSE 필터링에 필요한 공통 식별자와 관측 시각. */
public interface VehicleObservation {
    String vehicleId();
    long time();
}
