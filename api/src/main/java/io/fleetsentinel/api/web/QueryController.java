package io.fleetsentinel.api.web;

import io.fleetsentinel.api.query.TelemetryQueries;
import io.fleetsentinel.api.model.ClipSummary;
import io.fleetsentinel.api.model.PipelineHealth;
import io.fleetsentinel.api.model.VehicleSummary;
import java.util.List;
import jakarta.validation.constraints.Min;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 차량·클립·파이프라인 상태 조회 API. */
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class QueryController {

    private final TelemetryQueries queries;

    /** 차량 로스터. 프론트가 SSE 를 열기 전에 차량 메타를 심는다. */
    @GetMapping("/vehicles")
    public VehicleSummary.Roster vehicles() {
        return new VehicleSummary.Roster(queries.vehicles());
    }

    /** 클립 카탈로그. 검색 결과의 재생 진입점이다. */
    @GetMapping("/clips")
    public List<ClipSummary> clips(@RequestParam(defaultValue = "100") @Min(1) int limit) {
        return queries.clips(Math.min(limit, 1000));
    }

    /** 파이프라인 적재량·결번·중복 적재량. */
    @GetMapping("/health")
    public PipelineHealth health() {
        return queries.health();
    }
}
