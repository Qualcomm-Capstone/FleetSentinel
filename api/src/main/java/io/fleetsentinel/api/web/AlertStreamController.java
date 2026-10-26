package io.fleetsentinel.api.web;

import io.fleetsentinel.api.alert.AlertHub;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class AlertStreamController {

    private final AlertHub hub;

    @GetMapping(value = "/alerts", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter alerts(
            @RequestParam(required = false) String vehicle,
            @RequestParam(required = false) String from,
            @RequestHeader(name = "Last-Event-ID", required = false) String lastEventId) {
        return hub.subscribe(vehicle, lastEventId != null ? lastEventId : from);
    }
}
