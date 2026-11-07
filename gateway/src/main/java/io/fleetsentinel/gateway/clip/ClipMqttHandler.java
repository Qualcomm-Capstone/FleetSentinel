package io.fleetsentinel.gateway.clip;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

/** Vehicle identity comes only from the broker's mTLS/CN-bound topic ACL. */
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "fleetsentinel.clips.enabled", havingValue = "true", matchIfMissing = true)
public class ClipMqttHandler {
    public static final int MAX_BYTES = 1024 * 1024;
    private static final Pattern TOPIC = Pattern.compile("fleet/v1/([a-zA-Z0-9_-]{1,80})/clip/request");
    private final ClipStore store;
    private final ClipObjectStore objects;

    public record Reply(String topic, byte[] payload) { }

    public Optional<Reply> handle(String topic, byte[] payload, boolean retained) {
        var match = TOPIC.matcher(topic);
        if (retained || payload.length > MAX_BYTES || !match.matches()) return Optional.empty();
        String correlation, operation;
        tools.jackson.databind.JsonNode value;
        try {
            value = ClipJson.MAPPER.readTree(payload);
            if (value == null || !value.isObject() || !value.path("version").isIntegralNumber()
                    || value.path("version").asInt() != 1) return Optional.empty();
            correlation = value.path("correlation_id").asString();
            operation = value.path("operation").asString();
            if (!correlation.matches("[a-zA-Z0-9_-]{1,80}") || !operation.matches("[a-z_]{1,20}"))
                return Optional.empty();
        } catch (RuntimeException e) { return Optional.empty(); }
        String vehicle = match.group(1);
        int status = 200;
        Object data;
        try {
            var body = value.path("body");
            ClipApi.require(body.isObject(), "body must be an object");
            String id = value.path("job_id").asString("");
            if (!operation.equals("create")) ClipApi.require(id.matches("[a-zA-Z0-9_-]{1,80}"), "invalid job_id");
            data = switch (operation) {
                case "create" -> store.create(vehicle, ClipJson.MAPPER.treeToValue(body, ClipApi.Create.class)).view();
                case "status" -> store.get(vehicle, id).view();
                case "part_urls" -> {
                    var request = ClipJson.MAPPER.treeToValue(body, ClipApi.UrlRequest.class);
                    ClipApi.require(request.partNumbers() != null && request.partNumbers().size() <= 32,
                            "request at most 32 part URLs at a time");
                    yield objects.urls(store.get(vehicle, id), request.partNumbers());
                }
                case "complete" -> store.complete(vehicle, id, ClipJson.MAPPER.treeToValue(body, ClipApi.Complete.class)).view();
                case "cancel" -> store.cancel(vehicle, id).view();
                default -> throw new IllegalArgumentException("unsupported operation");
            };
        } catch (ResponseStatusException e) {
            status = e.getStatusCode().value(); data = Map.of("error", "operation_rejected");
        } catch (IllegalArgumentException | tools.jackson.core.JacksonException e) {
            status = 400; data = Map.of("error", "invalid_request");
        } catch (RuntimeException e) {
            status = 503; data = Map.of("error", "temporarily_unavailable");
        }
        byte[] wire = encode(correlation, operation, status, data);
        if (wire.length > MAX_BYTES) wire = encode(correlation, operation, 413, Map.of("error", "response_too_large"));
        return Optional.of(new Reply("fleet/v1/" + vehicle + "/clip/response", wire));
    }

    private byte[] encode(String correlation, String operation, int status, Object data) {
        return ClipJson.MAPPER.writeValueAsBytes(Map.of("version", 1, "correlation_id", correlation,
                "operation", operation, "status", status, "data", data));
    }
}
