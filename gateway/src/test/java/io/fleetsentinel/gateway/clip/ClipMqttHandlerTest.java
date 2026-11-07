package io.fleetsentinel.gateway.clip;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

class ClipMqttHandlerTest {
    final ClipStore store = mock(ClipStore.class);
    final ClipObjectStore objects = mock(ClipObjectStore.class);
    final ClipMqttHandler handler = new ClipMqttHandler(store, objects);
    byte[] wire(String operation, String body) {
        return ("{\"version\":1,\"correlation_id\":\"call-1\",\"operation\":\"" + operation
                + "\",\"job_id\":\"job-1\",\"body\":" + body + "}").getBytes(StandardCharsets.UTF_8);
    }
    @Test void topicIdentityScopesJobAndErrorsRemainCorrelated() {
        when(store.get("vehicle-1", "job-1")).thenThrow(new ResponseStatusException(HttpStatus.NOT_FOUND));
        var response = handler.handle("fleet/v1/vehicle-1/clip/request", wire("status", "{}"), false).orElseThrow();
        assertThat(response.topic()).isEqualTo("fleet/v1/vehicle-1/clip/response");
        var data = ClipJson.MAPPER.readTree(response.payload());
        assertThat(data.get("correlation_id").asString()).isEqualTo("call-1");
        assertThat(data.get("status").asInt()).isEqualTo(404);
        verify(store).get("vehicle-1", "job-1");
    }
    @Test void invalidRetainedAndOversizedRequestsNeverReachStore() {
        assertThat(handler.handle("fleet/v1/vehicle-1/clip/request", wire("status", "{}"), true)).isEmpty();
        assertThat(handler.handle("fleet/v1/vehicle-1/clip/response", wire("status", "{}"), false)).isEmpty();
        assertThat(handler.handle("fleet/v1/vehicle-1/clip/request", new byte[1024*1024+1], false)).isEmpty();
        assertThat(handler.handle("fleet/v1/vehicle-1/clip/request", "{}".getBytes(), false)).isEmpty();
        verifyNoInteractions(store, objects);
    }
    @Test void completeUsesDurableStoreAndDoesNotClaimKafkaCompletion() {
        var job = new ClipJob(); job.id="job-1"; job.state=ClipState.VERIFYING; job.metadata=new ClipJson().write(new ClipApi.Create("file", 1, 2, 3, "0".repeat(64),
                16777216, null, java.util.List.of("camera"), 1, java.util.Map.of()));
        when(store.complete(eq("vehicle-1"), eq("job-1"), any())).thenReturn(job);
        var response = handler.handle("fleet/v1/vehicle-1/clip/request",
                wire("complete", "{\"upload_id\":\"generation-1\",\"parts\":[{\"part_number\":1,\"etag\":\"tag\"}]}"), false).orElseThrow();
        assertThat(ClipJson.MAPPER.readTree(response.payload()).get("data").get("state").asString()).isEqualTo("VERIFYING");
        verify(store).complete(eq("vehicle-1"), eq("job-1"), any());
    }
}
