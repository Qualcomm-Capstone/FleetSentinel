package io.fleetsentinel.api.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;

import io.fleetsentinel.api.model.SignalSample;
import io.fleetsentinel.api.replay.ReplayService;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class StreamControllerTest {
    private final ReplayService replay = mock(ReplayService.class);
    private final StreamController controller = new StreamController(replay);
    // 수동 push로 검증하여 스케줄러 스레드·벽시계에 의존하지 않는다.
    private final MockMvc mvc = MockMvcBuilders.standaloneSetup(controller).build();

    @Test
    void doesNotQueryWithoutSubscribers() {
        // Given: no open stream
        // When
        controller.push();
        // Then
        verifyNoInteractions(replay);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void filtersVehicleAndEmitsEpochOnlyWhenWrapped(boolean wrapped) throws Exception {
        // Given
        var v1 = new SignalSample("v1", 1, "speed", 1000, Map.of("speed", 1.0));
        var v2 = new SignalSample("v2", 1, "speed", 1000, Map.of("speed", 2.0));
        when(replay.poll()).thenReturn(new ReplayService.Frame(List.of(v1, v2), List.of(), wrapped));
        var stream = mvc.perform(get("/api/stream").param("vehicle", "v1"))
                .andExpect(request().asyncStarted()).andReturn();
        // When
        controller.push();
        // Then
        var body = stream.getResponse().getContentAsString();
        assertThat(body).contains("event:signal", "\"vehicle_id\":\"v1\"")
                .doesNotContain("\"vehicle_id\":\"v2\"");
        assertThat(body.contains("event:epoch")).isEqualTo(wrapped);
        stream.getRequest().getAsyncContext().complete();
    }

    @Test
    void keepsConnectionAfterTransientQueryFailure() throws Exception {
        // Given
        when(replay.poll()).thenThrow(new DataAccessResourceFailureException("offline"))
                .thenReturn(new ReplayService.Frame(List.of(), List.of(), true));
        var stream = mvc.perform(get("/api/stream")).andExpect(request().asyncStarted()).andReturn();
        // When
        controller.push();
        controller.push();
        // Then
        verify(replay, times(2)).poll();
        assertThat(stream.getResponse().getContentAsString()).contains("event:epoch");
        stream.getRequest().getAsyncContext().complete();
    }
}
