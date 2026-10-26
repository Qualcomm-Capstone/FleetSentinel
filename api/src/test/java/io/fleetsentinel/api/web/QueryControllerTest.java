package io.fleetsentinel.api.web;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.fleetsentinel.api.query.TelemetryQueries;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;
import java.util.stream.Stream;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(QueryController.class)
class QueryControllerTest {
    @Autowired MockMvc mvc;
    @MockitoBean TelemetryQueries queries;

    @ParameterizedTest(name = "limit={0} → HTTP 400, no database query")
    @ValueSource(strings = {"-1", "0", "not-a-number", "2147483648"})
    void rejectsInvalidLimit(String limit) throws Exception {
        // Given
        var request = get("/api/clips").param("limit", limit);
        // When / Then
        mvc.perform(request).andExpect(status().isBadRequest());
        verifyNoInteractions(queries);
    }

    @ParameterizedTest(name = "limit={0} → database limit={1}")
    @CsvSource({"1,1", "1000,1000", "1001,1000", "2147483647,1000"})
    void preservesUpperLimitCap(int requested, int expected) throws Exception {
        // Given
        var request = get("/api/clips").param("limit", String.valueOf(requested));
        // When
        mvc.perform(request).andExpect(status().isOk());
        // Then
        verify(queries).clips(expected);
    }

    static Stream<Arguments> queryFailures() {
        return Stream.of(
                Arguments.of(new DataAccessResourceFailureException("private connection details"), 503,
                        "Telemetry storage is temporarily unavailable."),
                Arguments.of(new InvalidDataAccessResourceUsageException("private SQL details"), 500,
                        "Telemetry query failed."));
    }

    @ParameterizedTest
    @MethodSource("queryFailures")
    void returnsSafeErrorContract(DataAccessException error, int code, String detail) throws Exception {
        // Given
        when(queries.clips(100)).thenThrow(error);
        // When / Then
        mvc.perform(get("/api/clips"))
                .andExpect(status().is(code))
                .andExpect(jsonPath("$.status").value(code))
                .andExpect(jsonPath("$.detail").value(detail));
    }
}
