package io.fleetsentinel.api.web;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@RestControllerAdvice
@Slf4j
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {
    @ExceptionHandler(DataAccessException.class)
    public ProblemDetail databaseFailure(DataAccessException error) {
        log.error("관제 데이터 조회 실패", error);
        boolean unavailable = error instanceof TransientDataAccessException
                || error instanceof DataAccessResourceFailureException;
        return ProblemDetail.forStatusAndDetail(
                unavailable ? HttpStatus.SERVICE_UNAVAILABLE : HttpStatus.INTERNAL_SERVER_ERROR,
                unavailable ? "Telemetry storage is temporarily unavailable." : "Telemetry query failed.");
    }
}
