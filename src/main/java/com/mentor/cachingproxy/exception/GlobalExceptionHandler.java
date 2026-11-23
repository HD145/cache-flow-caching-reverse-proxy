package com.mentor.cachingproxy.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * RestControllerAdvice providing global mapping of custom exceptions to standard JSON formats.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * Handles BadGatewayException and maps it to HTTP 502 Bad Gateway.
     *
     * @param ex the caught BadGatewayException
     * @return a Mono/ResponseEntity containing structured error JSON
     */
    @ExceptionHandler(BadGatewayException.class)
    public ResponseEntity<Map<String, Object>> handleBadGateway(BadGatewayException ex) {
        return createErrorResponse(HttpStatus.BAD_GATEWAY, ex.getMessage());
    }

    /**
     * Handles GatewayTimeoutException and maps it to HTTP 504 Gateway Timeout.
     *
     * @param ex the caught GatewayTimeoutException
     * @return a Mono/ResponseEntity containing structured error JSON
     */
    @ExceptionHandler(GatewayTimeoutException.class)
    public ResponseEntity<Map<String, Object>> handleGatewayTimeout(GatewayTimeoutException ex) {
        return createErrorResponse(HttpStatus.GATEWAY_TIMEOUT, ex.getMessage());
    }

    private ResponseEntity<Map<String, Object>> createErrorResponse(HttpStatus status, String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", Instant.now().toString());
        body.put("status", status.value());
        body.put("error", status.getReasonPhrase());
        body.put("message", message);
        return ResponseEntity.status(status).body(body);
    }
}
