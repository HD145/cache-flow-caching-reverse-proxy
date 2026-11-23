package com.mentor.cachingproxy.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * High-concurrency WebFilter for structured access logging.
 * Logs method, path, query parameters, status code, response duration, cache hit/miss status,
 * and downstream backend latency.
 */
@Slf4j
@Component
@Order(-1) // Run first in the filter chain to capture accurate start time
public class LoggingFilter implements WebFilter {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        long startTime = System.currentTimeMillis();

        return chain.filter(exchange)
                .doFinally(signalType -> {
                    long duration = System.currentTimeMillis() - startTime;
                    logAccess(exchange, duration);
                });
    }

    private void logAccess(ServerWebExchange exchange, long duration) {
        ServerHttpRequest request = exchange.getRequest();
        ServerHttpResponse response = exchange.getResponse();

        // Retrieve properties and state
        String method = request.getMethod().name();
        String path = request.getPath().pathWithinApplication().value();
        String query = request.getURI().getRawQuery();
        int statusCode = response.getStatusCode() != null ? response.getStatusCode().value() : 500;

        // Skip access logging for metrics paths to prevent log cluttering
        if (path.equals("/metrics") || path.equals("/cache/stats")) {
            return;
        }

        // Retrieve cache hit/miss indicator
        String cacheStatus = response.getHeaders().getFirst("X-Cache");
        if (cacheStatus == null) {
            cacheStatus = "BYPASS"; // If endpoint doesn't support cache (e.g. static resources or non-proxy)
        }

        // Retrieve backend latency if set on the exchange attributes
        Long backendLatency = exchange.getAttribute("logging.backend-latency-ms");
        String backendLatencyStr = backendLatency != null ? String.valueOf(backendLatency) : "N/A";

        // Structured key-value logging format
        log.info("[ACCESS-LOG] method={} path={} query={} status={} duration_ms={} cache={} backend_latency_ms={}",
                method,
                path,
                query != null ? query : "",
                statusCode,
                duration,
                cacheStatus,
                backendLatencyStr
        );
    }
}
