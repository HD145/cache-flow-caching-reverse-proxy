package com.mentor.cachingproxy.controller;

import com.mentor.cachingproxy.metrics.CacheMetricsTracker;
import com.mentor.cachingproxy.metrics.CacheStatsResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Controller exposing caching performance endpoints.
 */
@RestController
public class MetricsController {

    private final CacheMetricsTracker metricsTracker;

    /**
     * Constructor injection for CacheMetricsTracker.
     *
     * @param metricsTracker the metrics tracker service
     */
    public MetricsController(CacheMetricsTracker metricsTracker) {
        this.metricsTracker = metricsTracker;
    }

    /**
     * Exposes stats JSON snapshot.
     * Accessible via GET /cache/stats.
     *
     * @return a Mono containing the stats DTO
     */
    @GetMapping("/cache/stats")
    public Mono<CacheStatsResponse> getCacheStats() {
        return Mono.fromCallable(metricsTracker::getStatsSnapshot);
    }

    /**
     * Exposes stats JSON snapshot.
     * Accessible via GET /metrics.
     *
     * @return a Mono containing the stats DTO
     */
    @GetMapping("/metrics")
    public Mono<CacheStatsResponse> getMetrics() {
        return Mono.fromCallable(metricsTracker::getStatsSnapshot);
    }
}
