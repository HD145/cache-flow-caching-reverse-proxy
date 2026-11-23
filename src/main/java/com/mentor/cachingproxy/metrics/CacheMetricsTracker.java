package com.mentor.cachingproxy.metrics;

import java.util.concurrent.atomic.LongAdder;
import org.springframework.stereotype.Component;

/**
 * Thread-safe component for collecting caching proxy metrics.
 * Utilizes LongAdder for maximum performance under high write concurrency.
 */
@Component
public class CacheMetricsTracker {

    private final LongAdder totalRequests = new LongAdder();
    private final LongAdder cacheHits = new LongAdder();
    private final LongAdder cacheMisses = new LongAdder();
    private final LongAdder backendCalls = new LongAdder();

    /**
     * Increments the total incoming client requests count.
     */
    public void incrementTotalRequests() {
        totalRequests.increment();
    }

    /**
     * Increments the cache hits count.
     */
    public void incrementCacheHits() {
        cacheHits.increment();
    }

    /**
     * Increments the cache misses count.
     */
    public void incrementCacheMisses() {
        cacheMisses.increment();
    }

    /**
     * Increments the backend calls count.
     */
    public void incrementBackendCalls() {
        backendCalls.increment();
    }

    /**
     * Generates a snapshot of current metrics in a response object.
     *
     * @return the current CacheStatsResponse snapshot
     */
    public CacheStatsResponse getStatsSnapshot() {
        long requests = totalRequests.sum();
        long hits = cacheHits.sum();
        long misses = cacheMisses.sum();
        long backend = backendCalls.sum();

        double ratio = requests == 0 ? 0.0 : (double) hits / requests;

        return new CacheStatsResponse(requests, hits, misses, backend, ratio);
    }
}
