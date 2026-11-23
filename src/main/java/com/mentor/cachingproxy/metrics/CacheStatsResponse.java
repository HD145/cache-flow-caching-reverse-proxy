package com.mentor.cachingproxy.metrics;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Data Transfer Object containing a snapshot of caching metrics.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CacheStatsResponse {

    /**
     * Total number of requests processed by the proxy.
     */
    private long totalRequests;

    /**
     * Total number of cache hits.
     */
    private long cacheHits;

    /**
     * Total number of cache misses.
     */
    private long cacheMisses;

    /**
     * Total number of backend calls executed.
     */
    private long backendCalls;

    /**
     * The cache hit ratio (cacheHits / totalRequests).
     */
    private double cacheHitRatio;
}
