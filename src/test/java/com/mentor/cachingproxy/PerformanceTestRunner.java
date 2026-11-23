package com.mentor.cachingproxy;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;

/**
 * Standalone Java Performance Load Test Runner.
 * Simulates concurrent load against the caching proxy gateway, checking for Cache Stampede
 * coalescing and measuring p50/p95 response latencies.
 */
public class PerformanceTestRunner {

    private static final String TARGET_URL = "http://localhost:8080/delay/2";
    private static final int CONCURRENT_REQUESTS = 40;

    public static void main(String[] args) throws Exception {
        System.out.println("==========================================================");
        System.out.println("    PROXY PERFORMANCE TEST RUNNER - CACHE STAMPEDE LOAD   ");
        System.out.println("==========================================================");
        System.out.println("Target: " + TARGET_URL);
        System.out.println("Concurrent requests: " + CONCURRENT_REQUESTS);
        System.out.println("Simulating sudden spike of uncached delayed requests...");
        System.out.println("----------------------------------------------------------");

        // Clear target key first if application is up (we can invoke administrative DELETE /cache/GET:/delay/2)
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        try {
            HttpRequest deleteRequest = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:8080/cache/GET:/delay/2"))
                    .DELETE()
                    .build();
            client.send(deleteRequest, HttpResponse.BodyHandlers.discarding());
            System.out.println("[PREPARATION] Evicted previous cache key for /delay/2 to ensure clean MISS");
        } catch (Exception e) {
            System.out.println("[PREPARATION] Eviction skipped (server might be starting up or clean)");
        }

        // Setup collections for stats
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        Map<String, Integer> cacheStatusCounts = new ConcurrentHashMap<>();
        Map<Integer, Integer> statusCodeCounts = new ConcurrentHashMap<>();

        long runStart = System.currentTimeMillis();

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_REQUESTS; i++) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(TARGET_URL))
                    .GET()
                    .build();

            long requestStart = System.currentTimeMillis();

            CompletableFuture<Void> future = client.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                    .thenAccept(response -> {
                        long duration = System.currentTimeMillis() - requestStart;
                        latencies.add(duration);

                        int statusCode = response.statusCode();
                        statusCodeCounts.merge(statusCode, 1, Integer::sum);

                        String xCache = response.headers().firstValue("X-Cache").orElse("UNKNOWN");
                        cacheStatusCounts.merge(xCache, 1, Integer::sum);
                    })
                    .exceptionally(ex -> {
                        long duration = System.currentTimeMillis() - requestStart;
                        latencies.add(duration);
                        statusCodeCounts.merge(500, 1, Integer::sum);
                        cacheStatusCounts.merge("ERROR: " + ex.getClass().getSimpleName(), 1, Integer::sum);
                        return null;
                    });

            futures.add(future);
        }

        // Wait for all requests to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        long runDuration = System.currentTimeMillis() - runStart;

        // Compute percentiles
        List<Long> sortedLatencies = new ArrayList<>(latencies);
        Collections.sort(sortedLatencies);

        int size = sortedLatencies.size();
        long min = size > 0 ? sortedLatencies.get(0) : 0;
        long max = size > 0 ? sortedLatencies.get(size - 1) : 0;
        double avg = sortedLatencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
        long p50 = size > 0 ? sortedLatencies.get((int) (size * 0.50)) : 0;
        long p95 = size > 0 ? sortedLatencies.get((int) (size * 0.95)) : 0;
        long p99 = size > 0 ? sortedLatencies.get((int) (size * 0.99)) : 0;

        System.out.println();
        System.out.println("==========================================================");
        System.out.println("                      TEST RESULTS                        ");
        System.out.println("==========================================================");
        System.out.printf("Total Execution Time: %d ms%n", runDuration);
        System.out.printf("Average Latency:      %.2f ms%n", avg);
        System.out.printf("Min Latency:          %d ms%n", min);
        System.out.printf("p50 (Median):         %d ms%n", p50);
        System.out.printf("p95 Percentile:       %d ms%n", p95);
        System.out.printf("p99 Percentile:       %d ms%n", p99);
        System.out.printf("Max Latency:          %d ms%n", max);
        System.out.println("----------------------------------------------------------");

        System.out.println("HTTP Status Codes Summary:");
        statusCodeCounts.forEach((code, count) -> 
                System.out.printf("  HTTP %d: %d requests%n", code, count));

        System.out.println("----------------------------------------------------------");
        System.out.println("X-Cache Header Distribution (Stampede Check):");
        cacheStatusCounts.forEach((status, count) -> 
                System.out.printf("  %s: %d requests%n", status, count));

        System.out.println("==========================================================");
        
        // Final validation logic
        if (cacheStatusCounts.containsKey("MISS") && cacheStatusCounts.containsKey("MISS-STAMPEDE")) {
            System.out.println("[SUCCESS] Cache Stampede coalesced concurrent calls successfully!");
            System.out.printf("          Initiator (MISS): %d, Coalesced (MISS-STAMPEDE): %d%n",
                    cacheStatusCounts.get("MISS"), cacheStatusCounts.get("MISS-STAMPEDE"));
        } else if (cacheStatusCounts.containsKey("BYPASS")) {
            System.out.println("[WARN] Cache bypassed. Ensure Redis or cache settings are correct.");
        } else {
            System.out.println("[INFO] Concurrency check: Ensure the server is caching requests correctly.");
        }
        System.out.println("==========================================================");
    }
}
