package com.mentor.cachingproxy.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mentor.cachingproxy.model.CachedResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Service managing Redis cache read, write, and key generation operations.
 * Implements graceful fallback to bypass the cache if Redis is down.
 */
@Slf4j
@Service
public class CacheService {

    private final ReactiveRedisTemplate<String, byte[]> redisTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Constructor injection for reactive redis template and jackson objectMapper.
     *
     * @param redisTemplate the configured template for keys
     * @param objectMapper the object mapper for json serialization
     */
    public CacheService(ReactiveRedisTemplate<String, byte[]> redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Generates a unique, normalized cache key based on path and query parameters.
     * Query parameters are sorted alphabetically to prevent duplicate caches for different query orderings.
     *
     * @param request the incoming ServerHttpRequest
     * @return a structured cache key string
     */
    public String generateKey(ServerHttpRequest request) {
        return buildKey(request.getPath().pathWithinApplication().value(), request.getURI().getRawQuery());
    }

    /**
     * Builds the canonical cache key from a path and raw query string (query params sorted).
     */
    static String buildKey(String path, String rawQuery) {
        if (rawQuery == null || rawQuery.isBlank()) {
            return "proxy:cache:GET:" + path;
        }

        String sortedQuery = Arrays.stream(rawQuery.split("&"))
                .sorted()
                .collect(Collectors.joining("&"));

        return "proxy:cache:GET:" + path + ":" + sortedQuery;
    }

    /**
     * Retrieves a cached response from Redis by key.
     * Gracefully logs and returns Mono.empty() if Redis is unavailable.
     *
     * @param key the cache key
     * @return a Mono containing the CachedResponse, or empty if miss or Redis down
     */
    public Mono<CachedResponse> get(String key) {
        return redisTemplate.opsForValue().get(key)
                .flatMap(bytes -> {
                    try {
                        CachedResponse response = objectMapper.readValue(bytes, CachedResponse.class);
                        return Mono.just(response);
                    } catch (Exception e) {
                        log.error("Failed to deserialize cached response for key: {}", key, e);
                        return Mono.empty();
                    }
                })
                .onErrorResume(e -> {
                    log.warn("Redis lookup failed for key: {}. Error: {}. Bypassing cache.", key, e.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Stores a response in Redis with a specified Time-to-Live.
     * Gracefully logs and returns false if Redis is unavailable.
     *
     * @param key the cache key
     * @param response the response payload to store
     * @param ttl the Time-to-Live duration
     * @return a Mono emitting true if saved successfully, false otherwise
     */
    public Mono<Boolean> put(String key, CachedResponse response, Duration ttl) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            return Mono.just(false); // Redis rejects non-positive expirations
        }
        return Mono.fromCallable(() -> objectMapper.writeValueAsBytes(response))
                .flatMap(bytes -> redisTemplate.opsForValue().set(key, bytes, ttl))
                .onErrorResume(e -> {
                    log.warn("Failed to write to Redis cache for key: {}. Error: {}. Bypassing store.", key, e.getMessage());
                    return Mono.just(false);
                });
    }

    /**
     * Purges all keys starting with the proxy cache prefix.
     * Uses non-blocking SCAN to scan keys in batches and delete them.
     *
     * @return a Mono signaling completion
     */
    public Mono<Void> purgeAll() {
        ScanOptions options = ScanOptions.scanOptions()
                .match("proxy:cache:*")
                .count(1000)
                .build();

        return redisTemplate.scan(options)
                .flatMap(redisTemplate::delete)
                .then()
                .onErrorResume(e -> {
                    log.error("Failed to purge all cache keys. Error: {}", e.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Safely deletes a specific key from Redis.
     * Validates and prepends prefix if missing to restrict deletion within the proxy namespace.
     *
     * @param key the targeted key
     * @return a Mono emitting true if deleted successfully, false otherwise
     */
    public Mono<Boolean> delete(String key) {
        String finalKey;
        if (key.startsWith("proxy:cache:")) {
            finalKey = key;
        } else {
            String target = key.startsWith("GET:") ? key.substring(4) : key;
            if (!target.startsWith("/")) {
                target = "/" + target;
            }
            // Normalize "path?query" into the same canonical form used when storing
            int q = target.indexOf('?');
            finalKey = q < 0 ? buildKey(target, null) : buildKey(target.substring(0, q), target.substring(q + 1));
        }
        
        return redisTemplate.delete(finalKey)
                .map(count -> count > 0)
                .onErrorResume(e -> {
                    log.error("Failed to delete cache key: {}. Error: {}", finalKey, e.getMessage());
                    return Mono.just(false);
                });
    }
}
