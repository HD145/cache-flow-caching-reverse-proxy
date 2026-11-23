package com.mentor.cachingproxy.controller;

import com.mentor.cachingproxy.cache.CacheService;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Controller managing cache purge and invalidation requests.
 */
@RestController
public class CacheManagementController {

    private final CacheService cacheService;

    /**
     * Constructor injection for CacheService.
     *
     * @param cacheService the cache service
     */
    public CacheManagementController(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    /**
     * Purges the entire proxy cache.
     * Accessible via DELETE /cache.
     *
     * @return a Mono containing a success response entity
     */
    @DeleteMapping("/cache")
    public Mono<ResponseEntity<String>> purgeAllCache() {
        return cacheService.purgeAll()
                .thenReturn(ResponseEntity.ok("Cache fully purged successfully"));
    }

    /**
     * Purges a specific cache key.
     * Supports keys with paths and parameters (e.g. DELETE /cache/GET:/get?a=1).
     * Accessible via DELETE /cache/**.
     *
     * @param request the current request to extract key path
     * @return a Mono containing success/failure response entity
     */
    @DeleteMapping("/cache/**")
    public Mono<ResponseEntity<String>> purgeCacheKey(ServerHttpRequest request) {
        String fullPath = request.getPath().pathWithinApplication().value();
        // Extract the key suffix after "/cache/"
        if (fullPath.length() <= 7) {
            return Mono.just(ResponseEntity.badRequest().body("Invalid cache key specified"));
        }
        String rawKey = fullPath.substring(7); // Remove "/cache/" prefix
        
        // Re-attach query parameters if present in the delete request (for precise cache keys)
        String query = request.getURI().getRawQuery();
        String targetKey = (query != null && !query.isBlank()) ? rawKey + "?" + query : rawKey;

        return cacheService.delete(targetKey)
                .map(success -> success 
                        ? ResponseEntity.ok("Cache key deleted successfully: " + targetKey) 
                        : ResponseEntity.notFound().build());
    }
}
