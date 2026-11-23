package com.mentor.cachingproxy.service;

import com.mentor.cachingproxy.cache.CacheService;
import com.mentor.cachingproxy.config.ProxyProperties;
import com.mentor.cachingproxy.metrics.CacheMetricsTracker;
import com.mentor.cachingproxy.model.CachedResponse;
import java.net.URI;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Service handling the outbound forwarding of client requests to the target backend
 * and coordinating cache storage/retrieval.
 */
@Slf4j
@Service
public class ProxyService {

    // Hop-by-hop headers that should not be forwarded downstream
    private static final Set<String> HOP_BY_HOP_HEADERS = Set.of(
            "connection",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailer",
            "transfer-encoding",
            "upgrade"
    );

    private final WebClient webClient;
    private final ProxyProperties proxyProperties;
    private final CacheService cacheService;
    private final CacheMetricsTracker metricsTracker;
    private final ConcurrentMap<String, Mono<CachedResponse>> inflightFetches = new ConcurrentHashMap<>();

    /**
     * Constructor injection for required configurations and services.
     *
     * @param webClient the configured proxy WebClient
     * @param proxyProperties the proxy configurations
     * @param cacheService the Redis cache service
     * @param metricsTracker the metrics tracker
     */
    public ProxyService(WebClient webClient, ProxyProperties proxyProperties, CacheService cacheService, CacheMetricsTracker metricsTracker) {
        this.webClient = webClient;
        this.proxyProperties = proxyProperties;
        this.cacheService = cacheService;
        this.metricsTracker = metricsTracker;
    }

    /**
     * Forwards an incoming HTTP GET request to the target backend.
     * Checks the cache first; if a hit occurs, evaluates client conditional validation headers
     * (If-None-Match, If-Modified-Since) to return 304 Not Modified, or returns the cached response.
     * On a miss, retrieves the payload from the backend, caches it in Redis, and returns it.
     *
     * @param exchange the current reactive request-response exchange
     * @return a Mono containing the ResponseEntity with streaming data buffers
     */
    public Mono<ResponseEntity<Flux<DataBuffer>>> forward(ServerWebExchange exchange) {
        metricsTracker.incrementTotalRequests();
        ServerHttpRequest request = exchange.getRequest();
        String cacheKey = cacheService.generateKey(request);

        // Credentialed requests must never be served from / written to the shared cache
        if (request.getHeaders().containsKey(HttpHeaders.AUTHORIZATION)
                || request.getHeaders().containsKey(HttpHeaders.COOKIE)) {
            log.info("Credentialed request for key: {}. Bypassing cache.", cacheKey);
            return fetchFromBackendAndCache(exchange, cacheKey, false).map(cr -> {
                HttpHeaders headers = new HttpHeaders();
                cr.getHeaders().forEach(headers::put);
                headers.set("X-Cache", "BYPASS");
                return ResponseEntity.status(cr.getStatusCode()).headers(headers)
                        .body(Flux.just(DefaultDataBufferFactory.sharedInstance.wrap(cr.getBody())));
            });
        }

        return cacheService.get(cacheKey)
                .flatMap(cachedResponse -> {
                    metricsTracker.incrementCacheHits();
                    log.info("Cache HIT for key: {}", cacheKey);

                    // Evaluate client conditional headers (If-None-Match, If-Modified-Since)
                    if (validateClientHeaders(request, cachedResponse)) {
                        log.info("Conditional request match for key: {}. Returning 304 Not Modified.", cacheKey);
                        HttpHeaders headers = new HttpHeaders();
                        cachedResponse.getHeaders().forEach(headers::put);
                        
                        // Add cache indicator header
                        headers.set("X-Cache", "HIT-304");

                        ResponseEntity<Flux<DataBuffer>> notModified = ResponseEntity
                                .status(304)
                                .headers(headers)
                                .body(Flux.empty()); // Empty body for 304

                        return Mono.just(notModified);
                    }

                    HttpHeaders headers = new HttpHeaders();
                    cachedResponse.getHeaders().forEach(headers::put);
                    headers.set("X-Cache", "HIT");

                    DataBuffer dataBuffer = DefaultDataBufferFactory.sharedInstance.wrap(cachedResponse.getBody());
                    ResponseEntity<Flux<DataBuffer>> responseEntity = ResponseEntity
                            .status(cachedResponse.getStatusCode())
                            .headers(headers)
                            .body(Flux.just(dataBuffer));

                    return Mono.just(responseEntity);
                })
                .switchIfEmpty(Mono.defer(() -> {
                    // Cache MISS: Check if there is an active concurrent fetch in progress
                    final boolean[] isInitiator = {false};
                    Mono<CachedResponse> sharedFetch = inflightFetches.computeIfAbsent(cacheKey, key -> {
                        isInitiator[0] = true;
                        // Start the actual fetch, make it a hot cache Mono, and clean up upon completion
                        // Clean up once the fetch itself finishes (not per-subscriber), then share it
                        return fetchFromBackendAndCache(exchange, key, true)
                                .doFinally(signalType -> inflightFetches.remove(key))
                                .cache();
                    });

                    return sharedFetch.map(cachedResponse -> {
                        HttpHeaders headers = new HttpHeaders();
                        cachedResponse.getHeaders().forEach(headers::put);
                        
                        if (isInitiator[0]) {
                            headers.set("X-Cache", "MISS");
                        } else {
                            log.info("Coalesced concurrent request (Cache Stampede Protection) for key: {}", cacheKey);
                            headers.set("X-Cache", "MISS-STAMPEDE");
                        }

                        DataBuffer buffer = DefaultDataBufferFactory.sharedInstance.wrap(cachedResponse.getBody());
                        return ResponseEntity.status(cachedResponse.getStatusCode())
                                .headers(headers)
                                .body(Flux.just(buffer));
                    });
                }));
    }

    /**
     * Fetches the response from the backend and stores it in Redis if cacheable.
     * Returns a Mono containing the CachedResponse.
     */
    private Mono<CachedResponse> fetchFromBackendAndCache(ServerWebExchange exchange, String cacheKey, boolean storeInCache) {
        if (storeInCache) {
            metricsTracker.incrementCacheMisses();
        }
        metricsTracker.incrementBackendCalls();
        log.info("Cache MISS for key: {}. Fetching from backend...", cacheKey);

        ServerHttpRequest request = exchange.getRequest();
        URI backendUri = UriComponentsBuilder.fromUriString(proxyProperties.getBackendUrl())
                .path(request.getPath().pathWithinApplication().value())
                .query(request.getURI().getRawQuery())
                .build(true)
                .toUri();

        HttpHeaders outboundHeaders = new HttpHeaders();
        request.getHeaders().forEach((name, values) -> {
            // Strip Host, Connection, and conditional headers.
            // Stripping conditional headers ensures the backend returns 200 OK with the full body
            // on cache miss, allowing our proxy to cache the complete resource.
            if (!name.equalsIgnoreCase("host") 
                    && !name.equalsIgnoreCase("connection")
                    && !name.equalsIgnoreCase(HttpHeaders.ACCEPT_ENCODING) // cache stores one (identity) representation
                    && !name.equalsIgnoreCase(HttpHeaders.IF_NONE_MATCH)
                    && !name.equalsIgnoreCase(HttpHeaders.IF_MODIFIED_SINCE)) {
                outboundHeaders.put(name, values);
            }
        });

        long backendStart = System.currentTimeMillis();

        return webClient.get()
                .uri(backendUri)
                .headers(headers -> headers.addAll(outboundHeaders))
                .exchangeToMono(clientResponse -> {
                    HttpHeaders incomingHeaders = clientResponse.headers().asHttpHeaders();
                    HttpHeaders responseHeaders = new HttpHeaders();
                    incomingHeaders.forEach((name, values) -> {
                        if (!HOP_BY_HOP_HEADERS.contains(name.toLowerCase())) {
                            responseHeaders.put(name, values);
                        }
                    });

                    // Join the response data buffer stream
                    return DataBufferUtils.join(clientResponse.bodyToFlux(DataBuffer.class), proxyProperties.getMaxBodyBytes())
                            .flatMap(dataBuffer -> {
                                byte[] bodyBytes = new byte[dataBuffer.readableByteCount()];
                                dataBuffer.read(bodyBytes);
                                DataBufferUtils.release(dataBuffer); // Release the joined Netty buffer

                                CachedResponse cachedResponse = new CachedResponse(
                                        clientResponse.statusCode().value(),
                                        responseHeaders,
                                        bodyBytes
                                );

                                // Only cache successful 200 OK responses that meet cacheability checks
                                if (storeInCache && clientResponse.statusCode().value() == 200 && isCacheable(responseHeaders)) {
                                    Duration ttl = calculateTtl(responseHeaders);
                                    if (ttl.isZero()) {
                                        return Mono.just(cachedResponse);
                                    }
                                    log.info("Caching response for key: {} with TTL: {}s", cacheKey, ttl.getSeconds());
                                    return cacheService.put(cacheKey, cachedResponse, ttl)
                                            .thenReturn(cachedResponse);
                                }

                                if (!clientResponse.statusCode().is2xxSuccessful()) {
                                    log.info("Response status is not successful ({}). Bypassing cache writing.", clientResponse.statusCode().value());
                                } else {
                                    log.info("Response headers contain no-store/private directives. Bypassing cache writing.");
                                }

                                return Mono.just(cachedResponse);
                            })
                            // If response body is empty, DataBufferUtils.join emits empty. Handle this case cleanly.
                            .switchIfEmpty(Mono.defer(() -> {
                                byte[] emptyBytes = new byte[0];
                                CachedResponse cachedResponse = new CachedResponse(
                                        clientResponse.statusCode().value(),
                                        responseHeaders,
                                        emptyBytes
                                );
                                if (storeInCache && clientResponse.statusCode().value() == 200 && isCacheable(responseHeaders)) {
                                    Duration ttl = calculateTtl(responseHeaders);
                                    log.info("Caching empty response for key: {} with TTL: {}s", cacheKey, ttl.getSeconds());
                                    return cacheService.put(cacheKey, cachedResponse, ttl)
                                            .thenReturn(cachedResponse);
                                }
                                return Mono.just(cachedResponse);
                            }));
                })
                .onErrorMap(e -> {
                    Throwable rootCause = e.getCause() != null ? e.getCause() : e;
                    log.error("Backend request failed for key: {}. Error type: {}, Root cause: {}, Message: {}", 
                            cacheKey, e.getClass().getSimpleName(), rootCause.getClass().getSimpleName(), e.getMessage());
                    if (rootCause instanceof java.net.ConnectException 
                            || rootCause instanceof io.netty.channel.ConnectTimeoutException) {
                        return new com.mentor.cachingproxy.exception.BadGatewayException("Failed to connect to backend server", e);
                    } else if (rootCause instanceof io.netty.handler.timeout.ReadTimeoutException 
                            || rootCause instanceof java.util.concurrent.TimeoutException) {
                        return new com.mentor.cachingproxy.exception.GatewayTimeoutException("Backend server timed out responding to request", e);
                    }
                    return new com.mentor.cachingproxy.exception.BadGatewayException("An unexpected error occurred while communicating with the backend", e);
                })
                .doFinally(signalType -> {
                    long latency = System.currentTimeMillis() - backendStart;
                    exchange.getAttributes().put("logging.backend-latency-ms", latency);
                    if (signalType == reactor.core.publisher.SignalType.CANCEL) {
                        log.info("Client cancelled request (early disconnect) for key: {}", cacheKey);
                    }
                });
    }

    /**
     * Determines whether a response is cacheable based on Cache-Control directives.
     */
    private boolean isCacheable(HttpHeaders headers) {
        if (headers.containsKey(HttpHeaders.SET_COOKIE) || "*".equals(headers.getFirst(HttpHeaders.VARY))) {
            return false;
        }
        String cacheControl = headers.getFirst(HttpHeaders.CACHE_CONTROL);
        if (cacheControl != null) {
            String lowercase = cacheControl.toLowerCase();
            return !lowercase.contains("no-store") && !lowercase.contains("private") && !lowercase.contains("no-cache");
        }
        return true; // Default cacheable if no cache-control header is present
    }

    /**
     * Computes the TTL for storing a cached entry, parsing s-maxage, max-age, or Expires fallbacks.
     */
    private Duration calculateTtl(HttpHeaders headers) {
        String cacheControl = headers.getFirst(HttpHeaders.CACHE_CONTROL);
        if (cacheControl != null) {
            // Check s-maxage first (CDNs and public shared proxies)
            Integer sMaxAge = parseDirective(cacheControl, "s-maxage");
            if (sMaxAge != null) {
                return Duration.ofSeconds(Math.max(0, sMaxAge));
            }
            // Check max-age next
            Integer maxAge = parseDirective(cacheControl, "max-age");
            if (maxAge != null) {
                return Duration.ofSeconds(Math.max(0, maxAge));
            }
        }

        // Check Expires fallback
        String expires = headers.getFirst(HttpHeaders.EXPIRES);
        if (expires != null) {
            try {
                java.time.ZonedDateTime expiresTime = java.time.ZonedDateTime.parse(expires, java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME);
                java.time.ZonedDateTime now = java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC);
                long seconds = java.time.Duration.between(now, expiresTime).getSeconds();
                if (seconds > 0) {
                    return Duration.ofSeconds(seconds);
                }
            } catch (Exception e) {
                log.debug("Failed to parse Expires header: {}", expires);
            }
        }

        return Duration.ofMinutes(5); // Default fallback TTL
    }

    /**
     * Parses a specific numeric directive value from the Cache-Control string.
     */
    private Integer parseDirective(String cacheControl, String directive) {
        String[] parts = cacheControl.split(",");
        for (String part : parts) {
            part = part.trim().toLowerCase();
            if (part.startsWith(directive + "=")) {
                try {
                    return Integer.parseInt(part.substring(directive.length() + 1));
                } catch (NumberFormatException e) {
                    // Ignore malformed directives
                }
            }
        }
        return null;
    }

    /**
     * Validates client request headers against cached responses to support 304 Not Modified.
     */
    private boolean validateClientHeaders(ServerHttpRequest request, CachedResponse cachedResponse) {
        // 1. ETag / If-None-Match Validation
        String ifNoneMatch = request.getHeaders().getFirst(HttpHeaders.IF_NONE_MATCH);
        if (ifNoneMatch != null && cachedResponse.getHeaders().containsKey(HttpHeaders.ETAG)) {
            String cachedEtag = cachedResponse.getHeaders().get(HttpHeaders.ETAG).get(0);
            if (ifNoneMatch.trim().equals(cachedEtag.trim()) 
                    || ifNoneMatch.trim().replace("W/", "").equals(cachedEtag.trim().replace("W/", ""))) {
                return true;
            }
        }

        // 2. Last-Modified / If-Modified-Since Validation
        String ifModifiedSince = request.getHeaders().getFirst(HttpHeaders.IF_MODIFIED_SINCE);
        if (ifModifiedSince != null && cachedResponse.getHeaders().containsKey(HttpHeaders.LAST_MODIFIED)) {
            String cachedLastModified = cachedResponse.getHeaders().get(HttpHeaders.LAST_MODIFIED).get(0);
            if (ifModifiedSince.trim().equalsIgnoreCase(cachedLastModified.trim())) {
                return true;
            }
        }

        return false;
    }
}
