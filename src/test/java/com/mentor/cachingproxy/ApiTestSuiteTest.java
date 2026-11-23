package com.mentor.cachingproxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mentor.cachingproxy.config.ProxyProperties;
import com.mentor.cachingproxy.metrics.CacheMetricsTracker;
import com.mentor.cachingproxy.metrics.CacheStatsResponse;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient(timeout = "10000")
public class ApiTestSuiteTest {

    private static MockWebServer mockWebServer;

    @Autowired
    private WebTestClient webTestClient;

    @Autowired
    private ProxyProperties proxyProperties;

    @MockBean
    private ReactiveRedisTemplate<String, byte[]> redisTemplate;

    @MockBean
    private ReactiveValueOperations<String, byte[]> valueOperations;

    // In-memory cache simulator backing the mocked Redis template
    private static final Map<String, byte[]> cacheStore = new ConcurrentHashMap<>();
    private static final Map<String, Duration> ttlStore = new ConcurrentHashMap<>();

    @BeforeAll
    static void startBackend() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
    }

    @AfterAll
    static void stopBackend() throws IOException {
        mockWebServer.shutdown();
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("proxy.backend-url", () -> mockWebServer.url("/").toString());
        registry.add("proxy.timeouts.connection-ms", () -> 2000);
        registry.add("proxy.timeouts.read-ms", () -> 2000);
    }

    @BeforeEach
    void setupRedisMocks() {
        cacheStore.clear();
        ttlStore.clear();

        when(redisTemplate.opsForValue()).thenReturn(valueOperations);

        // GET operation mock
        when(valueOperations.get(anyString())).thenAnswer(invocation -> {
            String key = invocation.getArgument(0);
            byte[] val = cacheStore.get(key);
            return val != null ? Mono.just(val) : Mono.empty();
        });

        // SET operation mock
        when(valueOperations.set(anyString(), any(byte[].class), any(Duration.class))).thenAnswer(invocation -> {
            String key = invocation.getArgument(0);
            byte[] val = invocation.getArgument(1);
            Duration ttl = invocation.getArgument(2);
            cacheStore.put(key, val);
            if (ttl != null) {
                ttlStore.put(key, ttl);
            }
            return Mono.just(true);
        });

        // DELETE key mock
        when(redisTemplate.delete(anyString())).thenAnswer(invocation -> {
            String key = invocation.getArgument(0);
            byte[] removed = cacheStore.remove(key);
            ttlStore.remove(key);
            return Mono.just(removed != null ? 1L : 0L);
        });

        // SCAN mock for purgeAll
        when(redisTemplate.scan(any(ScanOptions.class))).thenAnswer(invocation -> {
            ScanOptions options = invocation.getArgument(0);
            String pattern = options.getPattern();
            List<String> matching = new ArrayList<>();
            for (String k : cacheStore.keySet()) {
                if (pattern != null && pattern.contains("proxy:cache:*")) {
                    if (k.startsWith("proxy:cache:")) {
                        matching.add(k);
                    }
                }
            }
            return Flux.fromIterable(matching);
        });
    }

    // -------------------------------------------------------------------------
    // TEST CASE 01: Initial Request (Cache Miss)
    // -------------------------------------------------------------------------
    @Test
    void TC01_InitialRequest_CacheMiss() {
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody("{\"id\":1,\"title\":\"test post\"}"));

        webTestClient.get()
                .uri("/posts/1")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Cache", "MISS")
                .expectBody()
                .jsonPath("$.id").isEqualTo(1)
                .jsonPath("$.title").isEqualTo("test post");

        assertThat(cacheStore).containsKey("proxy:cache:GET:/posts/1");
    }

    // -------------------------------------------------------------------------
    // TEST CASE 02: Subsequent Request (Cache Hit)
    // -------------------------------------------------------------------------
    @Test
    void TC02_SubsequentRequest_CacheHit() {
        // Prime the cache with TC01
        TC01_InitialRequest_CacheMiss();

        // 2nd request should hit cache and NOT invoke MockWebServer
        webTestClient.get()
                .uri("/posts/1")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Cache", "HIT")
                .expectBody()
                .jsonPath("$.id").isEqualTo(1);
    }

    // -------------------------------------------------------------------------
    // TEST CASE 03: Query Parameter Sorting (Canonical Key)
    // -------------------------------------------------------------------------
    @Test
    void TC03_QueryParameterSorting_CanonicalKey() {
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody("{\"results\":[\"item1\"]}"));

        // Request with unordered query params: page=1&category=tech
        webTestClient.get()
                .uri("/items?page=1&category=tech")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Cache", "MISS");

        // Request with reordered query params: category=tech&page=1
        // Should produce identical cache key: proxy:cache:GET:/items:category=tech&page=1
        webTestClient.get()
                .uri("/items?category=tech&page=1")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Cache", "HIT");
    }

    // -------------------------------------------------------------------------
    // TEST CASE 04: Conditional Request - ETag Match (304 Not Modified)
    // -------------------------------------------------------------------------
    @Test
    void TC04_ConditionalRequest_ETagMatch_Returns304() {
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("ETag", "\"v123\"")
                .setHeader("Content-Type", "application/json")
                .setBody("{\"data\":\"value\"}"));

        // Cache entry
        webTestClient.get()
                .uri("/etag-test")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Cache", "MISS");

        // Conditional GET with matching If-None-Match header
        webTestClient.get()
                .uri("/etag-test")
                .header("If-None-Match", "\"v123\"")
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.NOT_MODIFIED)
                .expectHeader().valueEquals("X-Cache", "HIT-304")
                .expectBody().isEmpty();
    }

    // -------------------------------------------------------------------------
    // TEST CASE 05: Conditional Request - Last-Modified Match (304 Not Modified)
    // -------------------------------------------------------------------------
    @Test
    void TC05_ConditionalRequest_LastModifiedMatch_Returns304() {
        String lastModifiedDate = "Wed, 21 Oct 2015 07:28:00 GMT";

        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Last-Modified", lastModifiedDate)
                .setBody("{\"data\":\"modified\"}"));

        webTestClient.get()
                .uri("/last-mod-test")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Cache", "MISS");

        webTestClient.get()
                .uri("/last-mod-test")
                .header("If-Modified-Since", lastModifiedDate)
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.NOT_MODIFIED)
                .expectHeader().valueEquals("X-Cache", "HIT-304")
                .expectBody().isEmpty();
    }

    // -------------------------------------------------------------------------
    // TEST CASE 06: Respect Cache-Control max-age Directive & TTL
    // -------------------------------------------------------------------------
    @Test
    void TC06_CacheControl_MaxAge_SetsTTL() {
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Cache-Control", "max-age=60")
                .setBody("{\"ttl\":60}"));

        webTestClient.get()
                .uri("/ttl-max-age")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Cache", "MISS");

        String cacheKey = "proxy:cache:GET:/ttl-max-age";
        assertThat(ttlStore).containsKey(cacheKey);
        assertThat(ttlStore.get(cacheKey).getSeconds()).isEqualTo(60);
    }

    // -------------------------------------------------------------------------
    // TEST CASE 07: Respect Cache-Control s-maxage Directive over max-age
    // -------------------------------------------------------------------------
    @Test
    void TC07_CacheControl_SMaxAge_Over_MaxAge() {
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Cache-Control", "public, max-age=60, s-maxage=120")
                .setBody("{\"ttl\":120}"));

        webTestClient.get()
                .uri("/ttl-s-maxage")
                .exchange()
                .expectStatus().isOk();

        String cacheKey = "proxy:cache:GET:/ttl-s-maxage";
        assertThat(ttlStore).containsKey(cacheKey);
        assertThat(ttlStore.get(cacheKey).getSeconds()).isEqualTo(120);
    }

    // -------------------------------------------------------------------------
    // TEST CASE 08: Respect Cache-Control no-store / private (Bypass Cache)
    // -------------------------------------------------------------------------
    @Test
    void TC08_CacheControl_NoStore_BypassesCache() {
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Cache-Control", "no-store, private")
                .setBody("{\"secret\":\"data\"}"));

        webTestClient.get()
                .uri("/private-data")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Cache", "MISS");

        // Verify key was NOT stored in Redis
        assertThat(cacheStore).doesNotContainKey("proxy:cache:GET:/private-data");

        // 2nd request should also be a MISS
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Cache-Control", "no-store")
                .setBody("{\"secret\":\"data2\"}"));

        webTestClient.get()
                .uri("/private-data")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Cache", "MISS");
    }

    // -------------------------------------------------------------------------
    // TEST CASE 09: Cache Invalidation - Delete Specific Key
    // -------------------------------------------------------------------------
    @Test
    void TC09_CacheInvalidation_DeleteSingleKey() {
        // Prime cache
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody("hello"));
        webTestClient.get().uri("/purge-me").exchange().expectStatus().isOk();
        assertThat(cacheStore).containsKey("proxy:cache:GET:/purge-me");

        // Delete key
        webTestClient.delete()
                .uri("/cache/GET:/purge-me")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).value(res -> assertThat(res).contains("deleted successfully"));

        assertThat(cacheStore).doesNotContainKey("proxy:cache:GET:/purge-me");
    }

    // -------------------------------------------------------------------------
    // TEST CASE 10: Cache Invalidation - Delete Key With Query Parameters
    // -------------------------------------------------------------------------
    @Test
    void TC10_CacheInvalidation_DeleteKeyWithQueryParams() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody("query result"));

        // GET /search?b=2&a=1 creates key: proxy:cache:GET:/search:a=1&b=2
        webTestClient.get().uri("/search?b=2&a=1").exchange().expectStatus().isOk();
        assertThat(cacheStore).containsKey("proxy:cache:GET:/search:a=1&b=2");

        // Delete attempt via DELETE /cache/search?b=2&a=1
        webTestClient.delete()
                .uri("/cache/search?b=2&a=1")
                .exchange()
                .expectStatus().isOk();

        // Check if key was purged
        assertThat(cacheStore).doesNotContainKey("proxy:cache:GET:/search:a=1&b=2");
    }

    // -------------------------------------------------------------------------
    // TEST CASE 11: Cache Invalidation - Purge Entire Cache
    // -------------------------------------------------------------------------
    @Test
    void TC11_CacheInvalidation_PurgeAll() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody("1"));
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody("2"));

        webTestClient.get().uri("/page1").exchange();
        webTestClient.get().uri("/page2").exchange();
        assertThat(cacheStore.size()).isEqualTo(2);

        webTestClient.delete()
                .uri("/cache")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).value(res -> assertThat(res).contains("Cache fully purged successfully"));

        assertThat(cacheStore).isEmpty();
    }

    // -------------------------------------------------------------------------
    // TEST CASE 12: Metrics Endpoints (/metrics & /cache/stats)
    // -------------------------------------------------------------------------
    @Test
    void TC12_MetricsEndpoints_ReturnSnapshot() {
        webTestClient.get()
                .uri("/metrics")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.totalRequests").exists()
                .jsonPath("$.cacheHits").exists()
                .jsonPath("$.cacheMisses").exists()
                .jsonPath("$.backendCalls").exists()
                .jsonPath("$.cacheHitRatio").exists();

        webTestClient.get()
                .uri("/cache/stats")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.totalRequests").exists();
    }

    // -------------------------------------------------------------------------
    // TEST CASE 13: Error Handling - Backend Connection Refused (502 Bad Gateway)
    // -------------------------------------------------------------------------
    @Test
    void TC13_ErrorHandling_BadGateway_Returns502() {
        String originalUrl = proxyProperties.getBackendUrl();
        try {
            // Point to a non-existent port on localhost to trigger ConnectException
            proxyProperties.setBackendUrl("http://127.0.0.1:65530");

            webTestClient.get()
                    .uri("/offline-backend")
                    .exchange()
                    .expectStatus().isEqualTo(HttpStatus.BAD_GATEWAY)
                    .expectBody()
                    .jsonPath("$.status").isEqualTo(502)
                    .jsonPath("$.error").isEqualTo("Bad Gateway")
                    .jsonPath("$.message").value(msg -> assertThat(msg.toString()).contains("Failed to connect"));
        } finally {
            proxyProperties.setBackendUrl(originalUrl);
        }
    }

    // -------------------------------------------------------------------------
    // TEST CASE 14: Error Handling - Backend Timeout (504 Gateway Timeout)
    // -------------------------------------------------------------------------
    @Test
    void TC14_ErrorHandling_GatewayTimeout_Returns504() {
        mockWebServer.enqueue(new MockResponse()
                .setBody("slow payload")
                .setHeadersDelay(3, TimeUnit.SECONDS)); // Delay exceed 2s timeout

        webTestClient.get()
                .uri("/slow-backend")
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.GATEWAY_TIMEOUT)
                .expectBody()
                .jsonPath("$.status").isEqualTo(504)
                .jsonPath("$.error").isEqualTo("Gateway Timeout")
                .jsonPath("$.message").value(msg -> assertThat(msg.toString()).contains("timed out"));
    }

    // -------------------------------------------------------------------------
    // TEST CASE 15: Cache Stampede Protection (Concurrent Request Coalescing)
    // -------------------------------------------------------------------------
    @Test
    void TC15_CacheStampedeProtection_CoalescesConcurrentRequests() throws Exception {
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBodyDelay(500, TimeUnit.MILLISECONDS)
                .setBody("{\"stampede\":\"protected\"}"));

        int requestsBefore = mockWebServer.getRequestCount();
        int concurrency = 5;
        CountDownLatch latch = new CountDownLatch(concurrency);
        List<String> cacheHeaders = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < concurrency; i++) {
            new Thread(() -> {
                try {
                    EntityExchangeResult<byte[]> result = webTestClient.get()
                            .uri("/stampede-test")
                            .exchange()
                            .expectStatus().isOk()
                            .expectBody().returnResult();

                    String cacheHeader = result.getResponseHeaders().getFirst("X-Cache");
                    if (cacheHeader != null) {
                        cacheHeaders.add(cacheHeader);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        boolean finished = latch.await(5, TimeUnit.SECONDS);
        assertThat(finished).isTrue();
        assertThat(cacheHeaders.size()).isEqualTo(concurrency);

        long missCount = cacheHeaders.stream().filter("MISS"::equals).count();
        long coalescedCount = cacheHeaders.stream().filter("MISS-STAMPEDE"::equals).count();

        assertThat(missCount).isEqualTo(1);
        assertThat(coalescedCount).isEqualTo(concurrency - 1);
        assertThat(mockWebServer.getRequestCount() - requestsBefore).isEqualTo(1);
    }

    // -------------------------------------------------------------------------
    // TEST CASE 16: Hop-by-Hop Header Stripping
    // -------------------------------------------------------------------------
    @Test
    void TC16_HopByHopHeaderStripping() {
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Connection", "keep-alive")
                .setHeader("Proxy-Authenticate", "Basic")
                .setHeader("X-Custom-Header", "keep-me")
                .setBody("hop test"));

        webTestClient.get()
                .uri("/hop-test")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Custom-Header", "keep-me")
                .expectHeader().doesNotExist("Proxy-Authenticate");
    }

    // -------------------------------------------------------------------------
    // TEST CASE 17: Non-2xx Responses Are Not Cached
    // -------------------------------------------------------------------------
    @Test
    void TC17_Non2xxResponses_NotCached() {
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(404)
                .setBody("{\"error\":\"Not Found\"}"));

        webTestClient.get()
                .uri("/not-found")
                .exchange()
                .expectStatus().isNotFound();

        assertThat(cacheStore).doesNotContainKey("proxy:cache:GET:/not-found");
    }

    // -------------------------------------------------------------------------
    // TEST CASE 18: Redis Outage Resilience (Graceful Fallback)
    // -------------------------------------------------------------------------
    @Test
    void TC18_RedisOutage_GracefulFallback() {
        // Override opsForValue().get() to throw exception (simulating Redis down)
        when(valueOperations.get(anyString())).thenReturn(Mono.error(new RuntimeException("Redis connection error")));
        when(valueOperations.set(anyString(), any(), any())).thenReturn(Mono.error(new RuntimeException("Redis connection error")));

        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody("{\"fallback\":\"ok\"}"));

        // Proxy should bypass Redis failure cleanly and return 200 OK
        webTestClient.get()
                .uri("/redis-down")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.fallback").isEqualTo("ok");
    }

    @Test
    void TC20_AuthorizationHeader_BypassesCache() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody("private data"));
        webTestClient.get().uri("/secure/1").header("Authorization", "Bearer x").exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Cache", "BYPASS");
        assertThat(cacheStore).doesNotContainKey("proxy:cache:GET:/secure/1");
    }

    @Test
    void TC21_NoCacheAndMaxAgeZero_NotStored() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setHeader("Cache-Control", "no-cache").setBody("a"));
        webTestClient.get().uri("/nc").exchange().expectStatus().isOk();
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setHeader("Cache-Control", "max-age=0").setBody("b"));
        webTestClient.get().uri("/zero").exchange().expectStatus().isOk();
        assertThat(cacheStore).doesNotContainKeys("proxy:cache:GET:/nc", "proxy:cache:GET:/zero");
    }

    @Test
    void TC22_SetCookieResponse_NotStored() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setHeader("Set-Cookie", "s=1").setBody("c"));
        webTestClient.get().uri("/cookie").exchange().expectStatus().isOk();
        assertThat(cacheStore).doesNotContainKey("proxy:cache:GET:/cookie");
    }
}
