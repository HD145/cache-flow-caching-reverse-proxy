package com.mentor.cachingproxy.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mentor.cachingproxy.model.CachedResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CacheServiceTest {

    @Mock
    private ReactiveRedisTemplate<String, byte[]> redisTemplate;

    @Mock
    private ReactiveValueOperations<String, byte[]> valueOperations;

    @Mock
    private ServerHttpRequest serverHttpRequest;

    private CacheService cacheService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        cacheService = new CacheService(redisTemplate, objectMapper);
    }

    @Test
    void generateKey_shouldSortQueryParametersAlphabetically() {
        // Arrange
        org.springframework.http.server.RequestPath requestPath = mock(org.springframework.http.server.RequestPath.class);
        org.springframework.http.server.PathContainer pathContainer = mock(org.springframework.http.server.PathContainer.class);
        when(serverHttpRequest.getPath()).thenReturn(requestPath);
        when(requestPath.pathWithinApplication()).thenReturn(pathContainer);
        when(pathContainer.value()).thenReturn("/get");
        when(serverHttpRequest.getURI()).thenReturn(URI.create("http://localhost:8080/get?param2=val2&param1=val1"));

        // Act
        String key1 = cacheService.generateKey(serverHttpRequest);

        // Arrange matching request with different parameter ordering
        ServerHttpRequest request2 = mock(ServerHttpRequest.class);
        org.springframework.http.server.RequestPath requestPath2 = mock(org.springframework.http.server.RequestPath.class);
        org.springframework.http.server.PathContainer pathContainer2 = mock(org.springframework.http.server.PathContainer.class);
        when(request2.getPath()).thenReturn(requestPath2);
        when(requestPath2.pathWithinApplication()).thenReturn(pathContainer2);
        when(pathContainer2.value()).thenReturn("/get");
        when(request2.getURI()).thenReturn(URI.create("http://localhost:8080/get?param1=val1&param2=val2"));

        // Act
        String key2 = cacheService.generateKey(request2);

        // Assert
        assertThat(key1).isEqualTo(key2);
        assertThat(key1).isEqualTo("proxy:cache:GET:/get:param1=val1&param2=val2");
    }

    @Test
    void generateKey_withEmptyParams_shouldOmitQuerySuffix() {
        // Arrange
        org.springframework.http.server.RequestPath requestPath = mock(org.springframework.http.server.RequestPath.class);
        org.springframework.http.server.PathContainer pathContainer = mock(org.springframework.http.server.PathContainer.class);
        when(serverHttpRequest.getPath()).thenReturn(requestPath);
        when(requestPath.pathWithinApplication()).thenReturn(pathContainer);
        when(pathContainer.value()).thenReturn("/get");
        when(serverHttpRequest.getURI()).thenReturn(URI.create("http://localhost:8080/get"));

        // Act
        String key = cacheService.generateKey(serverHttpRequest);

        // Assert
        assertThat(key).isEqualTo("proxy:cache:GET:/get");
    }

    @Test
    void get_whenRedisHasKey_shouldReturnCachedResponse() throws Exception {
        // Arrange
        String key = "proxy:cache:GET:/get:";
        CachedResponse cachedResponse = new CachedResponse(200, new HttpHeaders(), "test-body".getBytes());
        byte[] serialized = objectMapper.writeValueAsBytes(cachedResponse);

        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(key)).thenReturn(Mono.just(serialized));

        // Act & Assert
        StepVerifier.create(cacheService.get(key))
                .assertNext(response -> {
                    assertThat(response.getStatusCode()).isEqualTo(200);
                    assertThat(new String(response.getBody())).isEqualTo("test-body");
                })
                .verifyComplete();
    }

    @Test
    void get_whenRedisThrowsException_shouldReturnEmptyFallback() {
        // Arrange
        String key = "proxy:cache:GET:/get:";
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(key)).thenReturn(Mono.error(new RuntimeException("Redis connection error")));

        // Act & Assert
        StepVerifier.create(cacheService.get(key))
                .verifyComplete(); // onErrorResume intercepts and completes empty
    }

    @Test
    void put_whenRedisThrowsException_shouldReturnFalseFallback() {
        // Arrange
        String key = "proxy:cache:GET:/get:";
        CachedResponse cachedResponse = new CachedResponse(200, new HttpHeaders(), "test-body".getBytes());
        
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.set(anyString(), any(byte[].class), any(Duration.class)))
                .thenReturn(Mono.error(new RuntimeException("Redis write timeout")));

        // Act & Assert
        StepVerifier.create(cacheService.put(key, cachedResponse, Duration.ofMinutes(5)))
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void delete_shouldResolveCorrectKeyPrefixes() {
        // Arrange
        when(redisTemplate.delete(anyString())).thenReturn(Mono.just(1L));

        // Act & Assert: Full Key
        StepVerifier.create(cacheService.delete("proxy:cache:GET:/get"))
                .expectNext(true)
                .verifyComplete();

        // Act & Assert: Helper ETag key prefix
        StepVerifier.create(cacheService.delete("GET:/get"))
                .expectNext(true)
                .verifyComplete();

        // Act & Assert: Relative path prefixing
        StepVerifier.create(cacheService.delete("get"))
                .expectNext(true)
                .verifyComplete();

        verify(redisTemplate, times(3)).delete("proxy:cache:GET:/get");
    }
}
