package com.mentor.cachingproxy.service;

import com.mentor.cachingproxy.cache.CacheService;
import com.mentor.cachingproxy.config.ProxyProperties;
import com.mentor.cachingproxy.exception.BadGatewayException;
import com.mentor.cachingproxy.exception.GatewayTimeoutException;
import com.mentor.cachingproxy.metrics.CacheMetricsTracker;
import com.mentor.cachingproxy.model.CachedResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ProxyServiceTest {

    @Mock
    private WebClient webClient;

    @Mock
    @SuppressWarnings("rawtypes")
    private WebClient.RequestHeadersUriSpec requestHeadersUriSpec;

    @Mock
    @SuppressWarnings("rawtypes")
    private WebClient.RequestHeadersSpec requestHeadersSpec;

    @Mock
    private ProxyProperties proxyProperties;

    @Mock
    private CacheService cacheService;

    @Mock
    private CacheMetricsTracker metricsTracker;

    @Mock
    private ServerWebExchange exchange;

    @Mock
    private ServerHttpRequest request;

    private ProxyService proxyService;

    @BeforeEach
    void setUp() {
        proxyService = new ProxyService(webClient, proxyProperties, cacheService, metricsTracker);
    }

    @Test
    void forward_onCacheHit_shouldReturnCachedResponse() {
        // Arrange
        String cacheKey = "proxy:cache:GET:/get:";
        HttpHeaders cachedHeaders = new HttpHeaders();
        cachedHeaders.add("Content-Type", "application/json");
        CachedResponse cachedResponse = new CachedResponse(200, cachedHeaders, "cached-payload".getBytes());

        when(exchange.getRequest()).thenReturn(request);
        when(request.getHeaders()).thenReturn(new HttpHeaders());
        when(cacheService.generateKey(request)).thenReturn(cacheKey);
        when(cacheService.get(cacheKey)).thenReturn(Mono.just(cachedResponse));

        // Act
        Mono<ResponseEntity<Flux<DataBuffer>>> result = proxyService.forward(exchange);

        // Assert
        StepVerifier.create(result)
                .assertNext(responseEntity -> {
                    assertThat(responseEntity.getStatusCode().value()).isEqualTo(200);
                    assertThat(responseEntity.getHeaders().getFirst("X-Cache")).isEqualTo("HIT");
                    assertThat(responseEntity.getHeaders().getFirst("Content-Type")).isEqualTo("application/json");
                })
                .verifyComplete();

        verify(metricsTracker).incrementTotalRequests();
        verify(metricsTracker).incrementCacheHits();
    }

    @Test
    void forward_onCacheHitWithMatchingEtag_shouldReturn304NotModified() {
        // Arrange
        String cacheKey = "proxy:cache:GET:/get:";
        HttpHeaders cachedHeaders = new HttpHeaders();
        cachedHeaders.add("ETag", "\"etag-val\"");
        CachedResponse cachedResponse = new CachedResponse(200, cachedHeaders, "cached-payload".getBytes());

        HttpHeaders clientHeaders = new HttpHeaders();
        clientHeaders.add("If-None-Match", "\"etag-val\"");

        when(exchange.getRequest()).thenReturn(request);
        when(request.getHeaders()).thenReturn(clientHeaders);
        when(cacheService.generateKey(request)).thenReturn(cacheKey);
        when(cacheService.get(cacheKey)).thenReturn(Mono.just(cachedResponse));

        // Act
        Mono<ResponseEntity<Flux<DataBuffer>>> result = proxyService.forward(exchange);

        // Assert
        StepVerifier.create(result)
                .assertNext(responseEntity -> {
                    assertThat(responseEntity.getStatusCode().value()).isEqualTo(304);
                    assertThat(responseEntity.getHeaders().getFirst("X-Cache")).isEqualTo("HIT-304");
                })
                .verifyComplete();

        verify(metricsTracker).incrementTotalRequests();
        verify(metricsTracker).incrementCacheHits();
    }

    @Test
    void forward_onReadTimeout_shouldThrowGatewayTimeoutException() {
        // Arrange
        String cacheKey = "proxy:cache:GET:/delay/6:";
        when(exchange.getRequest()).thenReturn(request);
        org.springframework.http.server.RequestPath requestPath = mock(org.springframework.http.server.RequestPath.class);
        org.springframework.http.server.PathContainer pathContainer = mock(org.springframework.http.server.PathContainer.class);
        when(request.getPath()).thenReturn(requestPath);
        when(requestPath.pathWithinApplication()).thenReturn(pathContainer);
        when(pathContainer.value()).thenReturn("/delay/6");
        when(request.getURI()).thenReturn(URI.create("http://localhost:8080/delay/6"));
        when(request.getHeaders()).thenReturn(new HttpHeaders());
        
        when(cacheService.generateKey(request)).thenReturn(cacheKey);
        when(cacheService.get(cacheKey)).thenReturn(Mono.empty()); // Cache miss

        when(proxyProperties.getBackendUrl()).thenReturn("https://httpbin.org");
        when(exchange.getAttributes()).thenReturn(new HashMap<>());

        // Mock WebClient chain to throw WebClientRequestException wrapped ReadTimeoutException
        when(webClient.get()).thenReturn(requestHeadersUriSpec);
        when(requestHeadersUriSpec.uri(any(URI.class))).thenReturn(requestHeadersSpec);
        when(requestHeadersSpec.headers(any())).thenReturn(requestHeadersSpec);
        
        Throwable readTimeoutException = new io.netty.handler.timeout.ReadTimeoutException();
        WebClientRequestException clientRequestException = new WebClientRequestException(
                readTimeoutException, null, URI.create("https://httpbin.org/delay/6"), new HttpHeaders()
        );
        when(requestHeadersSpec.exchangeToMono(any())).thenReturn(Mono.error(clientRequestException));

        // Act
        Mono<ResponseEntity<Flux<DataBuffer>>> result = proxyService.forward(exchange);

        // Assert
        StepVerifier.create(result)
                .expectError(GatewayTimeoutException.class)
                .verify();

        verify(metricsTracker).incrementTotalRequests();
        verify(metricsTracker).incrementCacheMisses();
        verify(metricsTracker).incrementBackendCalls();
    }

    @Test
    void forward_onConnectException_shouldThrowBadGatewayException() {
        // Arrange
        String cacheKey = "proxy:cache:GET:/get:";
        when(exchange.getRequest()).thenReturn(request);
        org.springframework.http.server.RequestPath requestPath = mock(org.springframework.http.server.RequestPath.class);
        org.springframework.http.server.PathContainer pathContainer = mock(org.springframework.http.server.PathContainer.class);
        when(request.getPath()).thenReturn(requestPath);
        when(requestPath.pathWithinApplication()).thenReturn(pathContainer);
        when(pathContainer.value()).thenReturn("/get");
        when(request.getURI()).thenReturn(URI.create("http://localhost:8080/get"));
        when(request.getHeaders()).thenReturn(new HttpHeaders());
        
        when(cacheService.generateKey(request)).thenReturn(cacheKey);
        when(cacheService.get(cacheKey)).thenReturn(Mono.empty()); // Cache miss

        when(proxyProperties.getBackendUrl()).thenReturn("https://httpbin.org");
        when(exchange.getAttributes()).thenReturn(new HashMap<>());

        // Mock WebClient chain to throw WebClientRequestException wrapped ConnectException
        when(webClient.get()).thenReturn(requestHeadersUriSpec);
        when(requestHeadersUriSpec.uri(any(URI.class))).thenReturn(requestHeadersSpec);
        when(requestHeadersSpec.headers(any())).thenReturn(requestHeadersSpec);
        
        Throwable connectException = new java.net.ConnectException("Connection refused");
        WebClientRequestException clientRequestException = new WebClientRequestException(
                connectException, null, URI.create("https://httpbin.org/get"), new HttpHeaders()
        );
        when(requestHeadersSpec.exchangeToMono(any())).thenReturn(Mono.error(clientRequestException));

        // Act
        Mono<ResponseEntity<Flux<DataBuffer>>> result = proxyService.forward(exchange);

        // Assert
        StepVerifier.create(result)
                .expectError(BadGatewayException.class)
                .verify();

        verify(metricsTracker).incrementTotalRequests();
        verify(metricsTracker).incrementCacheMisses();
        verify(metricsTracker).incrementBackendCalls();
    }
}
