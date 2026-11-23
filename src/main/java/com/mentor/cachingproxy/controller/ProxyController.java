package com.mentor.cachingproxy.controller;

import com.mentor.cachingproxy.service.ProxyService;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Controller acting as the reverse proxy gateway endpoint.
 * Intercepts all GET requests and forwards them to the backend service.
 */
@RestController
public class ProxyController {

    private final ProxyService proxyService;

    /**
     * Constructor injection for ProxyService.
     *
     * @param proxyService the service used to forward requests
     */
    public ProxyController(ProxyService proxyService) {
        this.proxyService = proxyService;
    }

    /**
     * Intercepts all incoming GET requests and routes them to the backend server.
     *
     * @param exchange the current reactive request-response exchange
     * @return a Mono containing a ResponseEntity with streaming payload chunks
     */
    @GetMapping("/**")
    public Mono<ResponseEntity<Flux<DataBuffer>>> handleProxyGet(ServerWebExchange exchange) {
        return proxyService.forward(exchange);
    }
}
