package com.mentor.cachingproxy.config;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * Protects cache-purge endpoints (DELETE /cache, DELETE /cache/**) with a shared secret
 * supplied in the {@code X-Admin-Token} header. Disabled when {@code proxy.admin-token} is blank.
 */
@Component
@Order(-2)
public class AdminAuthFilter implements WebFilter {

    private final ProxyProperties properties;

    public AdminAuthFilter(ProxyProperties properties) {
        this.properties = properties;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String token = properties.getAdminToken();
        String path = exchange.getRequest().getPath().pathWithinApplication().value();
        boolean admin = HttpMethod.DELETE.equals(exchange.getRequest().getMethod())
                && (path.equals("/cache") || path.startsWith("/cache/"));
        if (token == null || token.isBlank() || !admin) {
            return chain.filter(exchange);
        }
        String supplied = exchange.getRequest().getHeaders().getFirst("X-Admin-Token");
        if (supplied != null && MessageDigest.isEqual(
                supplied.getBytes(StandardCharsets.UTF_8), token.getBytes(StandardCharsets.UTF_8))) {
            return chain.filter(exchange);
        }
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
        DataBuffer body = exchange.getResponse().bufferFactory()
                .wrap("{\"status\":401,\"error\":\"Unauthorized\",\"message\":\"Missing or invalid X-Admin-Token\"}"
                        .getBytes(StandardCharsets.UTF_8));
        return exchange.getResponse().writeWith(Mono.just(body));
    }
}
