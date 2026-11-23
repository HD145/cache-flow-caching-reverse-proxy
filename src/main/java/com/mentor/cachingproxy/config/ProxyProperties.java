package com.mentor.cachingproxy.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.validator.constraints.URL;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration properties for the HTTP Caching Reverse Proxy.
 * Maps settings prefix "proxy" from configuration files.
 */
@Getter
@Setter
@Validated
@Configuration
@ConfigurationProperties(prefix = "proxy")
public class ProxyProperties {

    /**
     * The target backend URL where proxy requests will be forwarded.
     */
    @NotBlank(message = "Backend URL must not be blank")
    @URL(message = "Backend URL must be a valid HTTP/HTTPS URL")
    private String backendUrl;

    /**
     * Maximum backend response body size (bytes) the proxy will buffer and cache.
     */
    @Min(value = 1, message = "Max body size must be positive")
    private int maxBodyBytes = 10 * 1024 * 1024;

    /**
     * Optional shared secret protecting cache purge endpoints (header X-Admin-Token).
     * When blank, purge endpoints are open (local development only).
     */
    private String adminToken = "";

    /**
     * Timeout configurations for connections to the backend.
     */
    private Timeouts timeouts = new Timeouts();

    /**
     * Connection pooling configurations for the proxy.
     */
    private Pool pool = new Pool();

    /**
     * Timeout configurations nested class.
     */
    @Getter
    @Setter
    public static class Timeouts {
        /**
         * Connection timeout in milliseconds.
         */
        @Min(value = 1, message = "Connection timeout must be positive")
        private int connectionMs = 5000;

        /**
         * Read timeout in milliseconds.
         */
        @Min(value = 1, message = "Read timeout must be positive")
        private int readMs = 5000;
    }

    /**
     * Pool configurations nested class.
     */
    @Getter
    @Setter
    public static class Pool {
        /**
         * Maximum number of concurrent connections in the WebClient HTTP connection pool.
         */
        @Min(value = 1, message = "Max connections must be at least 1")
        private int maxConnections = 500;
    }
}
