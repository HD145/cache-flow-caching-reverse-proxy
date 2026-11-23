package com.mentor.cachingproxy.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

/**
 * Configuration class for creating and configuring the WebClient bean.
 * Configures connection pooling, connection limits, and connection timeouts.
 */
@Configuration
public class WebClientConfig {

    private final ProxyProperties proxyProperties;

    /**
     * Constructor injection for proxy properties.
     *
     * @param proxyProperties the proxy properties
     */
    public WebClientConfig(ProxyProperties proxyProperties) {
        this.proxyProperties = proxyProperties;
    }

    /**
     * Configures the WebClient bean to forward requests to the target backend.
     * Utilizes connection pooling and custom timeouts.
     *
     * @return the configured WebClient instance
     */
    @Bean
    public WebClient proxyWebClient() {
        // Create a custom connection pool
        ConnectionProvider connectionProvider = ConnectionProvider.builder("proxy-connection-pool")
                .maxConnections(proxyProperties.getPool().getMaxConnections())
                // Max idle time before connection is closed
                .maxIdleTime(Duration.ofSeconds(20))
                // Max life time of a connection in the pool
                .maxLifeTime(Duration.ofMinutes(5))
                // Pending acquire timeout (wait time for a connection from the pool)
                .pendingAcquireTimeout(Duration.ofSeconds(10))
                .build();

        // Configure Reactor Netty HttpClient
        HttpClient httpClient = HttpClient.create(connectionProvider)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, proxyProperties.getTimeouts().getConnectionMs())
                .doOnConnected(connection -> {
                    connection.addHandlerLast(new ReadTimeoutHandler(proxyProperties.getTimeouts().getReadMs(), TimeUnit.MILLISECONDS));
                    connection.addHandlerLast(new WriteTimeoutHandler(proxyProperties.getTimeouts().getReadMs(), TimeUnit.MILLISECONDS));
                });

        return WebClient.builder()
                .baseUrl(proxyProperties.getBackendUrl())
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                // We do not follow redirects automatically; the proxy should forward the 3xx redirect to the client
                .build();
    }
}
