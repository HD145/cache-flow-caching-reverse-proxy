package com.mentor.cachingproxy.config;

import java.net.URI;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/** Logs which Redis endpoint is configured (host/port/scheme only, never credentials). */
@Slf4j
@Component
public class RedisStartupLogger {

    private final String redisUrl;

    public RedisStartupLogger(@Value("${spring.data.redis.url:}") String redisUrl) {
        this.redisUrl = redisUrl;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void logRedisTarget() {
        try {
            URI uri = URI.create(redisUrl.trim());
            log.info("Redis configured: scheme={} host={} port={}", uri.getScheme(), uri.getHost(), uri.getPort());
        } catch (Exception e) {
            log.warn("Redis URL is not a valid URI (length={}). Expected redis:// or rediss://user:pass@host:port", redisUrl.length());
        }
    }
}
