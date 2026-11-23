package com.mentor.cachingproxy.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Configuration class for Redis caching.
 * Exposes a ReactiveRedisTemplate configured with String keys and byte array values.
 */
@Configuration
public class RedisConfig {

    /**
     * Creates a ReactiveRedisTemplate configured for String keys and byte[] values.
     * Storing responses as byte arrays ensures we can cache any media type (JSON, images, HTML)
     * without serialization issues or overhead.
     *
     * @param factory the reactive connection factory provided by Spring Boot
     * @return the configured ReactiveRedisTemplate
     */
    @Bean
    public ReactiveRedisTemplate<String, byte[]> reactiveRedisTemplate(ReactiveRedisConnectionFactory factory) {
        RedisSerializationContext<String, byte[]> serializationContext = RedisSerializationContext
                .<String, byte[]>newSerializationContext(new StringRedisSerializer())
                .value(RedisSerializer.byteArray())
                .hashKey(new StringRedisSerializer())
                .hashValue(RedisSerializer.byteArray())
                .build();

        return new ReactiveRedisTemplate<>(factory, serializationContext);
    }
}
