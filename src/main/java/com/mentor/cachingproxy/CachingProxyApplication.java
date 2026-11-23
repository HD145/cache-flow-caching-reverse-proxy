package com.mentor.cachingproxy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Main application entry point for the HTTP Caching Reverse Proxy.
 */
@SpringBootApplication
public class CachingProxyApplication {

    /**
     * Entry point to launch the Spring Boot application.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(CachingProxyApplication.class, args);
    }
}
