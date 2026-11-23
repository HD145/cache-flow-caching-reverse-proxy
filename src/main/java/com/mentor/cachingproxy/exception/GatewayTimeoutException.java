package com.mentor.cachingproxy.exception;

/**
 * Exception representing HTTP 504 Gateway Timeout error.
 */
public class GatewayTimeoutException extends RuntimeException {
    
    /**
     * Constructs a new GatewayTimeoutException with a message.
     *
     * @param message the error message
     */
    public GatewayTimeoutException(String message) {
        super(message);
    }

    /**
     * Constructs a new GatewayTimeoutException with a message and cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     */
    public GatewayTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
