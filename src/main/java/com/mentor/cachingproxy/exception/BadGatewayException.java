package com.mentor.cachingproxy.exception;

/**
 * Exception representing HTTP 502 Bad Gateway error.
 */
public class BadGatewayException extends RuntimeException {
    
    /**
     * Constructs a new BadGatewayException with a message.
     *
     * @param message the error message
     */
    public BadGatewayException(String message) {
        super(message);
    }

    /**
     * Constructs a new BadGatewayException with a message and cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     */
    public BadGatewayException(String message, Throwable cause) {
        super(message, cause);
    }
}
