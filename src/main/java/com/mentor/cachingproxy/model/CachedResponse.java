package com.mentor.cachingproxy.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Wrapper class representing a cached HTTP response.
 * Stores status, response headers, and the raw binary body payload.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CachedResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The HTTP status code of the backend response (e.g. 200, 404).
     */
    private int statusCode;

    /**
     * The preserved headers of the response.
     */
    private Map<String, List<String>> headers;

    /**
     * The raw body payload of the response.
     */
    private byte[] body;
}
