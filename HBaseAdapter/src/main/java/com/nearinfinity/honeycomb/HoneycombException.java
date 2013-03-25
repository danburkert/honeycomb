package com.nearinfinity.honeycomb;

public class HoneycombException extends RuntimeException {
    public HoneycombException() {
        super();
    }

    public HoneycombException(String message) {
        super(message);
    }

    public HoneycombException(String message, Throwable cause) {
        super(message, cause);
    }

    public HoneycombException(Throwable cause) {
        super(cause);
    }
}
