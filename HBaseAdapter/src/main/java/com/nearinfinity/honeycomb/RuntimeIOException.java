package com.nearinfinity.honeycomb;

public class RuntimeIOException extends RuntimeException {
    public RuntimeIOException(Throwable cause) {
        super(cause);
    }

    public RuntimeIOException(String message, Throwable cause) {
        super(message, cause);
    }
}
