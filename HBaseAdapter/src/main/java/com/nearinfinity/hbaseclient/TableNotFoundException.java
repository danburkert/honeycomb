package com.nearinfinity.hbaseclient;

public class TableNotFoundException extends RuntimeException {
    private final String message;

    public TableNotFoundException(String message) {
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return message;
    }
}
