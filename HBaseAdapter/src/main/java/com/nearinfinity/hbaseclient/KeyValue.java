package com.nearinfinity.hbaseclient;

public final class KeyValue {
    private final String key;
    private final byte[] value;
    private final boolean nullable;

    public KeyValue(String key, byte[] value, boolean nullable) {
        this.key = key;
        this.value = value;
        this.nullable = nullable;
    }

    public final String key() {
        return key;
    }

    public final byte[] value() {
        return value;
    }

    public final boolean nullable() {
        return nullable;
    }
}
