package com.nearinfinity.honeycomb.hbaseclient;

public final class KeyValue {
    private final String key;
    private final byte[] value;
    private final boolean nullable;
    private final boolean isNull;

    public KeyValue(String key, byte[] value, boolean nullable, boolean isNull) {
        this.key = key;
        this.value = value;
        this.nullable = nullable;
        this.isNull = isNull;
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

    public final boolean isNull() {
        return this.isNull;
    }
}
