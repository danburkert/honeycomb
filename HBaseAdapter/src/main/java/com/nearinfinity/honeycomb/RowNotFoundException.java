package com.nearinfinity.honeycomb;

import java.util.UUID;

public class RowNotFoundException extends Exception {
    private UUID uuid;

    public RowNotFoundException(UUID uuid) {
        this.uuid = uuid;
    };
}
