package com.nearinfinity.honeycomb.mysql;

import net.java.quickcheck.Generator;

import java.util.UUID;

public class UUIDGenerator implements Generator<UUID> {
    @Override
    public UUID next() {
        return UUID.randomUUID();
    }
}