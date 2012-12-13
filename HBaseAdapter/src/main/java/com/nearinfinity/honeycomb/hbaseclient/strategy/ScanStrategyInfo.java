package com.nearinfinity.honeycomb.hbaseclient.strategy;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.hbaseclient.KeyValue;

import java.util.*;

public final class ScanStrategyInfo {
    private final String tableName;
    private final Iterable<String> columnNames;
    private final List<KeyValue> keyValues;
    private final Map<String, byte[]> keyValueMap;

    public ScanStrategyInfo(final String tableName, final Iterable<String> columnNames, List<KeyValue> keyValues) {
        if (keyValues == null) {
            keyValues = new LinkedList<KeyValue>();
        }

        this.tableName = tableName;
        this.columnNames = columnNames;
        this.keyValues = keyValues;
        keyValueMap = new HashMap<String, byte[]>();
        for (KeyValue keyValue : keyValues) {
            keyValueMap.put(keyValue.key(), keyValue.value());
        }
    }

    public final String tableName() {
        return tableName;
    }

    public final Iterable<String> columnNames() {
        return columnNames;
    }

    public final List<KeyValue> keyValues() {
        return keyValues;
    }

    public final List<String> keyValueColumns() {
        return Lists.transform(keyValues, new Function<KeyValue, String>() {
            @Override
            public String apply(KeyValue input) {
                return input.key();
            }
        });
    }

    public final List<byte[]> keyValueValues() {
        return Lists.transform(keyValues, new Function<KeyValue, byte[]>() {
            @Override
            public byte[] apply(KeyValue input) {
                return input.value();
            }
        });
    }

    public final Map<String, byte[]> keyValueMap() {
        return this.keyValueMap;
    }

    public final Set<String> nullSearchColumns() {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (KeyValue keyValue : this.keyValues) {
            if (keyValue.isNull()) {
                builder.add(keyValue.key());
            }
        }

        return builder.build();
    }
}
