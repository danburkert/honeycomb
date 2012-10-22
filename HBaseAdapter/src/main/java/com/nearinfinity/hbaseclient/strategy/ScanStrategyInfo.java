package com.nearinfinity.hbaseclient.strategy;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.nearinfinity.hbaseclient.KeyValue;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class ScanStrategyInfo {
    private final String tableName;
    private final List<String> columnNames;
    private final List<KeyValue> keyValues;
    private final Map<String, byte[]> keyValueMap;

    public ScanStrategyInfo(final String tableName, final List<String> columnNames, List<KeyValue> keyValues) {
        if (keyValues == null) {
            keyValues = new LinkedList<KeyValue>();
        }

        this.tableName = tableName;
        this.columnNames = columnNames;
        this.keyValues = keyValues;
        ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
        for (KeyValue keyValue : keyValues) {
            builder.put(keyValue.key(), keyValue.value());
        }

        keyValueMap = builder.build();
    }

    public final String tableName() {
        return tableName;
    }

    public final List<String> columnNames() {
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
