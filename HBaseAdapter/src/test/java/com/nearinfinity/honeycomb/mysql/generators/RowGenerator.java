package com.nearinfinity.honeycomb.mysql.generators;

import com.google.common.collect.ImmutableMap;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.TableSchema;
import net.java.quickcheck.Generator;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class RowGenerator implements Generator<Row> {
    private static final Random RAND = new Random();
    private static final Generator<UUID> uuids = new UUIDGenerator();
    private final Map<String, Generator<ByteBuffer>> recordGenerators;

    public RowGenerator(TableSchema schema) {
        super();
        ImmutableMap.Builder<String, Generator<ByteBuffer>> recordGenerators = ImmutableMap.builder();
        for (ColumnSchema columns : schema.getColumns()) {
            recordGenerators.put(columns.getColumnName(), new RecordGenerator(columns));
        }
        this.recordGenerators = recordGenerators.build();
    }

    @Override
    public Row next() {
        ImmutableMap.Builder<String, ByteBuffer> records = ImmutableMap.builder();
        for (Map.Entry<String, Generator<ByteBuffer>> record : recordGenerators.entrySet()) {
            ByteBuffer nextValue = record.getValue().next();
            if (nextValue != null) {
                records.put(record.getKey(), nextValue);
            }
        }
        return new Row(records.build(), uuids.next());
    }
}