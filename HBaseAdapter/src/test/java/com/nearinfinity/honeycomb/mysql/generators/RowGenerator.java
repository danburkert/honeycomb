package com.nearinfinity.honeycomb.mysql.generators;

import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.CombinedGenerators;
import net.java.quickcheck.generator.PrimitiveGenerators;

import java.util.Map;
import java.util.UUID;

import com.nearinfinity.honeycomb.mysql.Row;

public class RowGenerator implements Generator<Row> {
    private static Generator<Map<String, Object>> records =
            CombinedGenerators.maps(
                    PrimitiveGenerators.strings(),
                    new ByteBufferGenerator<Object>());
    Generator<UUID> uuids = new UUIDGenerator();

    @Override
    public Row next() {
        return new Row(records.next(), uuids.next());
    }
}