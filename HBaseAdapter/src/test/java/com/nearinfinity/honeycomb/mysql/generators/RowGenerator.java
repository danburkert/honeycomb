package com.nearinfinity.honeycomb.mysql.generators;

import com.nearinfinity.honeycomb.mysql.Row;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.CombinedGenerators;
import net.java.quickcheck.generator.PrimitiveGenerators;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

public class RowGenerator implements Generator<Row> {
    private static Generator<Map<String, ByteBuffer>> records =
            CombinedGenerators.maps(
                    PrimitiveGenerators.strings(),
                    new ByteBufferGenerator<ByteBuffer>());
    Generator<UUID> uuids = new UUIDGenerator();

    @Override
    public Row next() {
        return new Row(records.next(), uuids.next());
    }
}