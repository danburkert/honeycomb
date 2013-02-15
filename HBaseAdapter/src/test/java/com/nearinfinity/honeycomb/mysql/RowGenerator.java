package com.nearinfinity.honeycomb.mysql;

import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.CombinedGenerators;
import net.java.quickcheck.generator.PrimitiveGenerators;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

public class RowGenerator implements Generator<Row> {
    Generator<Map<String, ByteBuffer>> records =
            CombinedGenerators.maps(
                    PrimitiveGenerators.strings(),
                    CombinedGenerators.nullsAnd(new ByteBufferGenerator()));
    Generator<UUID> uuids = new UUIDGenerator();

    @Override
    public Row next() {
        return new Row(records.next(), uuids.next());
    }

    private class ByteBufferGenerator implements Generator<ByteBuffer> {
        Generator<byte[]> bytes = CombinedGenerators.byteArrays();
        @Override
        public ByteBuffer next() {
            return ByteBuffer.wrap(bytes.next());
        }
    }
}