package com.nearinfinity.honeycomb.mysql.generators;

import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.CombinedGenerators;

import java.nio.ByteBuffer;

public class ByteBufferGenerator implements Generator<ByteBuffer> {
    Generator<byte[]> bytes = CombinedGenerators.byteArrays();

    @Override
    public ByteBuffer next() {
        return ByteBuffer.wrap(bytes.next());
    }
}