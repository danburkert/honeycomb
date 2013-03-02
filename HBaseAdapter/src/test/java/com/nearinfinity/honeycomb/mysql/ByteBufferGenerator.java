package com.nearinfinity.honeycomb.mysql;

import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.CombinedGenerators;

import java.nio.ByteBuffer;

public class ByteBufferGenerator<T> implements Generator<T> {
    Generator<byte[]> bytes = CombinedGenerators.byteArrays();

    @Override
    public T next() {
        return (T) ByteBuffer.wrap(bytes.next());
    }
}