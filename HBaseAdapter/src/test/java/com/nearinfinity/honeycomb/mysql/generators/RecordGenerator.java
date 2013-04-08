package com.nearinfinity.honeycomb.mysql.generators;

import com.google.common.primitives.Longs;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.CombinedGenerators;
import net.java.quickcheck.generator.PrimitiveGenerators;

import java.nio.ByteBuffer;

public class RecordGenerator implements Generator<ByteBuffer> {
    Generator<byte[]> generator;

    public RecordGenerator(ColumnSchema schema) {
        switch (schema.getType()) {
            case ULONG:
            case LONG:
            case TIME: {
                generator = new LongBytesGenerator();
                break;
            }
            case DOUBLE: {
                generator = new DoubleBytesGenerator();
                break;
            }
            case BINARY: {
                generator = CombinedGenerators.byteArrays(
                        PrimitiveGenerators.integers(0, schema.getMaxLength()));
                break;
            }
            case STRING: {
                generator = new StringBytesGenerator(schema.getMaxLength());
                break;
            }
            default:
                generator = CombinedGenerators.byteArrays(
                        PrimitiveGenerators.fixedValues(32));
        }
        if (schema.getIsNullable()) {
            generator = CombinedGenerators.nullsAnd(generator, 10);
        }
    }

    @Override
    public ByteBuffer next() {
        byte[] next = generator.next();
        if (next == null) { return null; }
        else { return ByteBuffer.wrap(next); }
    }

    private class LongBytesGenerator implements Generator<byte[]> {
        private Generator<Long> longs = PrimitiveGenerators.longs();
        @Override
        public byte[] next() {
            return Longs.toByteArray(longs.next());
        }
    }

    private class DoubleBytesGenerator implements Generator<byte[]> {
        private Generator<Double> doubles = PrimitiveGenerators.doubles();
        @Override
        public byte[] next() {
            return Longs.toByteArray(Double.doubleToLongBits(doubles.next()));
        }
    }

    private class StringBytesGenerator implements Generator<byte[]> {
        private Generator<String> strings;
        public StringBytesGenerator(int maxLength) {
            strings = PrimitiveGenerators.strings(maxLength);
        }
        @Override
        public byte[] next() {
            return strings.next().getBytes();
        }
    }
}