package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.hbase.gen.*;
import net.java.quickcheck.FrequencyGenerator;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.PrimitiveGenerators;
import net.java.quickcheck.generator.support.DefaultFrequencyGenerator;

public class RowKeyGenerator implements Generator<RowKey> {
    Generator<Long> longGen = PrimitiveGenerators.longs(0, 150);
    static FrequencyGenerator<Object> rowGen;

    public RowKeyGenerator() {
        rowGen = new DefaultFrequencyGenerator<Object>(new RootRowGenerator<Object>());
        rowGen.add(new ColumnsRowGenerator());
        rowGen.add(new ColumnsMetadataRowGenerator());
    }

    @Override
    public RowKey next() {
        return new RowKey(rowGen.next());
    }

    private class RootRowGenerator<Object> implements Generator<Object> {
        private final RootRow rootRow = new RootRow(new TablesConst("TABLES".getBytes()));
        @Override
        public Object next() {
            return (Object) rootRow;
        }
    }

    private class ColumnsRowGenerator implements Generator<Object> {
        @Override
        public Object next() {
            return new ColumnsRow(longGen.next());
        }
    }

    private class ColumnsMetadataRowGenerator implements Generator<Object> {
        @Override
        public Object next() {
            return new ColumnMetadataRow(longGen.next(), longGen.next());
        }
    }
}
