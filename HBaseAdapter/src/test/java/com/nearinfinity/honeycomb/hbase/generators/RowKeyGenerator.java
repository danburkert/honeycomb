package com.nearinfinity.honeycomb.hbase.generators;

import com.google.common.collect.Maps;
import com.nearinfinity.honeycomb.hbase.RowKey;
import com.nearinfinity.honeycomb.hbase.rowkey.*;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.generators.UUIDGenerator;
import net.java.quickcheck.FrequencyGenerator;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.CombinedGenerators;
import net.java.quickcheck.generator.PrimitiveGenerators;
import net.java.quickcheck.generator.distribution.Distribution;
import net.java.quickcheck.generator.support.DefaultFrequencyGenerator;
import net.java.quickcheck.generator.support.FixedValuesGenerator;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class RowKeyGenerator implements Generator<RowKey> {
    private static final Random RAND = new Random();
    private static final TablesRow tablesRow = new TablesRow();
    private static final RowsRow rowsRow = new RowsRow();
    private static final AutoIncRow autoIncRow = new AutoIncRow();
    private static final SchemaRow schemaRow = new SchemaRow();
    private static final Generator<Map<String, byte[]>> recordGen =
            CombinedGenerators.maps(PrimitiveGenerators.strings(), CombinedGenerators.byteArrays(),
                    PrimitiveGenerators.integers(4, 4, Distribution.UNIFORM));
    private static Generator<Long> randIdGen = PrimitiveGenerators.longs(0, Long.MAX_VALUE);
    private static Generator<UUID> uuidGen = new UUIDGenerator();
    private final FrequencyGenerator<RowKey> rowKeyGen;

    public RowKeyGenerator() {
        // The duplicated generator types are testing the sorts of the different
        // parts of the row key.  E.G. There are two ColumnSchema generators.
        // The first tests sorting on tableId, the second holds tableId constant
        // and tests sorting on columnId.
        rowKeyGen = new DefaultFrequencyGenerator<RowKey>(new PrefixRowGenerator(), 2);
        rowKeyGen.add(new ColumnsRowGenerator(), 1);
        rowKeyGen.add(new IndicesRowGenerator(), 1);
        rowKeyGen.add(new DataRowGenerator(randIdGen), 3);
        rowKeyGen.add(new DataRowGenerator(fixedLong()), 3);
        rowKeyGen.add(new IndexRowGenerator(randIdGen, randIdGen, recordGen), 3);
        rowKeyGen.add(new IndexRowGenerator(fixedLong(), randIdGen, recordGen), 4);
        rowKeyGen.add(new IndexRowGenerator(fixedLong(), fixedLong(), recordGen), 8);
        rowKeyGen.add(new IndexRowGenerator(fixedLong(), fixedLong(), fixedRecord()), 8);
    }

    private FixedValuesGenerator<Map<String, byte[]>> fixedRecord() {
        return new FixedValuesGenerator<Map<String, byte[]>>(recordGen.next());
    }

    private FixedValuesGenerator<Long> fixedLong() {
        return new FixedValuesGenerator<Long>(randIdGen.next());
    }

    @Override
    public RowKey next() {
        return rowKeyGen.next();
    }

    private class PrefixRowGenerator implements Generator<RowKey> {
        @Override
        public RowKey next() {
            switch (RAND.nextInt(4)) {
                case 0:
                    return tablesRow;
                case 1:
                    return rowsRow;
                case 2:
                    return autoIncRow;
                case 3:
                    return schemaRow;
                default:
                    throw new RuntimeException("Should never reach me");
            }
        }
    }

    private class ColumnsRowGenerator implements Generator<RowKey> {
        @Override
        public RowKey next() {
            return new ColumnsRow(randIdGen.next());
        }
    }

    private class IndicesRowGenerator implements Generator<RowKey> {
        @Override
        public RowKey next() {
            return new ColumnsRow(randIdGen.next());
        }
    }

    private class DataRowGenerator implements Generator<RowKey> {
        Generator<Long> tableIdGen;

        public DataRowGenerator(Generator<Long> generator) {
            tableIdGen = generator;
        }

        @Override
        public RowKey next() {
            return new DataRow(tableIdGen.next(), uuidGen.next());
        }
    }

    private class IndexRowGenerator implements Generator<RowKey> {
        private final Generator<Long> tableIdGen;
        private final Generator<Long> indexIdGen;
        private final Generator<Map<String, byte[]>> recordsGen;
        private final Generator<ColumnType> columnTypeGenerator = PrimitiveGenerators.enumValues(ColumnType.class);

        public IndexRowGenerator(
                Generator<Long> tableIdGen,
                Generator<Long> indexIdGen,
                Generator<Map<String, byte[]>> recordsGen) {
            this.tableIdGen = tableIdGen;
            this.indexIdGen = indexIdGen;
            this.recordsGen = recordsGen;
        }

        @Override
        public RowKey next() {
            Map<String, ColumnType> columnTypeMap = Maps.newHashMap();
            Map<String, byte[]> records = recordGen.next();
            for (Map.Entry<String, byte[]> entry : records.entrySet()) {
                ColumnType next = columnTypeGenerator.next();
                columnTypeMap.put(entry.getKey(), next);
                switch (next) {
                    case LONG:
                    case ULONG:
                    case TIME: {
                        records.put(entry.getKey(), Bytes.getBytes((ByteBuffer) ByteBuffer.allocate(8).putLong(RAND.nextLong()).rewind()));
                    }
                    case DOUBLE: {
                        records.put(entry.getKey(), Bytes.getBytes((ByteBuffer) ByteBuffer.allocate(8).putDouble(RAND.nextDouble()).rewind()));
                    }
                }
            }
            IndexRowBuilder builder = IndexRowBuilder
                    .newBuilder(tableIdGen.next(), indexIdGen.next())
                    .withRecords(records, columnTypeMap, records.keySet())
                    .withUUID(uuidGen.next());

            if (RAND.nextBoolean()) {
                return builder.withSortOrder(SortOrder.Ascending).build();
            }

            return builder.withSortOrder(SortOrder.Descending).build();
        }
    }
}