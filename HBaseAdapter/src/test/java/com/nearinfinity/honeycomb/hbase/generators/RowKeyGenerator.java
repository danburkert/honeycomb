package com.nearinfinity.honeycomb.hbase.generators;

import com.nearinfinity.honeycomb.hbase.RowKey;
import com.nearinfinity.honeycomb.hbase.rowkey.*;
import com.nearinfinity.honeycomb.mysql.generators.UUIDGenerator;
import net.java.quickcheck.FrequencyGenerator;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.CombinedGenerators;
import net.java.quickcheck.generator.PrimitiveGenerators;
import net.java.quickcheck.generator.support.DefaultFrequencyGenerator;
import net.java.quickcheck.generator.support.FixedValuesGenerator;

import java.util.*;

public class RowKeyGenerator implements Generator<RowKey> {
    private static final Random RAND = new Random();
    private static final TablesRow tablesRow = new TablesRow();
    private static final RowsRow rowsRow = new RowsRow();
    private static final AutoIncRow autoIncRow = new AutoIncRow();
    private static final SchemaRow schemaRow = new SchemaRow();
    private static Generator<Long> randIdGen = PrimitiveGenerators.longs(0, Long.MAX_VALUE);
    private static Generator<UUID> uuidGen = new UUIDGenerator();
    private static Generator<byte[]> randValueGen =
            CombinedGenerators.nullsAnd(CombinedGenerators.byteArrays(), 5);
    private static Generator<List<byte[]>> randRecordsGen =
            CombinedGenerators.lists(randValueGen, 1, 4);
    private final FrequencyGenerator<RowKey> rowKeyGen;

    public RowKeyGenerator() {
        // The duplicated generator types are testing the sorts of the different
        // parts of the row key.  E.G. There are two ColumnSchema generators.
        // The first tests sorting on tableId, the second holds tableId constant
        // and tests sorting on columnId.
        rowKeyGen = new DefaultFrequencyGenerator<RowKey>(new PrefixRowGenerator(), 2);
        rowKeyGen.add(new ColumnsRowGenerator(), 1);
        rowKeyGen.add(new IndicesRowGenerator(), 1);
        rowKeyGen.add(new DataRowGenerator(), 3);
        rowKeyGen.add(new DataRowGenerator(randIdGen.next()), 3);
        rowKeyGen.add(new IndexRowGenerator(), 3);
        rowKeyGen.add(new IndexRowGenerator(randIdGen.next()), 4);
        rowKeyGen.add(new IndexRowGenerator(randIdGen.next(), randIdGen.next()), 8);
        rowKeyGen.add(new IndexRowGenerator(randIdGen.next(), randIdGen.next(), randRecordsGen.next()), 8);
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
                    break;
            }
            throw new RuntimeException("Should never reach me");
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

        public DataRowGenerator() {
            tableIdGen = randIdGen;
        }

        public DataRowGenerator(Long tableId) {
            tableIdGen = new FixedValuesGenerator<Long>(tableId);
        }

        @Override
        public RowKey next() {
            return new DataRow(tableIdGen.next(), uuidGen.next());
        }
    }

    private class IndexRowGenerator implements Generator<RowKey> {
        private final Generator<Long> tableIdGen;
        private final Generator<Long> indexIdGen;
        private final Generator<List<byte[]>> recordsGen;

        public IndexRowGenerator() {
            this.tableIdGen = randIdGen;
            this.indexIdGen = randIdGen;
            this.recordsGen = randRecordsGen;
        }

        public IndexRowGenerator(Long tableId) {
            this.tableIdGen = new FixedValuesGenerator<Long>(tableId);
            this.indexIdGen = randIdGen;
            this.recordsGen = randRecordsGen;
        }

        public IndexRowGenerator(Long tableId, Long indexId) {
            this.tableIdGen = new FixedValuesGenerator<Long>(tableId);
            this.indexIdGen = new FixedValuesGenerator<Long>(indexId);
            int num_records = RAND.nextInt(16) + 1;
            this.recordsGen = CombinedGenerators.lists(randValueGen,
                    new FixedValuesGenerator<Integer>(num_records));
        }

        public IndexRowGenerator(Long tableId, Long indexId, List<byte[]> values) {
            this.tableIdGen = new FixedValuesGenerator<Long>(tableId);
            this.indexIdGen = new FixedValuesGenerator<Long>(indexId);
            this.recordsGen = new FixedValuesGenerator<List<byte[]>>(values);
        }

        @Override
        public RowKey next() {
            return createIndexRow(tableIdGen.next(), indexIdGen.next(),
                    recordsGen.next(), uuidGen.next()
            );
        }

        private RowKey createIndexRow(Long tableId, Long indexId,
                                      List<byte[]> records, UUID uuid) {
            if (RAND.nextBoolean()) {
                return new AscIndexRow(tableId, indexId, records, uuid);
            }
            return new DescIndexRow(tableId, indexId, records, uuid);
        }
    }
}