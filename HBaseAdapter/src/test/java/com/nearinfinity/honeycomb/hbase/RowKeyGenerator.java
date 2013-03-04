package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.hbase.rowkey.*;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.mysql.UUIDGenerator;
import net.java.quickcheck.FrequencyGenerator;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.CombinedGenerators;
import net.java.quickcheck.generator.PrimitiveGenerators;
import net.java.quickcheck.generator.support.DefaultFrequencyGenerator;
import net.java.quickcheck.generator.support.FixedValuesGenerator;

import java.util.*;

public class RowKeyGenerator implements Generator<RowKey> {
    private static final Random rand = new Random();
    private static final TablesRow tablesRow = new TablesRow();
    private static final RowsRow rowsRow = new RowsRow();
    private static final AutoIncRow autoIncRow = new AutoIncRow();
    private static final SchemaRow schemaRow = new SchemaRow();

    private static Generator<Long> randIdGen = PrimitiveGenerators.longs(0, Long.MAX_VALUE);
    private static Generator<UUID> uuidGen = new UUIDGenerator();
    private static Generator<byte[]> randValueGen =
            CombinedGenerators.nullsAnd(CombinedGenerators.byteArrays(), 5);

    private Generator<List<Long>> randIdsGen = CombinedGenerators.lists(randIdGen,
            PrimitiveGenerators.integers(1, Constants.KEY_PART_COUNT));
    private FrequencyGenerator<RowKey> rowKeyGen;

    public RowKeyGenerator() {
        // The duplicated generator types are testing the sorts of the different
        // parts of the row key.  E.G. There are two ColumnSchema generators.
        // The first tests sorting on tableId, the second holds tableId constant
        // and tests sorting on columnId.
        rowKeyGen = new DefaultFrequencyGenerator<RowKey>(new PrefixRowGenerator(), 2);
        rowKeyGen.add(new ColumnsRowGenerator(), 1);
        rowKeyGen.add(new DataRowGenerator(), 3);
        rowKeyGen.add(new DataRowGenerator(randIdGen.next()), 3);
        rowKeyGen.add(new IndexRowGenerator(), 3);
        rowKeyGen.add(new IndexRowGenerator(randIdGen.next()), 4);
        rowKeyGen.add(new IndexRowGenerator(randIdGen.next(), randIdsGen.next()), 8);

        List<Long> fixedIds = randIdsGen.next();
        List<byte[]> fixedValues = new ArrayList<byte[]>();
        for (int i = 0; i < fixedIds.size(); i++) {
            fixedValues.add(randValueGen.next());
        }
        rowKeyGen.add(new IndexRowGenerator(randIdGen.next(), fixedIds, fixedValues), 4);
    }

    @Override
    public RowKey next() {
        return rowKeyGen.next();
    }

    private class PrefixRowGenerator implements Generator<RowKey> {
        public RowKey next() {
            switch (rand.nextInt(4)) {
                case 0:
                    return tablesRow;
                case 1:
                    return rowsRow;
                case 2:
                    return autoIncRow;
                case 3:
                    return schemaRow;
            }
            throw new RuntimeException("Should never reach me");
        }
    }

    private class ColumnsRowGenerator implements Generator<RowKey> {
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
        private Generator<Long> columnIdGen;
        private Generator<List<Long>> columnsGen;
        private Generator<byte[]> valueGen;

        public IndexRowGenerator() {
            columnIdGen = randIdGen;
            columnsGen = randIdsGen;
            valueGen = randValueGen;
        }

        public IndexRowGenerator(Long tableId) {
            columnIdGen = new FixedValuesGenerator<Long>(tableId);
            columnsGen = CombinedGenerators.lists(randIdGen,
                    PrimitiveGenerators.integers(1, 4));
            valueGen = randValueGen;
        }

        public IndexRowGenerator(Long tableId, List<Long> tableIds) {
            columnIdGen = new FixedValuesGenerator<Long>(tableId);
            columnsGen = new FixedValuesGenerator<List<Long>>(tableIds);
            valueGen = randValueGen;
        }

        public IndexRowGenerator(Long tableId, List<Long> tableIds, List<byte[]> values) {
            columnIdGen = new FixedValuesGenerator<Long>(tableId);
            columnsGen = new FixedValuesGenerator<List<Long>>(tableIds);
            valueGen = new FixedValuesGenerator<byte[]>(values);
        }

        @Override
        public RowKey next() {
            List<Long> columnIds = columnsGen.next();
            Map<Long, byte[]> records = new HashMap<Long, byte[]>();
            for (Long columnId : columnIds) {
                records.put(columnId, valueGen.next());
            }
            return createIndexRow(columnIdGen.next(), uuidGen.next(),
                    columnIds, records);
        }

        private RowKey createIndexRow(Long tableId, UUID uuid, List<Long> columnIds,
                                      Map<Long, byte[]> records) {
            if (rand.nextBoolean()) {
                return new AscIndexRow(tableId, columnIds, records, uuid);
            } else {
                return new DescIndexRow(tableId, columnIds, records, uuid);
            }
        }
    }
}