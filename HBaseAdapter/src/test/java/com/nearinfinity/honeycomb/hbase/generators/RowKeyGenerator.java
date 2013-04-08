package com.nearinfinity.honeycomb.hbase.generators;

import com.nearinfinity.honeycomb.hbase.rowkey.*;
import com.nearinfinity.honeycomb.mysql.QueryKey;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.generators.QueryKeyGenerator;
import com.nearinfinity.honeycomb.mysql.generators.RowGenerator;
import com.nearinfinity.honeycomb.mysql.generators.TableSchemaGenerator;
import com.nearinfinity.honeycomb.mysql.generators.UUIDGenerator;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import net.java.quickcheck.FrequencyGenerator;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.CombinedGenerators;
import net.java.quickcheck.generator.PrimitiveGenerators;
import net.java.quickcheck.generator.support.DefaultFrequencyGenerator;

import java.util.Collection;
import java.util.Random;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;

public class RowKeyGenerator implements Generator<RowKey> {
    private static final Random RAND = new Random();
    private static final TablesRowKey tablesRow = new TablesRowKey();
    private static final RowsRowKey rowsRow = new RowsRowKey();
    private static final AutoIncRowKey autoIncRow = new AutoIncRowKey();
    private static final SchemaRowKey schemaRow = new SchemaRowKey();
    private static final Generator<Long> randIdGen = CombinedGenerators.uniqueValues(
            PrimitiveGenerators.longs(0, 1024));
    private static final Generator<SortOrder> randSortOrder = PrimitiveGenerators.enumValues(SortOrder.class);
    private static final Generator<UUID> uuidGen = new UUIDGenerator();
    private static final Generator<TableSchema> tableSchemaGen = new TableSchemaGenerator(1);
    private final FrequencyGenerator<RowKey> rowKeyGen;

    public RowKeyGenerator() {
        // The duplicated generator types are testing the sorts of the different
        // parts of the row key.  E.G. There are two ColumnSchema generators.
        // The first tests sorting on tableId, the second holds tableId constant
        // and tests sorting on columnId.

        rowKeyGen = new DefaultFrequencyGenerator<RowKey>(new PrefixRowGenerator(), 1);
        rowKeyGen.add(new TableIDRowGenerator(), 1);
        rowKeyGen.add(new DataRowGenerator(randIdGen), 3);
        rowKeyGen.add(new DataRowGenerator(fixedLong()), 3);
        rowKeyGen.add(new IndexRowGenerator(randIdGen, randIdGen, tableSchemaGen.next(), randSortOrder), 4);
        rowKeyGen.add(new IndexRowGenerator(fixedLong(), randIdGen, tableSchemaGen.next(), randSortOrder), 4);
        rowKeyGen.add(new IndexRowGenerator(fixedLong(), fixedLong(), tableSchemaGen.next(), randSortOrder), 8);

    }

    private Generator<Long> fixedLong() {
        return PrimitiveGenerators.fixedValues(randIdGen.next());
    }

    public static Generator<RowKey> getAscIndexRowKeyGenerator(TableSchema schema) {
        return new IndexRowGenerator(
                PrimitiveGenerators.fixedValues(randIdGen.next()),
                PrimitiveGenerators.fixedValues(randIdGen.next()),
                schema,
                PrimitiveGenerators.fixedValues(SortOrder.Ascending)
        );
    }

    public static Generator<RowKey> getDescIndexRowKeyGenerator(TableSchema schema) {
        return new IndexRowGenerator(
                PrimitiveGenerators.fixedValues(randIdGen.next()),
                PrimitiveGenerators.fixedValues(randIdGen.next()),
                schema,
                PrimitiveGenerators.fixedValues(SortOrder.Descending)
        );
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

    private class TableIDRowGenerator implements Generator<RowKey> {
        @Override
        public RowKey next() {
            if (RAND.nextBoolean()) {
                return new ColumnsRowKey(randIdGen.next());
            } else {
                return new IndicesRowKey(randIdGen.next());
            }
        }
    }

    private class DataRowGenerator implements Generator<RowKey> {
        Generator<Long> tableIdGen;

        public DataRowGenerator(Generator<Long> generator) {
            tableIdGen = generator;
        }

        @Override
        public RowKey next() {
            return new DataRowKey(tableIdGen.next(), uuidGen.next());
        }
    }

    private static class IndexRowGenerator implements Generator<RowKey> {
        private final Generator<Long> tableIds;
        private final Generator<Long> indexIds;
        private final TableSchema tableSchema;
        private final Generator<IndexSchema> indexSchemas;
        private final Generator<Row> rows;
        private final Generator<QueryKey> queryKeys;
        private final Generator<SortOrder> order;

        public IndexRowGenerator(
                Generator<Long> tableIds,
                Generator<Long> indexIds,
                TableSchema tableSchema,
                Generator<SortOrder> order) {
            this.tableIds = tableIds;
            this.indexIds = indexIds;
            this.tableSchema = tableSchema;
            Collection<IndexSchema> indices = this.tableSchema.getIndices();
            checkState(indices.size() > 0, "Generated table schema must have an index.");
            this.indexSchemas = PrimitiveGenerators.fixedValues(indices);
            this.rows = new RowGenerator(this.tableSchema);
            this.queryKeys = new QueryKeyGenerator(this.tableSchema);
            this.order = order;
        }

        @Override
        public RowKey next() {
            IndexRowKeyBuilder builder;
            if (RAND.nextBoolean()) {
                Row row = rows.next();
                builder = IndexRowKeyBuilder
                        .newBuilder(tableIds.next(), indexIds.next())
                        .withRow(row,
                                indexSchemas.next().getIndexName(),
                                tableSchema)
                        .withUUID(row.getUUID());
            } else {
                builder = IndexRowKeyBuilder
                        .newBuilder(tableIds.next(), indexIds.next())
                        .withQueryKey(queryKeys.next(), tableSchema);
            }
            return builder.withSortOrder(order.next()).build();
        }
    }
}