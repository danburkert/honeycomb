/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 * Copyright 2013 Altamira Corporation.
 */


package com.nearinfinity.honeycomb.hbase.generators;

import static com.google.common.base.Preconditions.checkState;

import java.util.Collection;
import java.util.Random;
import java.util.UUID;

import net.java.quickcheck.FrequencyGenerator;
import net.java.quickcheck.Generator;
import net.java.quickcheck.collection.Pair;
import net.java.quickcheck.generator.CombinedGenerators;
import net.java.quickcheck.generator.PrimitiveGenerators;
import net.java.quickcheck.generator.support.DefaultFrequencyGenerator;

import com.nearinfinity.honeycomb.hbase.rowkey.AutoIncRowKey;
import com.nearinfinity.honeycomb.hbase.rowkey.ColumnsRowKey;
import com.nearinfinity.honeycomb.hbase.rowkey.DataRowKey;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRowKey;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRowKeyBuilder;
import com.nearinfinity.honeycomb.hbase.rowkey.IndicesRowKey;
import com.nearinfinity.honeycomb.hbase.rowkey.RowKey;
import com.nearinfinity.honeycomb.hbase.rowkey.RowsRowKey;
import com.nearinfinity.honeycomb.hbase.rowkey.SchemaRowKey;
import com.nearinfinity.honeycomb.hbase.rowkey.SortOrder;
import com.nearinfinity.honeycomb.hbase.rowkey.TablesRowKey;
import com.nearinfinity.honeycomb.mysql.QueryKey;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.generators.QueryKeyGenerator;
import com.nearinfinity.honeycomb.mysql.generators.RowGenerator;
import com.nearinfinity.honeycomb.mysql.generators.TableSchemaGenerator;
import com.nearinfinity.honeycomb.mysql.generators.UUIDGenerator;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;

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
        rowKeyGen.add(new DataRowKeyGenerator(randIdGen), 3);
        rowKeyGen.add(new DataRowKeyGenerator(fixedLong()), 3);
        rowKeyGen.add(new IndexRowKeyGenerator(randIdGen, randIdGen, tableSchemaGen.next(), randSortOrder), 4);
        rowKeyGen.add(new IndexRowKeyGenerator(fixedLong(), randIdGen, tableSchemaGen.next(), randSortOrder), 4);
        rowKeyGen.add(new IndexRowKeyGenerator(fixedLong(), fixedLong(), tableSchemaGen.next(), randSortOrder), 8);

    }

    private Generator<Long> fixedLong() {
        return PrimitiveGenerators.fixedValues(randIdGen.next());
    }

    public static IndexRowKeyGenerator getAscIndexRowKeyGenerator(TableSchema schema) {
        return new IndexRowKeyGenerator(
                PrimitiveGenerators.fixedValues(randIdGen.next()),
                PrimitiveGenerators.fixedValues(randIdGen.next()),
                schema,
                PrimitiveGenerators.fixedValues(SortOrder.Ascending)
        );
    }

    public static IndexRowKeyGenerator getDescIndexRowKeyGenerator(TableSchema schema) {
        return new IndexRowKeyGenerator(
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
            }

            return new IndicesRowKey(randIdGen.next());
        }
    }

    private class DataRowKeyGenerator implements Generator<RowKey> {
        Generator<Long> tableIdGen;

        public DataRowKeyGenerator(Generator<Long> generator) {
            tableIdGen = generator;
        }

        @Override
        public RowKey next() {
            return new DataRowKey(tableIdGen.next(), uuidGen.next());
        }
    }

    public static class IndexRowKeyGenerator implements Generator<RowKey> {
        private final Generator<Long> tableIds;
        private final Generator<Long> indexIds;
        private final TableSchema tableSchema;
        private final Generator<IndexSchema> indexSchemas;
        private final Generator<Row> rows;
        private final Generator<QueryKey> queryKeys;
        private final Generator<SortOrder> order;

        public IndexRowKeyGenerator(
                Generator<Long> tableIds,
                Generator<Long> indexIds,
                TableSchema tableSchema,
                Generator<SortOrder> order) {
            this.tableIds = tableIds;
            this.indexIds = indexIds;
            this.tableSchema = tableSchema;
            Collection<IndexSchema> indices = this.tableSchema.getIndices();
            checkState(indices.size() > 0, "Generated table schema must have an index.");
            indexSchemas = PrimitiveGenerators.fixedValues(indices);
            rows = new RowGenerator(this.tableSchema);
            queryKeys = new QueryKeyGenerator(this.tableSchema);
            this.order = order;
        }

        @Override
        public RowKey next() {
            IndexRowKeyBuilder builder;
            Row row = rows.next();
            builder = IndexRowKeyBuilder
                    .newBuilder(tableIds.next(), indexIds.next())
                    .withRow(row,
                            indexSchemas.next().getIndexName(),
                            tableSchema)
                    .withUUID(row.getUUID());

            return builder.withSortOrder(order.next()).build();
        }

        public Pair<IndexRowKey, QueryKey> nextWithQueryKey() {
            QueryKey queryKey = queryKeys.next();
            IndexRowKey rowKey = IndexRowKeyBuilder
                    .newBuilder(tableIds.next(), indexIds.next())
                    .withQueryKey(queryKey, tableSchema)
                    .withSortOrder(order.next())
                    .build();
            return new Pair<IndexRowKey, QueryKey>(rowKey, queryKey);
        }
    }
}