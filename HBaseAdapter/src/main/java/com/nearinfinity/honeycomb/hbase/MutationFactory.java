package com.nearinfinity.honeycomb.hbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.config.Constants;
import com.nearinfinity.honeycomb.hbase.rowkey.DataRow;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRowBuilder;
import com.nearinfinity.honeycomb.hbase.rowkey.RowKey;
import com.nearinfinity.honeycomb.hbase.rowkey.SortOrder;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import com.nearinfinity.honeycomb.util.Verify;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates put and delete lists for various operations.  Meant to have no
 * side effects except for requesting metadata from the store.
 */
public class MutationFactory {
    private final HBaseStore store;

    public MutationFactory(HBaseStore store) {
        super();
        this.store = store;
    }

    /**
     * Build put list for a row insert with indices
     * @param tableId
     * @param row
     * @return
     */
    public List<Put> insert(long tableId, final Row row) {
        return insert(tableId, row, store.getSchema(tableId).getIndices());
    }

    /**
     * Build put list for a row insert with specified indices
     * @param tableId
     * @param row
     * @param indices
     * @return
     */
    public List<Put> insert(long tableId, final Row row,
                            final Map<String, IndexSchema> indices) {
        checkNotNull(row);
        // tableId, indices checked by called methods

        final byte[] serializedRow = row.serialize();
        final UUID uuid = row.getUUID();
        final ImmutableList.Builder<Put> puts = ImmutableList.builder();

        puts.add(emptyQualifierPut(new DataRow(tableId, uuid), serializedRow));
        puts.addAll(insertIndices(tableId, row, indices));

        return puts.build();
    }

    /**
     * Build put list for inserting only the specified indices of the row
     * @param tableId
     * @param row
     * @param indices
     * @return
     */
    public List<Put> insertIndices(long tableId, final Row row,
                                   final Map<String, IndexSchema> indices) {
        checkNotNull(row);
        final byte[] serializedRow = row.serialize();
        final ImmutableList.Builder<Put> puts = ImmutableList.builder();
        doToIndices(tableId, row, indices, new IndexAction() {
            @Override
            public void execute(IndexRowBuilder builder) {
                puts.add(emptyQualifierPut(builder.withSortOrder(SortOrder.Ascending).build(), serializedRow));
                puts.add(emptyQualifierPut(builder.withSortOrder(SortOrder.Descending).build(), serializedRow));
            }
        });
        return puts.build();
    }

    /**
     * Build delete list for the data and indices belonging to the row
     * @param tableId
     * @param row
     * @return
     */
    public List<Delete> delete(long tableId, final Row row) {
        List<Delete> deletes = deleteIndices(tableId, row);
        deletes.add(new Delete(new DataRow(tableId, row.getUUID()).encode()));
        return deletes;
    }

    /**
     * Build delete list for the indices belonging to the row
     * @param tableId
     * @param row
     * @return
     */
    public List<Delete> deleteIndices(long tableId, final Row row) {
        Verify.isValidId(tableId);

        final Map<String, IndexSchema> indices = store.getSchema(tableId).getIndices();
        return deleteIndices(tableId, row, indices);
    }

    /**
     * Build delete list for the specified indices belonging to the row
     * @param tableId
     * @param row
     * @return
     */
    public List<Delete> deleteIndices(long tableId, final Row row,
                                      final Map<String, IndexSchema> indices) {
        Verify.isValidId(tableId);
        checkNotNull(row);

        final List<Delete> deletes = Lists.newLinkedList();
        doToIndices(tableId, row, indices, new IndexAction() {
            @Override
            public void execute(IndexRowBuilder builder) {
                deletes.add(new Delete(builder.withSortOrder(SortOrder.Ascending).build().encode()));
                deletes.add(new Delete(builder.withSortOrder(SortOrder.Descending).build().encode()));
            }
        });
        return deletes;
    }

    private static Put emptyQualifierPut(final RowKey rowKey,
                                         final byte[] serializedRow) {
        return new Put(rowKey.encode()).add(Constants.DEFAULT_COLUMN_FAMILY,
                new byte[0], serializedRow);
    }

    private void doToIndices(long tableId,
                             final Row row,
                             final Map<String, IndexSchema> indices,
                             final IndexAction action) {

        for (Map.Entry<String, IndexSchema> index : indices.entrySet()) {
            long indexId = store.getIndexId(tableId, index.getKey());
            TableSchema schema = store.getSchema(tableId);

            IndexRowBuilder builder = IndexRowBuilder
                    .newBuilder(tableId, indexId)
                    .withUUID(row.getUUID())
                    .withSqlRow(row, index.getValue().getColumns(), schema.getColumns());
            action.execute(builder);
        }
    }

    private interface IndexAction {
        public void execute(IndexRowBuilder builder);
    }
}