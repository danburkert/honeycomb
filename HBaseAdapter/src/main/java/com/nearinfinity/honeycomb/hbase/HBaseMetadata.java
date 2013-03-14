package com.nearinfinity.honeycomb.hbase;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.nearinfinity.honeycomb.TableNotFoundException;
import com.nearinfinity.honeycomb.hbase.rowkey.*;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.Verify;
import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manages writing and reading table & column schemas, table & column ids, and
 * row & autoincrement counters to and from HBase.
 */
public class HBaseMetadata {
    private static final byte[] COLUMN_FAMILY = Constants.NIC;
    private final Provider<HTableInterface> provider;

    @Inject
    public HBaseMetadata(HTableProvider provider) {
        checkNotNull(provider);
        this.provider = provider;
    }

    public long getTableId(String table)
            throws IOException, TableNotFoundException {
        Verify.isNotNullOrEmpty(table);
        Get get = new Get(new TablesRow().encode());
        byte[] serializedName = serializeName(table);
        get.addColumn(COLUMN_FAMILY, serializedName);
        HTableInterface hTable = getHTable();
        try {
            Result result = hTable.get(get);

            byte[] tableIdBytes = result.getValue(COLUMN_FAMILY, serializedName);
            if (tableIdBytes == null) {
                throw new TableNotFoundException(table);
            }
            return deserializeId(tableIdBytes);
        } finally {
            hTable.close();
        }
    }

    public Map<String, Long> getIndexIds(long tableId) throws IOException {
        checkValidTableId(tableId);
        return getNameToIdMap(new IndicesRow(tableId).encode());
    }

    public BiMap<String, Long> getColumnIds(long tableId)
            throws IOException {
        checkValidTableId(tableId);
        Map<String, Long> nameToId = getNameToIdMap(new ColumnsRow(tableId).encode());
        return ImmutableBiMap.copyOf(nameToId);
    }

    public TableSchema getSchema(long tableId)
            throws IOException, TableNotFoundException {
        checkValidTableId(tableId);
        byte[] serializedTableId = serializeId(tableId);

        Get get = new Get(new SchemaRow().encode());
        get.addColumn(COLUMN_FAMILY, serializedTableId);
        HTableInterface hTable = getHTable();
        try {
            Result result = hTable.get(get);

            byte[] serializedSchema = result.getValue(COLUMN_FAMILY, serializedTableId);
            if (serializedSchema == null) {
                throw new TableNotFoundException(tableId);
            }
            return Util.deserializeTableSchema(serializedSchema);
        } finally {
            hTable.close();
        }
    }

    public void createTable(String tableName, TableSchema schema)
            throws IOException {
        Verify.isNotNullOrEmpty(tableName);
        checkNotNull(schema);
        checkIndicesMatchColumns(schema.getIndices(), schema.getColumns());
        long tableId = getNextTableId();
        List<Put> puts = new ArrayList<Put>();
        puts.add(putTableId(tableName, tableId));
        puts.add(putColumnIds(tableId, schema.getColumns()));
        puts.add(putTableSchema(tableId, schema));
        puts.add(putIndices(tableId, schema.getIndices()));

        performMutations(ImmutableList.<Delete>of(), puts);
    }

    public void deleteSchema(String tableName)
            throws TableNotFoundException, IOException {
        Verify.isNotNullOrEmpty(tableName);
        long tableId = getTableId(tableName);
        byte[] serializedId = serializeId(tableId);

        List<Delete> deletes = new ArrayList<Delete>();

        Delete columnIdsDelete = new Delete(new ColumnsRow(tableId).encode());
        Delete indicesIdsDelete = new Delete(new IndicesRow(tableId).encode());

        Delete rowsDelete = new Delete(new RowsRow().encode());
        rowsDelete.deleteColumn(COLUMN_FAMILY, serializedId);

        deletes.add(deleteTableId(tableName));
        deletes.add(columnIdsDelete);
        deletes.add(indicesIdsDelete);
        deletes.add(rowsDelete);
        deletes.add(deleteAutoIncCounter(tableId));
        deletes.add(deleteTableSchema(tableId));

        performMutations(deletes, ImmutableList.<Put>of());
    }

    public void updateSchema(long tableId, TableSchema oldSchema, TableSchema newSchema)
            throws IOException {
        checkValidTableId(tableId);
        checkNotNull(oldSchema);
        checkNotNull(newSchema);

        if (oldSchema.equals(newSchema)) {
            return;
        }

        List<Put> puts = new ArrayList<Put>();
        List<Delete> deletes = new ArrayList<Delete>();

        MapDifference<String, ColumnSchema> diff = Maps.difference(oldSchema.getColumns(),
                newSchema.getColumns());

        for (Map.Entry<String, ColumnSchema> deletedColumn :
                diff.entriesOnlyOnLeft().entrySet()) {
            String columnName = deletedColumn.getKey();
            ColumnSchema schema = deletedColumn.getValue();

            Delete columnIdDelete = new Delete(new ColumnsRow(tableId).encode());
            columnIdDelete.deleteColumn(COLUMN_FAMILY, serializeName(columnName));
            deletes.add(columnIdDelete);

            if (schema.getIsAutoIncrement()) {
                deletes.add(deleteAutoIncCounter(tableId));
            }
        }

        puts.add(putColumnIds(tableId, diff.entriesOnlyOnRight())); // New columns

        for (Map.Entry<String, MapDifference.ValueDifference<ColumnSchema>> changedColumn :
                diff.entriesDiffering().entrySet()) {
            if (changedColumn.getValue().leftValue().getIsAutoIncrement()
                    && !changedColumn.getValue().rightValue().getIsAutoIncrement()) {
                deletes.add(deleteAutoIncCounter(tableId));
            }
        }

        if (diff.entriesInCommon().size() == 0) {
            // All columns have been changed.  Perhaps we should truncate.
        }

        puts.add(putTableSchema(tableId, newSchema));

        performMutations(deletes, puts);
    }

    /**
     * Performs the operations necessary to rename an existing table stored in a {@link TablesRow} row
     *
     * @param oldTableName The name of the existing table
     * @param newTableName The new name to use for this table
     * @throws IOException            Thrown on HBase mutation failure
     * @throws TableNotFoundException Thrown when existing table cannot be found
     */
    public void renameExistingTable(final String oldTableName, final String newTableName) throws IOException, TableNotFoundException {
        Verify.isNotNullOrEmpty(oldTableName, "Old table name must have value");
        Verify.isNotNullOrEmpty(newTableName, "New table name must have value");

        final long tableId = getTableId(oldTableName);

        List<Delete> deletes = Lists.newArrayList(deleteTableId(oldTableName));
        List<Put> puts = Lists.newArrayList(putTableId(newTableName, tableId));

        performMutations(deletes, puts);
    }

    public long getAutoInc(long tableId) throws IOException {
        checkValidTableId(tableId);
        return getCounter(new AutoIncRow().encode(), serializeId(tableId));
    }

    public long incrementAutoInc(long tableId, long amount) throws IOException {
        checkValidTableId(tableId);
        return incrementCounter(new AutoIncRow().encode(),
                serializeId(tableId), amount);
    }

    public void truncateAutoInc(long tableId) throws IOException {
        checkValidTableId(tableId);
        performMutations(Lists.<Delete>newArrayList(deleteAutoIncCounter(tableId)),
                ImmutableList.<Put>of());
    }

    public long getRowCount(long tableId) throws IOException {
        checkValidTableId(tableId);
        return getCounter(new RowsRow().encode(), serializeId(tableId));
    }

    public long incrementRowCount(long tableId, long amount) throws IOException {
        checkValidTableId(tableId);
        return incrementCounter(new RowsRow().encode(), serializeId(tableId), amount);
    }

    public void truncateRowCount(long tableId) throws IOException {
        checkValidTableId(tableId);
        performMutations(Lists.<Delete>newArrayList(deleteRowsCounter(tableId)),
                ImmutableList.<Put>of());
    }

    private Map<String, Long> getNameToIdMap(byte[] encodedRow) throws IOException {
        HTableInterface hTable = getHTable();
        try {
            Get get = new Get(encodedRow);
            get.addFamily(COLUMN_FAMILY);
            Result result = hTable.get(get);
            Map<byte[], byte[]> serializedNameIds = result.getFamilyMap(COLUMN_FAMILY);
            Map<String, Long> nameToId = new HashMap<String, Long>(serializedNameIds.size());

            for (Map.Entry<byte[], byte[]> entry : serializedNameIds.entrySet()) {
                if (entry.getKey().length > 0) {
                    nameToId.put(
                            deserializeName(entry.getKey()),
                            deserializeId(entry.getValue()));
                }
            }
            return nameToId;
        } finally {
            hTable.close();
        }
    }

    private void checkIndicesMatchColumns(Map<String, IndexSchema> indices, Map<String, ColumnSchema> columns) {
        for (IndexSchema index : indices.values()) {
            for (String column : index.getColumns()) {
                if (!columns.containsKey(column)) {
                    throw new IllegalArgumentException("Only columns in the table maybe indexed.");
                }
            }
        }
    }

    private long getCounter(byte[] row, byte[] identifier) throws IOException {
        Get get = new Get(row).addColumn(COLUMN_FAMILY, identifier);
        HTableInterface hTable = getHTable();
        try {
            byte[] value = hTable.get(get).getValue(COLUMN_FAMILY, identifier);
            return value == null ? 0 : Bytes.toLong(value);
        } finally {
            hTable.close();
        }
    }

    private long incrementCounter(byte[] row, byte[] identifier, long amount)
            throws IOException {
        HTableInterface hTable = getHTable();
        try {
            return hTable.incrementColumnValue(row, COLUMN_FAMILY, identifier, amount);
        } finally {
            hTable.close();
        }
    }

    private long getNextTableId() throws IOException {
        HTableInterface hTable = getHTable();
        try {
            return hTable.incrementColumnValue(new TablesRow().encode(),
                    COLUMN_FAMILY, new byte[0], 1);
        } finally {
            hTable.close();
        }
    }

    private long getNextIndexId(long tableId, int n) throws IOException {
        HTableInterface hTable = getHTable();
        try {
            return hTable.incrementColumnValue(new IndicesRow(tableId).encode(),
                    COLUMN_FAMILY, new byte[0], n);
        } finally {
            hTable.close();
        }
    }

    private long getNextColumnId(long tableId, int n) throws IOException {
        HTableInterface hTable = getHTable();
        try {
            return hTable.incrementColumnValue(new ColumnsRow(tableId).encode(),
                    COLUMN_FAMILY, new byte[0], n);
        } finally {
            hTable.close();
        }
    }

    private Delete deleteTableId(String tableName) {
        return new Delete(new TablesRow().encode())
                .deleteColumns(COLUMN_FAMILY, serializeName(tableName));
    }

    private Delete deleteAutoIncCounter(long tableId) {
        return new Delete(new AutoIncRow().encode())
                .deleteColumns(COLUMN_FAMILY, serializeId(tableId));
    }

    private Delete deleteRowsCounter(long tableId) {
        return new Delete(new RowsRow().encode())
                .deleteColumns(COLUMN_FAMILY, serializeId(tableId));
    }

    private Put putTableSchema(long tableId, TableSchema schema)
            throws IOException {
        return new Put(new SchemaRow().encode())
                .add(COLUMN_FAMILY, serializeId(tableId),
                        Util.serializeTableSchema(schema));
    }

    private Delete deleteTableSchema(long tableId) {
        return new Delete(new SchemaRow().encode())
                .deleteColumns(COLUMN_FAMILY, serializeId(tableId));
    }

    private Put putColumnIds(long tableId, Map<String, ColumnSchema> columns)
            throws IOException {
        long columnId = getNextColumnId(tableId, columns.size());
        Put put = new Put(new ColumnsRow(tableId).encode());

        for (Map.Entry<String, ColumnSchema> columnEntry : columns.entrySet()) {
            put.add(COLUMN_FAMILY, serializeName(columnEntry.getKey()),
                    serializeId(columnId--));
        }
        return put;
    }

    private Put putIndices(long tableId, Map<String, IndexSchema> indices) throws IOException {
        long indexId = getNextIndexId(tableId, indices.size());
        Put put = new Put(new IndicesRow(tableId).encode());

        for (Map.Entry<String, IndexSchema> columnEntry : indices.entrySet()) {
            put.add(COLUMN_FAMILY, serializeName(columnEntry.getKey()),
                    serializeId(indexId--));
        }
        return put;
    }

    private Put putTableId(String tableName, long tableId) {
        Put idPut = new Put(new TablesRow().encode());
        idPut.add(COLUMN_FAMILY, serializeName(tableName), serializeId(tableId));
        return idPut;
    }

    private byte[] serializeName(String name) {
        return name.getBytes(Charsets.UTF_8);
    }

    private String deserializeName(byte[] serializedName) {
        return new String(serializedName, Charsets.UTF_8);
    }

    private byte[] serializeId(long id) {
        return VarEncoder.encodeULong(id);
    }

    private long deserializeId(byte[] id) {
        return VarEncoder.decodeULong(id);
    }

    /**
     * Performs the operations necessary to commit the specified mutations to the underlying data store.
     * If no operations are specified then no mutations occur.
     *
     * @param deletes A list of {@link Delete} operations to execute, not null
     * @param puts    A list of  {@link Put} operations to execute, not null
     * @throws IOException Thrown on mutation commit failure
     */
    private void performMutations(final List<Delete> deletes, final List<Put> puts) throws IOException {
        Preconditions.checkNotNull(deletes, "The delete mutations container is invalid");
        Preconditions.checkNotNull(puts, "The put mutations container is invalid");
        checkArgument(!deletes.isEmpty() || !puts.isEmpty(), "At least one mutation operation must be specified");

        HTableInterface hTable = getHTable();
        try {
            if (!deletes.isEmpty()) {
                hTable.delete(deletes);
            }

            if (!puts.isEmpty()) {
                hTable.put(puts);
            }

            // Only flush if the table is not configured to auto flush
            if (!hTable.isAutoFlush()) {
                hTable.flushCommits();
            }
        } finally {
            hTable.close();
        }
    }

    private HTableInterface getHTable() {
        return this.provider.get();
    }

    private void checkValidTableId(long tableId) {
        checkArgument(tableId >= 0, "Table id must be greater than zero.");
    }
}
