package com.nearinfinity.honeycomb.hbase;

import com.google.common.base.Charsets;
import com.google.common.collect.*;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.nearinfinity.honeycomb.config.Constants;
import com.nearinfinity.honeycomb.exceptions.TableNotFoundException;
import com.nearinfinity.honeycomb.hbase.rowkey.*;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.Verify;
import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.*;

/**
 * Manages writing and reading table & column schemas, table & column ids, and
 * row & autoincrement counters to and from HBase.
 */
public class HBaseMetadata {
    private static final byte[] COLUMN_FAMILY = Constants.DEFAULT_COLUMN_FAMILY;
    private final Provider<HTableInterface> provider;

    @Inject
    public HBaseMetadata(HTableProvider provider) {
        checkNotNull(provider);
        this.provider = provider;
    }

    public long getTableId(String table) {
        Verify.isNotNullOrEmpty(table);
        Get get = new Get(new TablesRow().encode());
        byte[] serializedName = serializeName(table);
        get.addColumn(COLUMN_FAMILY, serializedName);
        HTableInterface hTable = getHTable();
        try {
            Result result = HBaseOperations.performGet(hTable, get);

            byte[] tableIdBytes = result.getValue(COLUMN_FAMILY, serializedName);
            if (tableIdBytes == null) {
                throw new TableNotFoundException(table);
            }
            return deserializeId(tableIdBytes);
        } finally {
            HBaseOperations.closeTable(hTable);
        }
    }

    public Map<String, Long> getIndexIds(long tableId) {
        Verify.isValidTableId(tableId);
        return getNameToIdMap(new IndicesRow(tableId).encode());
    }

    public BiMap<String, Long> getColumnIds(long tableId) {
        Verify.isValidTableId(tableId);
        Map<String, Long> nameToId = getNameToIdMap(new ColumnsRow(tableId).encode());
        return ImmutableBiMap.copyOf(nameToId);
    }

    public TableSchema getSchema(long tableId) {
        Verify.isValidTableId(tableId);
        byte[] serializedTableId = serializeId(tableId);

        Get get = new Get(new SchemaRow().encode());
        get.addColumn(COLUMN_FAMILY, serializedTableId);
        HTableInterface hTable = getHTable();
        try {
            Result result = HBaseOperations.performGet(hTable, get);

            byte[] serializedSchema = result.getValue(COLUMN_FAMILY, serializedTableId);
            if (serializedSchema == null) {
                throw new TableNotFoundException(tableId);
            }
            return Util.deserializeTableSchema(serializedSchema);
        } finally {
            HBaseOperations.closeTable(hTable);
        }
    }

    public void createTable(String tableName, TableSchema schema) {
        Verify.isNotNullOrEmpty(tableName);
        checkNotNull(schema);
        long tableId = getNextTableId();
        List<Put> puts = new ArrayList<Put>();
        puts.add(putTableId(tableName, tableId));
        puts.add(putColumnIds(tableId, schema.getColumns()));
        puts.add(putTableSchema(tableId, schema));
        if (!schema.getIndices().isEmpty()) {
            puts.add(putIndices(tableId, schema.getIndices()));
        }
        performMutations(ImmutableList.<Delete>of(), puts);
    }

    /**
     * Performs all metadata operations necessary to create a table index
     *
     * @param tableId The id of the table to create the index
     * @param indexName The identifying name of the index, not null or empty
     * @param indexSchema The {@link IndexSchema} representing the index details, not null
     */
    public void createTableIndex(final long tableId, final String indexName,
            final IndexSchema indexSchema) {
        Verify.isNotNullOrEmpty(indexName, "The index name is invalid");
        checkNotNull(indexSchema, "The index schema is invalid");

        final List<Put> puts = Lists.newArrayList();
        final List<Delete> deletes = Lists.newArrayList();

        final Map<String, IndexSchema> indexDetailMap = ImmutableMap.<String, IndexSchema>of(indexName, indexSchema);

        // Update the table schema to store the new index schema details
        final TableSchema existingSchema = getSchema(tableId);
        final TableSchema updatedSchema = TableSchema.newBuilder(existingSchema).build();
        updatedSchema.getIndices().putAll(indexDetailMap);

        // Delete the previous table schema
        deletes.add(deleteTableSchema(tableId));

        // Write the updated table schema and created index
        puts.add(putTableSchema(tableId, updatedSchema));
        puts.add(putIndices(tableId, indexDetailMap));

        performMutations(deletes, puts);
    }

    public void deleteTable(String tableName) {
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

    /**
     * Performs the operations necessary to rename an existing table stored in a {@link TablesRow} row
     *
     * @param oldTableName The name of the existing table
     * @param newTableName The new name to use for this table
     * @throws TableNotFoundException Thrown when existing table cannot be found
     */
    public void renameExistingTable(final String oldTableName, final String newTableName) {
        Verify.isNotNullOrEmpty(oldTableName, "Old table name must have value");
        Verify.isNotNullOrEmpty(newTableName, "New table name must have value");

        final long tableId = getTableId(oldTableName);

        List<Delete> deletes = Lists.newArrayList(deleteTableId(oldTableName));
        List<Put> puts = Lists.newArrayList(putTableId(newTableName, tableId));

        performMutations(deletes, puts);
    }

    public long getAutoInc(long tableId) {
        Verify.isValidTableId(tableId);
        return getCounter(new AutoIncRow().encode(), serializeId(tableId));
    }

    public long incrementAutoInc(long tableId, long amount) {
        Verify.isValidTableId(tableId);
        return incrementCounter(new AutoIncRow().encode(),
                serializeId(tableId), amount);
    }

    public void setAutoInc(long tableId, long value) {
        Verify.isValidTableId(tableId);
        Put put = new Put(new AutoIncRow().encode());
        put.add(COLUMN_FAMILY, serializeId(tableId), Bytes.toBytes(value));
        HTableInterface hTable = getHTable();
        try {
            HBaseOperations.performPut(hTable, put);
        } finally {
            HBaseOperations.closeTable(hTable);
        }
    }

    public long getRowCount(long tableId) {
        Verify.isValidTableId(tableId);
        return getCounter(new RowsRow().encode(), serializeId(tableId));
    }

    public long incrementRowCount(long tableId, long amount) {
        Verify.isValidTableId(tableId);
        return incrementCounter(new RowsRow().encode(), serializeId(tableId), amount);
    }

    public void truncateRowCount(long tableId) {
        Verify.isValidTableId(tableId);
        performMutations(Lists.<Delete>newArrayList(deleteRowsCounter(tableId)),
                ImmutableList.<Put>of());
    }

    private Map<String, Long> getNameToIdMap(byte[] encodedRow) {
        HTableInterface hTable = getHTable();
        try {
            Get get = new Get(encodedRow);
            get.addFamily(COLUMN_FAMILY);
            Result result = HBaseOperations.performGet(hTable, get);
            if (result.isEmpty()) {
                return Maps.newHashMap();
            }

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
            HBaseOperations.closeTable(hTable);
        }
    }

    private long getCounter(byte[] row, byte[] identifier) {
        Get get = new Get(row).addColumn(COLUMN_FAMILY, identifier);
        HTableInterface hTable = getHTable();
        try {
            byte[] value = HBaseOperations.performGet(hTable, get).getValue(COLUMN_FAMILY, identifier);
            return value == null ? 0 : Bytes.toLong(value);
        } finally {
            HBaseOperations.closeTable(hTable);
        }
    }

    private long incrementCounter(byte[] row, byte[] identifier, long amount) {
        HTableInterface hTable = getHTable();
        try {
            return HBaseOperations.performIncrementColumnValue(hTable, row, COLUMN_FAMILY, identifier, amount);
        } finally {
            HBaseOperations.closeTable(hTable);
        }
    }

    private long getNextTableId() {
        HTableInterface hTable = getHTable();
        try {
            return HBaseOperations.performIncrementColumnValue(hTable, new TablesRow().encode(),
                    COLUMN_FAMILY, new byte[0], 1);
        } finally {
            HBaseOperations.closeTable(hTable);
        }
    }

    private long getNextIndexId(long tableId, int n) {
        HTableInterface hTable = getHTable();
        try {
            return HBaseOperations.performIncrementColumnValue(hTable, new IndicesRow(tableId).encode(),
                    COLUMN_FAMILY, new byte[0], n);
        } finally {
            HBaseOperations.closeTable(hTable);
        }
    }

    private long getNextColumnId(long tableId, int n) {
        HTableInterface hTable = getHTable();
        try {
            return HBaseOperations.performIncrementColumnValue(hTable, new ColumnsRow(tableId).encode(),
                    COLUMN_FAMILY, new byte[0], n);
        } finally {
            HBaseOperations.closeTable(hTable);
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

    private Put putTableSchema(long tableId, TableSchema schema) {
        return new Put(new SchemaRow().encode())
                .add(COLUMN_FAMILY, serializeId(tableId),
                        Util.serializeTableSchema(schema));
    }

    private Delete deleteTableSchema(long tableId) {
        return new Delete(new SchemaRow().encode())
                .deleteColumns(COLUMN_FAMILY, serializeId(tableId));
    }

    private Put putColumnIds(long tableId, Map<String, ColumnSchema> columns) {
        long columnId = getNextColumnId(tableId, columns.size());
        Put put = new Put(new ColumnsRow(tableId).encode());

        for (Map.Entry<String, ColumnSchema> columnEntry : columns.entrySet()) {
            put.add(COLUMN_FAMILY, serializeName(columnEntry.getKey()),
                    serializeId(columnId--));
        }
        return put;
    }

    private Put putIndices(long tableId, Map<String, IndexSchema> indices) {
        checkState(!indices.isEmpty(), "putIndices requires 1 or more indices.");
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
     */
    private void performMutations(final List<Delete> deletes, final List<Put> puts) {
        checkNotNull(deletes, "The delete mutations container is invalid");
        checkNotNull(puts, "The put mutations container is invalid");
        checkArgument(!deletes.isEmpty() || !puts.isEmpty(), "At least one mutation operation must be specified");

        HTableInterface hTable = getHTable();
        try {
            if (!deletes.isEmpty()) {
                HBaseOperations.performDelete(hTable, deletes);
            }

            if (!puts.isEmpty()) {
                HBaseOperations.performPut(hTable, puts);
            }

            // Only flush if the table is not configured to auto flush
            if (!hTable.isAutoFlush()) {
                HBaseOperations.performFlush(hTable);
            }
        } finally {
            HBaseOperations.closeTable(hTable);
        }
    }

    private HTableInterface getHTable() {
        return provider.get();
    }
}
