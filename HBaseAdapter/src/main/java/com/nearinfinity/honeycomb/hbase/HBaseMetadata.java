package com.nearinfinity.honeycomb.hbase;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Charsets;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.nearinfinity.honeycomb.config.Constants;
import com.nearinfinity.honeycomb.exceptions.TableNotFoundException;
import com.nearinfinity.honeycomb.hbase.rowkey.AutoIncRow;
import com.nearinfinity.honeycomb.hbase.rowkey.ColumnsRow;
import com.nearinfinity.honeycomb.hbase.rowkey.IndicesRow;
import com.nearinfinity.honeycomb.hbase.rowkey.RowsRow;
import com.nearinfinity.honeycomb.hbase.rowkey.SchemaRow;
import com.nearinfinity.honeycomb.hbase.rowkey.TablesRow;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.IndexSchema;
import com.nearinfinity.honeycomb.mysql.TableSchema;
import com.nearinfinity.honeycomb.util.Verify;

/**
 * Manages writing and reading table & column schemas, table & column ids, and
 * row & autoincrement counters to and from HBase.
 */
public class HBaseMetadata {
    private static final byte[] COLUMN_FAMILY = Constants.DEFAULT_COLUMN_FAMILY;
    private final Provider<HTableInterface> provider;

    @Inject
    public HBaseMetadata(final HTableProvider provider) {
        checkNotNull(provider);

        this.provider = provider;
    }

    /**
     * Fetches the table identifier for the specified table name from the underlying
     * data store
     *
     * @param tableName The name of the table this lookup is for, not null or empty
     * @return The table identifier stored for the table name
     */
    public long getTableId(final String tableName) {
        Verify.isNotNullOrEmpty(tableName);

        final byte[] serializedName = serializeName(tableName);
        final Get get = new Get(new TablesRow().encode());
        get.addColumn(COLUMN_FAMILY, serializedName);

        final HTableInterface hTable = getHTable();

        try {
            final Result result = HBaseOperations.performGet(hTable, get);

            final byte[] tableIdBytes = result.getValue(COLUMN_FAMILY, serializedName);
            if( tableIdBytes == null ) {
                throw new TableNotFoundException(tableName);
            }

            return deserializeId(tableIdBytes);
        } finally {
            HBaseOperations.closeTable(hTable);
        }
    }

    /**
     * Fetches the index name to index identifier mappings for the table corresponding
     * to the specified table identifier
     *
     * @param tableId The valid table identifier of the table this lookup is for
     * @return The indices mapping details for the table
     */
    public Map<String, Long> getIndexIds(final long tableId) {
        Verify.isValidId(tableId);

        return getNameToIdMap(new IndicesRow(tableId).encode());
    }

    /**
     * Fetches the column name to column identifier mappings for the table corresponding
     * to the specified table identifier
     *
     * @param tableId The valid table identifier of the table this lookup is for
     * @return The columns mapping details for the table
     */
    public BiMap<String, Long> getColumnIds(final long tableId) {
        Verify.isValidId(tableId);

        final Map<String, Long> nameToId = getNameToIdMap(new ColumnsRow(tableId).encode());
        return ImmutableBiMap.copyOf(nameToId);
    }

    /**
     * Fetches the {@link TableSchema} for the table corresponding to the specified table identifier
     *
     * @param tableId The valid table identifier of the table this lookup is for
     * @return The table schema details for the table
     */
    public TableSchema getSchema(final long tableId) {
        Verify.isValidId(tableId);

        final byte[] serializedTableId = serializeId(tableId);
        final Get get = new Get(new SchemaRow().encode());
        get.addColumn(COLUMN_FAMILY, serializedTableId);

        final HTableInterface hTable = getHTable();

        try {
            final Result result = HBaseOperations.performGet(hTable, get);

            final byte[] serializedSchema = result.getValue(COLUMN_FAMILY, serializedTableId);
            if (serializedSchema == null) {
                throw new TableNotFoundException(tableId);
            }

            return Util.deserializeTableSchema(serializedSchema);
        } finally {
            HBaseOperations.closeTable(hTable);
        }
    }

    /**
     * Performs all metadata operations necessary to create a table
     *
     * @param tableName The name of the table to create, not null or empty
     * @param schema The schema details of the table to create, not null
     */
    public void createTable(final String tableName, final TableSchema schema) {
        Verify.isNotNullOrEmpty(tableName);
        checkNotNull(schema);

        final List<Put> puts = Lists.newArrayList();

        // Fetch the next table id to use for this table
        final long tableId = getNextTableId();

        puts.add(putTableId(tableName, tableId));
        puts.add(putColumnIds(tableId, schema.getColumns()));
        puts.add(putTableSchema(tableId, schema));

        if( !schema.getIndices().isEmpty() ) {
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
        Verify.isValidId(tableId);
        Verify.isNotNullOrEmpty(indexName, "The index name is invalid");
        checkNotNull(indexSchema, "The index schema is invalid");

        final List<Put> puts = Lists.newArrayList();

        final Map<String, IndexSchema> indexDetailMap = ImmutableMap.<String, IndexSchema>of(indexName, indexSchema);

        // Update the table schema to store the new index schema details
        final TableSchema existingSchema = getSchema(tableId);
        final TableSchema updatedSchema = TableSchema.newBuilder(existingSchema).build();
        updatedSchema.getIndices().putAll(indexDetailMap);

        // Write the updated table schema and created index
        puts.add(putTableSchema(tableId, updatedSchema));
        puts.add(putIndices(tableId, indexDetailMap));

        performMutations(ImmutableList.<Delete>of(), puts);
    }

    /**
     * Performs all metadata operations necessary to remove the specified index from the specified table
     *
     * @param tableId The id of the table to create the index
     * @param indexName The identifying name of the index, not null or empty
     */
    public void deleteTableIndex(final long tableId, final String indexName) {
        Verify.isValidId(tableId);
        Verify.isNotNullOrEmpty(indexName, "The index name is invalid");

        final List<Put> puts = Lists.newArrayList();
        final List<Delete> deletes = Lists.newArrayList();

        // Update the table schema to remove index schema details
        final TableSchema existingSchema = getSchema(tableId);
        final TableSchema updatedSchema = TableSchema.newBuilder(existingSchema).build();
        updatedSchema.getIndices().remove(indexName);

        // Delete the old index
        deletes.add(generateIndexDelete(tableId, indexName));

        // Write the updated table schema
        puts.add(putTableSchema(tableId, updatedSchema));

        performMutations(deletes, puts);
    }

    /**
     * Performs all metadata operations necessary to delete the specified table
     *
     * @param tableName The name of the table to delete, not null or empty
     */
    public void deleteTable(String tableName) {
        Verify.isNotNullOrEmpty(tableName);

        List<Delete> deletes = Lists.newArrayList();

        final long tableId = getTableId(tableName);
        final byte[] serializedId = serializeId(tableId);

        final Delete columnIdsDelete = new Delete(new ColumnsRow(tableId).encode());
        final Delete indicesIdsDelete = new Delete(new IndicesRow(tableId).encode());

        final Delete rowsDelete = new Delete(new RowsRow().encode());
        rowsDelete.deleteColumns(COLUMN_FAMILY, serializedId);

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
        Verify.isValidId(tableId);
        return getCounter(new AutoIncRow().encode(), serializeId(tableId));
    }

    public long incrementAutoInc(long tableId, long amount) {
        Verify.isValidId(tableId);
        return incrementCounter(new AutoIncRow().encode(),
                serializeId(tableId), amount);
    }

    public void setAutoInc(long tableId, long value) {
        Verify.isValidId(tableId);
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
        Verify.isValidId(tableId);
        return getCounter(new RowsRow().encode(), serializeId(tableId));
    }

    public long incrementRowCount(long tableId, long amount) {
        Verify.isValidId(tableId);
        return incrementCounter(new RowsRow().encode(), serializeId(tableId), amount);
    }

    public void truncateRowCount(long tableId) {
        Verify.isValidId(tableId);
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

    private long incrementCounter(final byte[] row, final byte[] identifier, final long amount) {
        final HTableInterface hTable = getHTable();

        try {
            return HBaseOperations.performIncrementColumnValue(hTable, row, COLUMN_FAMILY, identifier, amount);
        } finally {
            HBaseOperations.closeTable(hTable);
        }
    }

    private long getNextTableId() {
        return incrementCounter(new TablesRow().encode(), new byte[0], 1);
    }

    private long getNextIndexId(final long tableId, final int n) {
        return incrementCounter(new IndicesRow(tableId).encode(), new byte[0], n);
    }

    private long getNextColumnId(final long tableId, final int n) {
        return incrementCounter(new ColumnsRow(tableId).encode(), new byte[0], n);
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

    private Delete generateIndexDelete(final long tableId, final String indexName) {
        return new Delete(new IndicesRow(tableId).encode())
            .deleteColumns(COLUMN_FAMILY, serializeName(indexName));
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
