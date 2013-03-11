package com.nearinfinity.honeycomb.hbase;

import java.io.IOException;
import java.util.ArrayList;
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
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.nearinfinity.honeycomb.TableNotFoundException;
import com.nearinfinity.honeycomb.hbase.rowkey.AutoIncRow;
import com.nearinfinity.honeycomb.hbase.rowkey.ColumnsRow;
import com.nearinfinity.honeycomb.hbase.rowkey.RowsRow;
import com.nearinfinity.honeycomb.hbase.rowkey.SchemaRow;
import com.nearinfinity.honeycomb.hbase.rowkey.TablesRow;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;

/**
 * Manages writing and reading table & column schemas, table & column ids, and
 * row & autoincrement counters to and from HBase.
 */
public class HBaseMetadata {
    private static final byte[] COLUMN_FAMILY = Constants.NIC;
    private final HTableInterface hTable;

    public HBaseMetadata(HTableInterface hTable) {
        this.hTable = hTable;
    }

    public long getTableId(String table)
            throws IOException, TableNotFoundException {
        Get get = new Get(new TablesRow().encode());
        byte[] serializedName = serializeName(table);
        get.addColumn(COLUMN_FAMILY, serializedName);
        Result result = hTable.get(get);
        byte[] tableIdBytes = result.getValue(COLUMN_FAMILY, serializedName);
        if (tableIdBytes == null) {
            throw new TableNotFoundException(table);
        }
        return deserializeId(tableIdBytes);
    }

    public BiMap<String, Long> getColumnIds(long tableId)
            throws IOException {
        Get get = new Get(new ColumnsRow(tableId).encode());
        get.addFamily(COLUMN_FAMILY);
        Result result = hTable.get(get);

        Map<byte[], byte[]> serializedColumnIds = result.getFamilyMap(COLUMN_FAMILY);
        Map<String, Long> columnIds = new HashMap<String, Long>(serializedColumnIds.size());

        for (Map.Entry<byte[], byte[]> entry : serializedColumnIds.entrySet()) {
            if (entry.getKey().length > 0) {
                columnIds.put(
                        deserializeName(entry.getKey()),
                        deserializeId(entry.getValue()));
            }
        }
        return ImmutableBiMap.copyOf(columnIds);
    }

    public TableSchema getSchema(long tableId)
            throws IOException, TableNotFoundException {
        byte[] serializedTableId = serializeId(tableId);

        Get get = new Get(new SchemaRow().encode());
        get.addColumn(COLUMN_FAMILY, serializedTableId);
        Result result = hTable.get(get);
        byte[] serializedSchema = result.getValue(COLUMN_FAMILY, serializedTableId);
        if (serializedSchema == null) {
            throw new TableNotFoundException(tableId);
        }
        return Util.deserializeTableSchema(serializedSchema);
    }

    public void putSchema(String tableName, TableSchema schema)
            throws IOException {
        long tableId = getNextTableId();
        List<Put> puts = new ArrayList<Put>();
        puts.add(putTableId(tableName, tableId));
        puts.add(putColumnIds(tableId, schema.getColumns()));
        puts.add(putTableSchema(tableId, schema));

        performMutations(ImmutableList.<Delete>of(), puts);
    }


    public void deleteSchema(String tableName)
            throws TableNotFoundException, IOException {
        long tableId = getTableId(tableName);
        byte[] serializedId = serializeId(tableId);

        List<Delete> deletes = new ArrayList<Delete>();

        Delete columnIdsDelete = new Delete(new ColumnsRow(tableId).encode());
        // commented out because MockHTable seems to be broken
        // columnIdsDelete.deleteFamily(COLUMN_FAMILY);

        Delete rowsDelete = new Delete(new RowsRow().encode());
        rowsDelete.deleteColumn(COLUMN_FAMILY, serializedId);

        deletes.add(deleteTableId(tableName));
        deletes.add(columnIdsDelete);
        deletes.add(rowsDelete);
        deletes.add(deleteAutoIncCounter(tableId));
        deletes.add(deleteTableSchema(tableId));

        performMutations(deletes, ImmutableList.<Put>of());
    }

    public void updateSchema(long tableId, TableSchema oldSchema, TableSchema newSchema)
            throws IOException, TableNotFoundException {
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

            if (schema.getIsAutoincrement()) {
                deletes.add(deleteAutoIncCounter(tableId));
            }
        }

        puts.add(putColumnIds(tableId, diff.entriesOnlyOnRight())); // New columns

        for (Map.Entry<String, MapDifference.ValueDifference<ColumnSchema>> changedColumn :
                diff.entriesDiffering().entrySet()) {
            if (changedColumn.getValue().leftValue().getIsAutoincrement()
                    && !changedColumn.getValue().rightValue().getIsAutoincrement()) {
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
     * @param oldTableName The name of the existing table
     * @param newTableName The new name to use for this table
     * @throws IOException Thrown on HBase mutation failure
     * @throws TableNotFoundException Thrown when existing table cannot be found
     */
    public void renameExistingTable(final String oldTableName, final String newTableName) throws IOException, TableNotFoundException {
        Preconditions.checkNotNull(oldTableName, "The current table name provided is invalid");
        Preconditions.checkArgument(!oldTableName.isEmpty(), "The current table name provided cannot be empty");
        Preconditions.checkNotNull(newTableName, "The new table name provided is invalid");
        Preconditions.checkArgument(!newTableName.isEmpty(), "The new table name provided cannot be empty");

        final long tableId = getTableId(oldTableName);

        List<Delete> deletes = ImmutableList.of(deleteTableId(oldTableName));
        List<Put> puts = ImmutableList.of(putTableId(newTableName, tableId));

        performMutations(deletes, puts);
    }


    public long getAutoInc(long tableId) throws IOException {
        return getCounter(new AutoIncRow().encode(), serializeId(tableId));
    }

    public long incrementAutoInc(long tableId, long amount) throws IOException {
        return incrementCounter(new AutoIncRow().encode(),
                serializeId(tableId), amount);
    }

    public void truncateAutoInc(long tableId) throws IOException {
        performMutations(ImmutableList.<Delete>of(deleteAutoIncCounter(tableId)),
                ImmutableList.<Put>of());
    }

    public long getRowCount(long tableId) throws IOException {
        return getCounter(new RowsRow().encode(), serializeId(tableId));
    }

    public long incrementRowCount(long tableId, long amount) throws IOException {
        return incrementCounter(new RowsRow().encode(), serializeId(tableId), amount);
    }

    public void truncateRowCount(long tableId) throws IOException {
        performMutations(ImmutableList.<Delete>of(deleteRowsCounter(tableId)),
                ImmutableList.<Put>of());
    }

    private long getCounter(byte[] row, byte[] identifier) throws IOException {
        Get get = new Get(row).addColumn(COLUMN_FAMILY, identifier);
        byte[] value = hTable.get(get).getValue(COLUMN_FAMILY, identifier);
        return value == null ? 0 : Bytes.toLong(value);
    }

    private long incrementCounter(byte[] row, byte[] identifier, long amount)
            throws IOException {
        return hTable.incrementColumnValue(row, COLUMN_FAMILY, identifier, amount);
    }

    private long getNextTableId() throws IOException {
        return hTable.incrementColumnValue(new TablesRow().encode(),
                COLUMN_FAMILY, new byte[0], 1);
    }

    private long getNextColumnId(long tableId, int n) throws IOException {
        return hTable.incrementColumnValue(new ColumnsRow(tableId).encode(),
                COLUMN_FAMILY, new byte[0], n);
    }

    private Delete deleteTableId(String tableName) {
        return new Delete(new TablesRow().encode())
                .deleteColumn(COLUMN_FAMILY, serializeName(tableName));
    }

    private Delete deleteAutoIncCounter(long tableId) {
        return new Delete(new AutoIncRow().encode())
                .deleteColumn(COLUMN_FAMILY, serializeId(tableId));
    }

    private Delete deleteRowsCounter(long tableId) {
        return new Delete(new RowsRow().encode())
                .deleteColumn(COLUMN_FAMILY, serializeId(tableId));
    }

    private Put putTableSchema(long tableId, TableSchema schema)
            throws IOException {
        return new Put(new SchemaRow().encode())
                .add(COLUMN_FAMILY, serializeId(tableId),
                        Util.serializeTableSchema(schema));
    }

    private Delete deleteTableSchema(long tableId) {
        return new Delete(new SchemaRow().encode())
                .deleteColumn(COLUMN_FAMILY, serializeId(tableId));
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
     * @param deletes A list of {@link Delete} operations to execute, not null
     * @param puts A list of  {@link Put} operations to execute, not null
     * @throws IOException Thrown on mutation commit failure
     */
    private void performMutations(final List<Delete> deletes, final List<Put> puts) throws IOException {
        Preconditions.checkNotNull(deletes, "The delete mutations container is invalid");
        Preconditions.checkNotNull(puts, "The put mutations container is invalid");
        Preconditions.checkArgument(!deletes.isEmpty() || !puts.isEmpty(), "At least one mutation operation must be specified");

        if( !deletes.isEmpty() ) {
            hTable.delete(deletes);
        }

        if( !puts.isEmpty() ) {
            hTable.put(puts);
        }

        // Only flush if the table is not configured to auto flush
        if( !hTable.isAutoFlush() ) {
            hTable.flushCommits();
        }
    }
}
