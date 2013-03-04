package com.nearinfinity.honeycomb.hbase;

import com.google.common.base.Charsets;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.nearinfinity.honeycomb.TableNotFoundException;
import com.nearinfinity.honeycomb.hbase.rowkey.*;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages writing and reading table & column schemas, table & column ids, and
 * row & autoincrement counters to and from HBase.
 */
public class HBaseMetadata {
    private static final byte[] COLUMN_FAMILY = Constants.NIC;
    private HTableInterface hTable;
    public HBaseMetadata(HTableInterface hTable) {
        this.hTable = hTable;
    }

    public TableSchema getSchema(String tableName)
            throws IOException, TableNotFoundException {
        Long tableId = getTableId(tableName);
        byte[] serializedTableId = serializeId(tableId);

        Get get = new Get(new SchemaRow().encode());
        get.addColumn(COLUMN_FAMILY, serializedTableId);
        Result result = hTable.get(get);
        byte[] serializedSchema = result.getValue(COLUMN_FAMILY, serializedTableId);
        if (serializedSchema == null) { throw new TableNotFoundException(tableName); }
        return Util.deserializeTableSchema(serializedSchema);
    }

    public void putSchema(TableSchema schema)
            throws IOException {
        long tableId = getNextTableId();
        Map<String, ColumnSchema> columns = schema.getColumns();
        long columnId = getNextColumnId(tableId, columns.size());
        List<Put> puts = new ArrayList<Put>();

        Put idPut = new Put(new TablesRow().encode());
        idPut.add(COLUMN_FAMILY, serializeName(schema.getName()), serializeId(tableId));
        puts.add(idPut);

        Put columnIdPut = new Put(new ColumnsRow(tableId).encode());

        for (Map.Entry<String, ColumnSchema> columnEntry : columns.entrySet()) {
            columnIdPut.add(COLUMN_FAMILY, serializeName(columnEntry.getKey()),
                    serializeId(columnId--));
        }
        puts.add(columnIdPut);

        Put schemaPut = new Put(new SchemaRow().encode());
        schemaPut.add(COLUMN_FAMILY, serializeId(tableId),
                Util.serializeTableSchema(schema));
        puts.add(schemaPut);

        hTable.put(puts);
        hTable.flushCommits();
    }

    private long getNextTableId() throws IOException {
        return hTable.incrementColumnValue(new TablesRow().encode(),
                COLUMN_FAMILY, new byte[0], 1);
    }

    private long getNextColumnId(long tableId, int n) throws IOException {
        return hTable.incrementColumnValue(new ColumnsRow(tableId).encode(),
                COLUMN_FAMILY, new byte[0], n);
    }

    public long getTableId(String table)
            throws IOException, TableNotFoundException {
        Get get = new Get(new TablesRow().encode());
        byte[] serializedName = serializeName(table);
        get.addColumn(COLUMN_FAMILY, serializedName);
        Result result = hTable.get(get);
        byte[] tableIdBytes = result.getValue(COLUMN_FAMILY, serializedName);
        if (tableIdBytes == null) { throw new TableNotFoundException(table); }
        return deserializeId(tableIdBytes);
    }

    public BiMap<String, Long> getColumnIds(String tableName)
            throws IOException, TableNotFoundException {
        Long tableId = getTableId(tableName);
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

    public void deleteSchema(String tableName)
            throws TableNotFoundException, IOException {
        long tableId = getTableId(tableName);
        byte[] serializedId = serializeId(tableId);

        List<Delete> deletes = new ArrayList<Delete>();

        Delete tableIdDelete = new Delete(new TablesRow().encode());
        tableIdDelete.deleteColumn(COLUMN_FAMILY, serializeName(tableName));

        Delete columnIdsDelete = new Delete(new ColumnsRow(tableId).encode());
        // commented out because MockHTable seems to be broken
        // columnIdsDelete.deleteFamily(COLUMN_FAMILY);

        Delete rowsDelete = new Delete(new RowsRow().encode());
        tableIdDelete.deleteColumn(COLUMN_FAMILY, serializedId);

        Delete autoIncDelete = new Delete(new AutoIncRow().encode());
        autoIncDelete.deleteColumn(COLUMN_FAMILY, serializedId);

        Delete schemaDelete = new Delete(new SchemaRow().encode());
        schemaDelete.deleteColumn(COLUMN_FAMILY, serializedId);

        hTable.delete(tableIdDelete);
        hTable.delete(columnIdsDelete);
        hTable.delete(rowsDelete);
        hTable.delete(autoIncDelete);
        hTable.delete(schemaDelete);

        hTable.delete(deletes);
        hTable.flushCommits();
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
}