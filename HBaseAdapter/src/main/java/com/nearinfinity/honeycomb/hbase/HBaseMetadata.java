package com.nearinfinity.honeycomb.hbase;

import com.google.common.base.Charsets;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.nearinfinity.honeycomb.ColumnNotFoundException;
import com.nearinfinity.honeycomb.TableNotFoundException;
import com.nearinfinity.honeycomb.hbase.rowkey.ColumnMetadataRow;
import com.nearinfinity.honeycomb.hbase.rowkey.ColumnsRow;
import com.nearinfinity.honeycomb.hbase.rowkey.TablesRow;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.gen.ColumnMetadata;
import com.nearinfinity.honeycomb.mysql.gen.TableMetadata;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages writing and reading table & column metadata to and from HBase.
 */
public class HBaseMetadata {
    private static final byte[] COLUMN_FAMILY = Constants.NIC;
    private HTableInterface hTable;
    public HBaseMetadata(HTableInterface hTable) {
        this.hTable = hTable;
    }

    public TableMetadata getTableMetadata(String table)
            throws IOException, TableNotFoundException, ColumnNotFoundException {
        long tableId = getTableId(table);
        TableMetadata tableMetadata = new TableMetadata();
        tableMetadata.setName(table);
        tableMetadata.setColumns(getColumnMetadata(tableId));
        return tableMetadata;
    }

    public void putTableMetadata(TableMetadata metadata)
            throws IOException {

        long tableId = getNextTableId();
        Put idPut = new Put(new TablesRow().encode());
        idPut.add(COLUMN_FAMILY, serializeName(metadata.getName()),
                serializeId(tableId));

        hTable.put(idPut);

        for (Map.Entry<String, ColumnMetadata> entry : metadata.getColumns().entrySet()) {
            putColumnMetadata(tableId, entry.getKey(), entry.getValue());
        }

        hTable.flushCommits();
    }

    private void putColumnMetadata(long tableId, String name, ColumnMetadata metadata)
            throws IOException {
        List<Put> puts = new ArrayList<Put>();

        long columnId = getNextColumnId(tableId);
        Put idPut = new Put(new ColumnsRow(tableId).encode());
        idPut.add(COLUMN_FAMILY, serializeName(name), serializeId(columnId));

        Put metadataPut = new Put(new ColumnMetadataRow(tableId).encode());
        metadataPut.add(COLUMN_FAMILY, serializeId(columnId),
                Util.serializeColumnMetadata(metadata));

        puts.add(idPut);
        puts.add(metadataPut);
        hTable.put(puts);
    }

    private long getNextTableId() throws IOException {
        return hTable.incrementColumnValue(new TablesRow().encode(),
                COLUMN_FAMILY, new byte[0], 1);
    }

    private long getNextColumnId(long tableId) throws IOException {
        return hTable.incrementColumnValue(new ColumnsRow(tableId).encode(),
                COLUMN_FAMILY, new byte[0], 1);
    }

    public long getTableId(String table)
            throws IOException, TableNotFoundException {
        Get get = new Get(new TablesRow().encode());
        get.addColumn(COLUMN_FAMILY, serializeName(table));
        Result result = hTable.get(get);
        byte[] tableIdBytes = result.getValue(COLUMN_FAMILY, serializeName(table));
        if (tableIdBytes == null) {
            throw new TableNotFoundException(table);
        }
        return deserializeId(tableIdBytes);
    }

    public BiMap<String, Long> getColumnIds(String tableName)
            throws IOException, TableNotFoundException {
        return getColumnIds(getTableId(tableName));
    }

    private BiMap<String, Long> getColumnIds(Long tableId)
            throws IOException {
        Get get = new Get(new ColumnsRow(tableId).encode());
        get.addFamily(COLUMN_FAMILY);
        Result result = hTable.get(get);
        Map<byte[], byte[]> serializedColumnIds =
                result.getFamilyMap(COLUMN_FAMILY);
        Map<String, Long> columnIds = new HashMap<String, Long>();
        for (Map.Entry<byte[], byte[]> entry : serializedColumnIds.entrySet()) {
            if (entry.getKey().length > 0) {
                columnIds.put(
                        deserializeName(entry.getKey()),
                        deserializeId(entry.getValue()));
            }
        }
        return ImmutableBiMap.copyOf(columnIds);
    }

    private Map<String, ColumnMetadata> getColumnMetadata(Long tableId)
            throws IOException, ColumnNotFoundException {
        Get get = new Get(new ColumnMetadataRow(tableId).encode());
        get.addFamily(COLUMN_FAMILY);
        Result result = hTable.get(get);
        Map<byte[], byte[]> rawMetadata = result.getFamilyMap(COLUMN_FAMILY);

        Map<String, Long> columnIds = getColumnIds(tableId);

        Map<String, ColumnMetadata> columnMetadata = new HashMap<String, ColumnMetadata>();
        byte[] serializedMetadata;
        for(Map.Entry<String, Long> columnEntry : columnIds.entrySet()) {
            serializedMetadata = rawMetadata.get(serializeId(columnEntry.getValue()));
            if (serializedMetadata == null) {
                throw new ColumnNotFoundException("Column metadata for " +
                        columnEntry.getKey() + " not found.");
            }
            columnMetadata.put(columnEntry.getKey(),
                    Util.deserializeColumnMetadata(serializedMetadata));
        }
        return ImmutableMap.copyOf(columnMetadata);
    }

    public void deleteColumnMetadata(String tableName)
            throws TableNotFoundException, IOException {
        long tableId = getTableId(tableName);
        List<Delete> deletes = new ArrayList<Delete>();

        Delete tableIdDelete = new Delete(new TablesRow().encode());
        tableIdDelete.deleteColumn(COLUMN_FAMILY, serializeName(tableName));

        Delete columnsDelete = new Delete(new ColumnsRow(tableId).encode());
        Delete columnMetadataDelete = new Delete(new ColumnMetadataRow(tableId).encode());

        deletes.add(tableIdDelete);
        deletes.add(columnsDelete);
        deletes.add(columnMetadataDelete);

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