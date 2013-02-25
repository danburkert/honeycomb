package com.nearinfinity.honeycomb.hbaseclient;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.hbase.ResultReader;
import com.nearinfinity.honeycomb.hbaseclient.strategy.PrefixScanStrategy;
import com.nearinfinity.honeycomb.hbaseclient.strategy.ScanStrategy;
import com.nearinfinity.honeycomb.hbaseclient.strategy.ScanStrategyInfo;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysqlengine.HBaseResultScanner;
import com.nearinfinity.honeycomb.mysqlengine.SingleResultScanner;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class HBaseReader {
    private static final Logger logger = Logger.getLogger(HBaseReader.class);
    private final HTableInterface table;

    public HBaseReader(HTableInterface table) {
        this.table = table;
    }

    /**
     * Retrieve a SQL row from HBase and move cursor forward.
     *
     * @param tableName SQL table name
     * @param scanner   Cursor to a record in HBase
     * @return Row object, or null if no more rows
     * @throws IOException
     */
    public Row nextRow(String tableName, HBaseResultScanner scanner) throws IOException {
        Result result = scanner.next(null);
        if (result == null) {
            return null;
        }

        TableInfo info = getTableInfo(tableName);
        return ResultReader.readDataRow(result, info);
    }

    /**
     * Retrieves a SQL row based on a unique identifier.
     *
     *
     * @param uuid      Data row unique identifier
     * @param tableName SQL table name
     * @return Row object, or null if row with UUID does not exist
     * @throws IOException
     */
    public Row getDataRow(UUID uuid, String tableName) throws IOException {
        TableInfo info = getTableInfo(tableName);
        long tableId = info.getId();

        byte[] rowKey = RowKeyFactory.buildDataKey(tableId, uuid);

        Get get = new Get(rowKey);
        Result result = table.get(get);
        if (result.getRow() == null) {
            return null;
        }
        return ResultReader.readDataRow(result, info);
    }

    /**
     * Checks that the SQL row would not violate a unique index on update.
     *
     * @param tableName      SQL table name
     * @param values         SQL row to check
     * @param changedColumns Columns that will change on update
     * @return Columns that violate unique index
     * @throws IOException
     */
    public String findDuplicateKeyOnUpdate(final String tableName, final Map<String, byte[]> values, List<String> changedColumns) throws IOException {
        if (values.isEmpty()) {
            return null;
        }

        Map<String, byte[]> subMap = selectKeys(values, changedColumns);
        return findDuplicateKey(tableName, subMap);
    }

    /**
     * Checks that the SQL row would not violate a unique index on insert.
     *
     * @param tableName SQL table name
     * @param values    SQL row to check
     * @return Columns that violate unique index
     * @throws IOException
     */
    public String findDuplicateKey(final String tableName, final Map<String, byte[]> values) throws IOException {
        TableInfo info = getTableInfo(tableName);
        List<List<String>> allUniqueKeys = Index.uniqueKeysForTable(info.tableMetadata());
        for (List<String> uniqueKeySet : allUniqueKeys) {
            Map<String, byte[]> subKeyMap = selectKeys(values, uniqueKeySet);
            if (subKeyMap.size() == 0) {
                continue;
            }

            String duplicateFound = doFindDuplicateKey(tableName, subKeyMap);
            if (duplicateFound != null) {
                return duplicateFound;
            }
        }

        return null;
    }

    /**
     * Checks that adding a unique index on an existing table would not have duplicates.
     *
     * @param tableName         SQL table name
     * @param columnNameStrings Columns used in index
     * @return A value that is duplicated in the table (null if no duplicates found)
     * @throws IOException
     */
    public byte[] findDuplicateValue(String tableName, String columnNameStrings) throws IOException {
        TableInfo info = getTableInfo(tableName);
        long tableId = info.getId();
        List<String> columnNames = Arrays.asList(columnNameStrings.split(","));
        byte[] startKey = RowKeyFactory.buildDataKey(tableId, Constants.ZERO_UUID);
        byte[] endKey = RowKeyFactory.buildDataKey(tableId, Constants.FULL_UUID);
        Scan scan = ScanFactory.buildScan(startKey, endKey);

        List<byte[]> columnIds = new LinkedList<byte[]>();
        for (String columnName : columnNames) {
            byte[] columnIdBytes = Bytes.toBytes(info.getColumnIdByName(columnName));
            columnIds.add(columnIdBytes);
            scan.addColumn(Constants.NIC, columnIdBytes);
        }

        Set<ByteBuffer> columnValues = new HashSet<ByteBuffer>();

        ResultScanner scanner = table.getScanner(scan);
        Result result;

        while ((result = scanner.next()) != null) {
            List<byte[]> values = new LinkedList<byte[]>();
            int size = 0;
            for (byte[] columnIdBytes : columnIds) {
                byte[] value = result.getValue(Constants.NIC, columnIdBytes);
                values.add(value);
                size += value.length;
            }
            ByteBuffer value = ByteBuffer.wrap(Util.mergeByteArrays(values, size));

            if (columnValues.contains(value)) {
                return value.array();
            }

            columnValues.add(value);
        }

        return null;
    }

    /**
     * Sets up the search key for a scan through the index.
     * When MySQL does a index first or last search, the key it passes through is empty.
     * This method is used to simulate being passed a key of all 0x00 or 0xFF.
     *
     * @param tableName  SQL table name
     * @param columnName Columns to use in index
     * @param fill       Byte value to fill the search key
     * @throws IOException
     */
    public List<KeyValue> setupKeyValues(String tableName, List<String> columnName, byte fill) throws IOException {
        TableInfo info = getTableInfo(tableName);
        List<KeyValue> keyValues = Lists.newLinkedList();
        for (String column : columnName) {
            ColumnMetadata metadata = info.getColumnMetadata(column);
            byte[] value = new byte[metadata.getMaxLength()];
            Arrays.fill(value, fill);
            keyValues.add(new KeyValue(column, value, metadata.isNullable(), false));
        }

        return keyValues;
    }

    /**
     * Query whether a column on a SQL table is NULL
     *
     * @param tableName  SQL table name
     * @param columnName Column name in question
     * @return Whether the column is nullable
     * @throws IOException
     */
    public boolean isNullable(String tableName, String columnName) throws IOException {
        TableInfo info = getTableInfo(tableName);
        return info.getColumnMetadata(columnName).isNullable();
    }

    /**
     * Retrieve the current value of a autoincrement column
     *
     * @param tableName SQL table name
     * @param fieldName Column with autoincrement qualifier
     * @return Autoincrement value
     * @throws IOException
     */
    public long getAutoincrementValue(String tableName, String fieldName) throws IOException {
        TableInfo tableInfo = getTableInfo(tableName);
        ColumnMetadata columnMetadata = tableInfo.getColumnMetadata(fieldName);

        logger.info("isAutoincrement:" + columnMetadata.isAutoincrement() + ", autoincrementValue:" + columnMetadata.getAutoincrementValue());

        if (!columnMetadata.isAutoincrement())
            return -1L;
        else
            return columnMetadata.getAutoincrementValue();
    }

    /**
     * Retrieve the SQL row count in HBase
     *
     * @param tableName SQL table name
     * @return SQL row count
     * @throws IOException
     */
    public long getRowCount(String tableName) throws IOException {
        TableInfo tableInfo = getTableInfo(tableName);
        long tableId = tableInfo.getId();
        byte[] rowKey = RowKeyFactory.buildTableInfoKey(tableId);
        Get get = new Get(rowKey);
        Result result = table.get(get);
        NavigableMap<byte[], byte[]> map = result.getFamilyMap(Constants.NIC);
        long rowCount = ByteBuffer.wrap(map.get(Constants.ROW_COUNT)).getLong();
        return rowCount;
    }

    /**
     * Create a {@code ResultScanner} from a {@code ScanStrategy}
     *
     * @param strategy How the scan is going to move through HBase
     * @return HBase scanner
     * @throws IOException
     */
    public ResultScanner getScanner(ScanStrategy strategy) throws IOException {
        TableInfo info = getTableInfo(strategy.getTableName());

        Scan scan = strategy.getScan(info);

        return table.getScanner(scan);
    }

    /**
     * Set the cache size of a Scan
     *
     * @param cacheSize Cache size
     */
    public void setCacheSize(int cacheSize) {
        logger.info("Setting table scan row cache to " + cacheSize);
        ScanFactory.setCacheAmount(cacheSize);
    }

    private TableInfo getTableInfo(String tableName) throws IOException {
        return TableCache.getTableInfo(tableName, table);
    }

    private <K, V> Map<K, V> selectKeys(Map<K, V> map, Iterable<K> keys) {
        Map<K, V> subset = new HashMap<K, V>();
        for (K key : keys) {
            V value = map.get(key);
            if (map.containsKey(key) && value != null) {
                subset.put(key, value);
            }
        }

        return subset;
    }

    private String doFindDuplicateKey(String tableName, Map<String, byte[]> values) throws IOException {
        ScanStrategyInfo scanInfo = new ScanStrategyInfo(tableName, values.keySet(), valueMapToKeyValues(tableName, values));
        PrefixScanStrategy strategy = new PrefixScanStrategy(scanInfo);

        HBaseResultScanner scanner = new SingleResultScanner(getScanner(strategy));

        if (scanner.next(null) != null) {
            return Joiner.on(",").join(scanInfo.columnNames());
        }

        return null;
    }

    private List<KeyValue> valueMapToKeyValues(String tableName, Map<String, byte[]> valueMap) throws IOException {
        TableInfo info = getTableInfo(tableName);
        List<KeyValue> keyValues = new LinkedList<KeyValue>();
        for (Map.Entry<String, byte[]> entry : valueMap.entrySet()) {
            String key = entry.getKey();
            ColumnMetadata metadata = info.getColumnMetadata(key);
            byte[] value = entry.getValue();
            keyValues.add(new KeyValue(key, value, metadata.isNullable(), value == null));
        }

        return keyValues;
    }
}
