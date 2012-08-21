package com.nearinfinity.mysqlengine.strategy;

import com.nearinfinity.mysqlengine.ColumnMetadata;
import com.nearinfinity.mysqlengine.RowKeyFactory;
import com.nearinfinity.mysqlengine.ScanFactory;
import com.nearinfinity.mysqlengine.TableInfo;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/21/12
 * Time: 8:20 AM
 * To change this template use File | Settings | File Templates.
 */
public class ReverseScanStrategy extends ScanStrategyBase {

    public ReverseScanStrategy(String tableName, String columnName, byte[] value) {
        super(tableName, columnName, value);
    }

    @Override
    public Scan getScan(TableInfo info) {
        long tableId = info.getId();
        long columnId = info.getColumnIdByName(columnName);
        ColumnMetadata columnType = info.getColumnTypeByName(columnName);

        byte[] startKey = RowKeyFactory.buildReverseIndexKey(tableId, columnId, value, columnType);
        byte[] endKey = RowKeyFactory.buildReverseIndexKey(tableId, columnId+1, new byte[0], ColumnMetadata.NONE);

        return ScanFactory.buildScan(startKey, endKey);
    }
}
