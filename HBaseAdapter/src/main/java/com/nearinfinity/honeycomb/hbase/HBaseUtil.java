package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.hbase.gen.ColumnMetadataRow;
import com.nearinfinity.honeycomb.hbase.gen.ColumnsRow;
import com.nearinfinity.honeycomb.hbase.gen.RootRow;
import com.nearinfinity.honeycomb.hbase.gen.RowKey;
import com.nearinfinity.honeycomb.mysql.Util;

import java.io.IOException;
import java.util.Comparator;

public class HBaseUtil {
    public static byte[] serializeRowKey(RowKey rowKey) throws IOException {
        return Util.serializeAvroObject(rowKey, RowKey.class);
    }

    public static RowKey deserializeRowKey(byte[] rowKey) throws IOException {
        return (RowKey) Util.deserializeAvroObject(rowKey, RowKey.class);
    }





}
