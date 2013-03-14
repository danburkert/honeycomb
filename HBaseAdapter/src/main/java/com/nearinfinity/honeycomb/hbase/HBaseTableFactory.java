package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.Table;

public interface HBaseTableFactory {
    Table createTable(String tableName);
}
