package com.nearinfinity.honeycomb.hbase;

public interface HBaseTableFactory {
    HBaseTable create(String tableName);
}
