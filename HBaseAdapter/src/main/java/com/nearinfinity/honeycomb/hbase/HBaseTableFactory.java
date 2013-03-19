package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;

public interface HBaseTableFactory {
    Table createTable(Long tableId, TableSchema schema);
}
