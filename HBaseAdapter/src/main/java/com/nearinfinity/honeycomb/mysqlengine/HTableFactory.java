package com.nearinfinity.honeycomb.mysqlengine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;

import java.io.IOException;

public class HTableFactory implements HTableInterfaceFactory {
    private final long writeBufferSize;
    private final boolean autoFlush;

    public HTableFactory(long writeBufferSize, boolean autoFlush) {
        this.writeBufferSize = writeBufferSize;
        this.autoFlush = autoFlush;
    }

    @Override
    public HTableInterface createHTableInterface(Configuration config, byte[] tableName) {
        try {
            HTable table = new HTable(config, tableName);
            table.setAutoFlush(autoFlush);
            table.setWriteBufferSize(writeBufferSize);
            return table;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void releaseHTableInterface(HTableInterface table) throws IOException {
        table.close();
    }
}
