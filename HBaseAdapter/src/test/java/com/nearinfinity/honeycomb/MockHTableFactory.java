package com.nearinfinity.honeycomb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;

import java.io.IOException;

public class MockHTableFactory implements HTableInterfaceFactory {
    @Override
    public HTableInterface createHTableInterface(Configuration config, byte[] tableName) {
        return MockHTable.create();
    }

    @Override
    public void releaseHTableInterface(HTableInterface table) throws IOException {
    }
}
