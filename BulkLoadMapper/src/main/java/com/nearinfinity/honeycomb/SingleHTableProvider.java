package com.nearinfinity.honeycomb;

import com.google.inject.Provider;
import org.apache.hadoop.hbase.client.HTableInterface;

public class SingleHTableProvider implements Provider<HTableInterface> {
    private final HTableInterface hTable;
    public SingleHTableProvider(HTableInterface hTable) {
        this.hTable = hTable;
    }
    @Override
    public HTableInterface get() {
        return hTable;
    }
}
