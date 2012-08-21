package com.nearinfinity.hbaseclient;

import com.google.inject.AbstractModule;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;

public class HBaseModule extends AbstractModule {
    private final HTable table;

    public HBaseModule(HTable table) {
        this.table = table;
    }

    @Override
    protected void configure() {
        bind(HTableInterface.class).toInstance(this.table);
    }
}
