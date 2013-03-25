package com.nearinfinity.honeycomb.config;

/**
 * Stores the configuration constants represented by {@link ConfigurationHolder}
 *
 */
public final class ConfigConstants {

    private ConfigConstants() {
        throw new AssertionError();
    }


    public static final String PROP_AUTO_FLUSH_CHANGES = "hbase.flushChangesImmediately";
    public static final boolean DEFAULT_AUTO_FLUSH_CHANGES = false;


    public static final String PROP_WRITE_BUFFER_SIZE = "hbase.writeBufferSize";
    /**
     * Default number of bytes used for write buffer storage
     */
    public static final long DEFAULT_WRITE_BUFFER_SIZE = 5 * 1024 * 1024;


    public static final String PROP_TABLE_POOL_SIZE = "hbase.tablePoolSize";
    /**
     * Default number of references to keep active for a table
     */
    public static final int DEFAULT_TABLE_POOL_SIZE = 5;


    public static final String PROP_TABLE_SCAN_CACHE_ROW_SIZE = "hbase.tableScanCacheRows";
    /**
     * Default number of rows for caching that will be passed to scanners
     */
    public static final int DEFAULT_TABLE_SCAN_CACHE_ROW_SIZE = 2500;


    public static final String PROP_TABLE_NAME = "hbase.tableName";


    public static final String PROP_ZOOKEEPER_QUORUM = "hbase.zookeeperQuorum";

    public static final String PROP_CONFIGURED_ADAPTERS = "configuredAdapters";
}
