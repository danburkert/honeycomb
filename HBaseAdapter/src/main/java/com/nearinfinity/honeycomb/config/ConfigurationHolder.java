package com.nearinfinity.honeycomb.config;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.nearinfinity.honeycomb.util.Verify;

import net.jcip.annotations.Immutable;
import net.jcip.annotations.ThreadSafe;
import org.apache.hadoop.conf.Configuration;

import java.util.Collection;

/**
 * Responsible for holding the configuration settings used throughout the application
 */
@ThreadSafe @Immutable
public final class ConfigurationHolder {

    private final Configuration config;

    public static final String DEFAULT_STRING_VALUE = "";

    /**
     * Creates a wrapper around the provided existing configuration
     *
     * @param conf The {@link Configuration} instance to wrap, not null
     */
    public ConfigurationHolder(final Configuration conf) {
        Preconditions.checkNotNull(conf, "The provided configuration is invalid");

        // Make a copy of the provided configuration to enforce immutability
        config = new Configuration(conf);
    }

    /**
     * Retrieves the configuration for auto flushing changes on the underlying storage table
     * @return The configured value, {@link ConfigConstants#DEFAULT_AUTO_FLUSH_CHANGES} by default
     */
    public boolean getStorageAutoFlushChanges() {
        return config.getBoolean(ConfigConstants.PROP_AUTO_FLUSH_CHANGES,
                ConfigConstants.DEFAULT_AUTO_FLUSH_CHANGES);
    }

    /**
     * Retrieves the configuration for the underlying storage table name
     * @return The configured value, {@value #DEFAULT_STRING_VALUE} by default
     */
    public String getStorageTableName() {
        return config.get(ConfigConstants.PROP_TABLE_NAME, DEFAULT_STRING_VALUE);
    }

    /**
     * Retrieves the configuration for the write buffer size on the underlying storage
     * @return The configured value, {@link ConfigConstants#DEFAULT_WRITE_BUFFER_SIZE} by default
     */
    public long getStorageWriteBufferSize() {
        return config.getLong(ConfigConstants.PROP_WRITE_BUFFER_SIZE,
                ConfigConstants.DEFAULT_WRITE_BUFFER_SIZE);
    }

    /**
     * Retrieves the configuration for the table pool size on the underlying storage
     * @return The configured value, {@link ConfigConstants#DEFAULT_TABLE_POOL_SIZE} by default
     */
    public int getStorageTablePoolSize() {
        return config.getInt(ConfigConstants.PROP_TABLE_POOL_SIZE,
                ConfigConstants.DEFAULT_TABLE_POOL_SIZE);
    }

    /**
     * Retrieves the configuration for the table scan cache size on the underlying storage
     * @return The configured value, {@link ConfigConstants#DEFAULT_TABLE_SCAN_CACHE_ROW_SIZE} by default
     */
    public int getStorageTableScanCacheSize() {
        return config.getInt(ConfigConstants.PROP_TABLE_SCAN_CACHE_ROW_SIZE,
                ConfigConstants.DEFAULT_TABLE_SCAN_CACHE_ROW_SIZE);
    }

    /**
     * Retrieves the configuration for the Zookeeper quorum on the system
     * @return The configured value, {@value #DEFAULT_STRING_VALUE} by default
     */
    public String getZookeeperQuorum() {
        return config.get(ConfigConstants.PROP_ZOOKEEPER_QUORUM, DEFAULT_STRING_VALUE);
    }

    /**
     * Determines if the adapter for the specified adapter name has been configured
     * @param adapterName The name of the adapter to look for, not null or empty
     * @return True if configured, False otherwise
     */
    public boolean isAdapterConfigured(final String adapterName) {
        Verify.isNotNullOrEmpty(adapterName, "The adapter name is not valid");

        final Collection<String> adapters = config.getStringCollection(ConfigConstants.PROP_CONFIGURED_ADAPTERS);

        return adapters.contains(adapterName);
    }

    /**
     * Retrieves a copy of the wrapped {@link Configuration} object used to store configuration data
     * @return A copy of the underlying configuration
     */
    public Configuration getConfiguration() {
        // Copy the current configuration to enforce immutability
        return new Configuration(config);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this.getClass())
                .add("config", config)
                .add(ConfigConstants.PROP_AUTO_FLUSH_CHANGES, getStorageAutoFlushChanges())
                .add(ConfigConstants.PROP_TABLE_NAME, getStorageTableName())
                .add(ConfigConstants.PROP_WRITE_BUFFER_SIZE, getStorageWriteBufferSize())
                .add(ConfigConstants.PROP_TABLE_POOL_SIZE, getStorageTablePoolSize())
                .add(ConfigConstants.PROP_TABLE_SCAN_CACHE_ROW_SIZE, getStorageTableScanCacheSize())
                .add(ConfigConstants.PROP_ZOOKEEPER_QUORUM, getZookeeperQuorum())
                .add(ConfigConstants.PROP_CONFIGURED_ADAPTERS, config.getStringCollection(ConfigConstants.PROP_CONFIGURED_ADAPTERS))
                .toString();
    }
}
