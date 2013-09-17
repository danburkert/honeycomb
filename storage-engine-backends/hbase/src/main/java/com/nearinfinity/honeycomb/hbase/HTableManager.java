package com.nearinfinity.honeycomb.hbase;

import com.google.inject.Inject;
import com.nearinfinity.honeycomb.exceptions.RuntimeIOException;
import com.nearinfinity.honeycomb.hbase.config.HBaseProperties;
import com.nearinfinity.honeycomb.mysql.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Manager class to simplify working with HBase tables.
 */
public class HTableManager {
    private static final Logger logger = Logger.getLogger(HTableManager.class);
    private final Configuration configuration;
    private final String columnFamily;

    @Inject
    public HTableManager(Configuration configuration) {
        this.configuration = configuration;
        this.columnFamily = configuration.get(HBaseProperties.COLUMN_FAMILY);
    }

    /**
     * Checks if the HBase table exists.  If it does not, it creates it.
     */
    public void ensureTableExists(String tableName)  {
        HBaseAdmin admin = getHBaseAdmin();
        try {
            if (!admin.tableExists(tableName)) {
                createTable(admin,
                        tableName,
                        columnFamily,
                        new byte[0][0]);
            }
        } catch (IOException e) {
            String msg = "Failed to connect to HBase while ensuring the directory table exists";
            logger.fatal(msg);
            throw new RuntimeIOException(msg, e);
        }
    }

    public boolean checkTableExists(String tableName) {
        HBaseAdmin admin = getHBaseAdmin();
        try {
            return admin.tableExists(tableName);
        } catch (IOException e) {
            String msg = "Unable to check existence of table: " + tableName;
            logger.error(msg);
            throw new RuntimeIOException(msg, e);
        }
    }

    public void createTable(final String tableName, byte[][] splits) {
        HBaseAdmin admin = getHBaseAdmin();
        createTable(admin,
                tableName,
                columnFamily,
                splits);
    }

    private HBaseAdmin getHBaseAdmin() {
        try {
            return new HBaseAdmin(configuration);
        } catch (MasterNotRunningException e) {
            String msg = "Failed to getHBaseAdmin to HMaster. Zookeeper quorum: %s" + configuration.get("hbase.zookeeper.quorum");
            logger.fatal(msg);
            throw new RuntimeIOException(msg, e);
        } catch (ZooKeeperConnectionException e) {
            String msg = "Failed to getHBaseAdmin to Zookeeper. Zookeeper quorum: %s" + configuration.get("hbase.zookeeper.quorum");
            logger.fatal(msg);
            throw new RuntimeIOException(msg, e);
        }
    }

    private void createTable(HBaseAdmin admin, String tableName,
                             String columnFamily, byte[][] splits) {
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(getColumnDescriptor(columnFamily));
        try {
            admin.createTable(tableDescriptor, splits);
        } catch (IOException e) {
            String msg = "Unable to create HBase table: " + tableName;
            logger.error(msg);
            throw new RuntimeIOException(msg, e);
        }
    }

    /**
     * Returns an HColumnDescriptor configured for Honeycomb tables
     * @param columnFamily  Name of column family
     */
    private HColumnDescriptor getColumnDescriptor(String columnFamily) {
        return new HColumnDescriptor(columnFamily)
                .setBloomFilterType(StoreFile.BloomType.ROW)
                .setDataBlockEncoding(DataBlockEncoding.PREFIX)
//                .setCompressionType(Compression.Algorithm.SNAPPY)
                .setMaxVersions(1);
    }

    public void dropTable(String tableName) {
        HBaseAdmin admin = getHBaseAdmin();
        try {
            admin.deleteTable(tableName);
        } catch (IOException e) {
            String msg = "Failed to drop HBase table: " + tableName;
            logger.error(msg, e);
            throw new RuntimeIOException(msg, e);
        }
    }

    /**
     * Split an HBase table at the given split points
     * @param tableName  name of table to be split
     * @param splits collection of split points
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    public void splitTable(String tableName, Collection<byte[]> splits)
            throws IOException, InterruptedException {
        HBaseAdmin admin = getHBaseAdmin();

        for (byte[] split : splits) {
            while(!splitExists(split, reallyGetTableRegions(admin, tableName))) {
                logger.info("Attempting to split table " + tableName + " on key: " + Util.generateHexString(split));
                admin.split(tableName.getBytes(), split);
            }
        }
    }

    /**
     * This method should not exist.  Alas, the HBase client is a pile of crap.
     */
    private static List<HRegionInfo> reallyGetTableRegions(HBaseAdmin admin, String tableName)
            throws IOException {
        List<HRegionInfo> regions = null;
        while (regions == null) {
            regions = admin.getTableRegions(tableName.getBytes());
        }
        return regions;
    }

    /**
     * Check that the split key exists as start key of a region in the list.
     */
    private static boolean splitExists(byte[] split, List<HRegionInfo> regions) {
        for (HRegionInfo region : regions) {
            if (Arrays.equals(split, region.getStartKey())) {
                return true;
            }
        }
        return false;
    }
}