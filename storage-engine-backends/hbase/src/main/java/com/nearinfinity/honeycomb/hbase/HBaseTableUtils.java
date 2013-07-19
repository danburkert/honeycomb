package com.nearinfinity.honeycomb.hbase;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;
import com.nearinfinity.honeycomb.config.Constants;
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
 * Honeycomb helper functions to simplify working with HBase tables.
 */
public class HBaseTableUtils {
    private static final Logger logger = Logger.getLogger(HBaseTableUtils.class);

    private static enum TableType {
        Directory,
        SQL
    }

    @Inject
    public static void ensureDirectoryTableExists(final Configuration configuration,
                                                  final @Named(Constants.INSTANCE_NAME) String instance)
            throws IOException {
        ensureTableExists(configuration, instance, new byte[0][0]);
    }

    @Inject
    public static void ensureSQLTableExists(final Configuration configuration,
                                            final @Named(Constants.INSTANCE_NAME) String instance,
                                            @Assisted final String tableName) {
        ensureTableExists(configuration, )
    }

    public static void ensureTableExists(final Configuration configuration,
                                         final String tableName,
                                         final byte[][] splits) {
    }
    private static HBaseAdmin connect(final Configuration configuration)
            throws IOException {
        try {
            return new HBaseAdmin(configuration);
        } catch (MasterNotRunningException e) {
            logger.fatal(String.format("HMaster doesn't appear to be running. Zookeeper quorum: %s", configuration.get("hbase.zookeeper.quorum")), e);
            throw e;
        } catch (ZooKeeperConnectionException e) {
            logger.fatal("Failed to connect to Zookeeper. Zookeeper quorum: " + configuration.get("hbase.zookeeper.quorum"), e);
            throw e;
        }
    }

    private static void createTable(HBaseAdmin admin, String tableName,
                                    String columnFamily, byte[][] splits)
            throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(getColumnDescriptor(columnFamily));
        admin.createTable(tableDescriptor, splits);
    }

    /**
     * Returns an HColumnDescriptor configured for Honeycomb tables
     * @param columnFamily  Name of column family
     */
    private static HColumnDescriptor getColumnDescriptor(String columnFamily) {
        return new HColumnDescriptor(columnFamily)
                .setBloomFilterType(StoreFile.BloomType.ROW)
                .setDataBlockEncoding(DataBlockEncoding.PREFIX)
//                .setCompressionType(Compression.Algorithm.SNAPPY)
                .setMaxVersions(1);
    }

    /**
     * Split an HBase table at the given split points
     * @param tableName  name of table to be split
     * @param splits collection of split points
     * @throws IOException
     * @throws InterruptedException
     */
    @Inject
    public static void splitTable(Configuration conf,
                                  @Assisted String tableName,
                                  @Assisted Collection<byte[]> splits)
            throws IOException, InterruptedException {
        HBaseAdmin admin = connect(conf);

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
     * @param split
     * @param regions
     * @return
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
