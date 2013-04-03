package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.config.ConfigurationHolder;
import com.nearinfinity.honeycomb.config.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.log4j.Logger;

import java.io.IOException;

public class TableCreator {
    private static final Logger logger = Logger.getLogger(TableCreator.class);

    /**
     * Creates a table in HBase to store all Honeycomb tables
     *
     * @param configuration Configuration of the HTable
     * @throws IOException
     */
    public static void createTable(ConfigurationHolder configuration)
            throws IOException {
        HTableDescriptor tableDescriptor;
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(Constants.DEFAULT_COLUMN_FAMILY);
        HBaseAdmin admin = new HBaseAdmin(configuration.getConfiguration());
        byte[] tableName = configuration.getStorageTableName().getBytes();

        columnDescriptor.setBloomFilterType(StoreFile.BloomType.ROW)
                .setDataBlockEncoding(DataBlockEncoding.PREFIX)
//                .setCompressionType(Compression.Algorithm.SNAPPY)
                .setMaxVersions(1);

        if (!admin.tableExists(tableName)) {
            logger.info("Creating HBase table");
            tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(columnDescriptor);

            admin.createTable(tableDescriptor);
        }

        tableDescriptor = admin.getTableDescriptor(tableName);
        if (!tableDescriptor.hasFamily(Constants.DEFAULT_COLUMN_FAMILY)) {
            logger.info("Adding column family to HBase table");

            if (!admin.isTableDisabled(tableName)) {
                logger.info("Disabling HBase table");
                admin.disableTable(tableName);
            }

            admin.addColumn(tableName, columnDescriptor);
        }

        if (admin.isTableDisabled(tableName)) {
            logger.info("Enabling HBase table");
            admin.enableTable(tableName);
        }

        try {
            admin.flush(tableName);
        } catch (InterruptedException e) {
            logger.warn("HBaseAdmin flush was interrupted. Retrying.");
            try {
                admin.flush(tableName);
            } catch (InterruptedException e1) {
                throw new RuntimeException(e1);
            }
        }

        logger.info("HBase table successfully initialized.");
    }
}