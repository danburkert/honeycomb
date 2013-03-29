package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.config.ConfigurationHolder;
import com.nearinfinity.honeycomb.config.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

import java.io.IOException;

public class TableCreator {
    private static final Logger logger = Logger.getLogger(TableCreator.class);

    /**
     * Creates an HTable, with the correct column family, in HBase that will
     * store all of the SQL tables.
     *
     * @param configuration Configuration of the HTable
     * @throws IOException
     */
    public static void createTable(ConfigurationHolder configuration)
            throws IOException {
        HTableDescriptor sqlTableDescriptor;
        HColumnDescriptor nicColumn = new HColumnDescriptor(Constants.DEFAULT_COLUMN_FAMILY);
        HBaseAdmin admin = new HBaseAdmin(configuration.getConfiguration());
        byte[] tableName = configuration.getStorageTableName().getBytes();

        if (!admin.tableExists(tableName)) {
            logger.info("Creating sql table");
            sqlTableDescriptor = new HTableDescriptor(tableName);
            sqlTableDescriptor.addFamily(nicColumn);

            admin.createTable(sqlTableDescriptor);
        }

        sqlTableDescriptor = admin.getTableDescriptor(tableName);
        if (!sqlTableDescriptor.hasFamily(Constants.DEFAULT_COLUMN_FAMILY)) {
            logger.info("Adding nic column family to sql table");

            if (!admin.isTableDisabled(tableName)) {
                logger.info("Disabling sql table");
                admin.disableTable(tableName);
            }

            admin.addColumn(tableName, nicColumn);
        }

        if (admin.isTableDisabled(tableName)) {
            logger.info("Enabling sql table");
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

        logger.info("Sql table successfully initialized.");
    }
}