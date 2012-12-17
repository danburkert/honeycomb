package com.nearinfinity.honeycomb.hbaseclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

import java.io.IOException;

public class SqlTableCreator {
    private static final Logger logger = Logger.getLogger(SqlTableCreator.class);

    public static void initializeSqlTable(Configuration configuration) throws IOException {
        HTableDescriptor sqlTableDescriptor;
        HColumnDescriptor nicColumn = new HColumnDescriptor(Constants.NIC);
        HBaseAdmin admin = new HBaseAdmin(configuration);

        if (!admin.tableExists(Constants.SQL)) {
            logger.info("Creating sql table");
            sqlTableDescriptor = new HTableDescriptor(Constants.SQL);
            sqlTableDescriptor.addFamily(nicColumn);

            admin.createTable(sqlTableDescriptor);
        }

        sqlTableDescriptor = admin.getTableDescriptor(Constants.SQL);
        if (!sqlTableDescriptor.hasFamily(Constants.NIC)) {
            logger.info("Adding nic column family to sql table");

            if (!admin.isTableDisabled(Constants.SQL)) {
                logger.info("Disabling sql table");
                admin.disableTable(Constants.SQL);
            }

            admin.addColumn(Constants.SQL, nicColumn);
        }

        if (admin.isTableDisabled(Constants.SQL)) {
            logger.info("Enabling sql table");
            admin.enableTable(Constants.SQL);
        }

        try {
            admin.flush(Constants.SQL);
        } catch (InterruptedException e) {
            logger.warn("HBaseAdmin flush was interrupted. Retrying.");
            try {
                admin.flush(Constants.SQL);
            } catch (InterruptedException e1) {
                throw new RuntimeException(e1);
            }
        }

        logger.info("Sql table successfully initialized.");
    }
}
