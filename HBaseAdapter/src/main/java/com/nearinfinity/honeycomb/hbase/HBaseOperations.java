package com.nearinfinity.honeycomb.hbase;

import com.google.common.base.Objects;
import com.nearinfinity.honeycomb.RuntimeIOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class HBaseOperations {
    private static final Logger logger = Logger.getLogger(HBaseOperations.class);

    public static void performPut(HTableInterface hTable, List<Put> puts) {
        try {
            hTable.put(puts);
        } catch (IOException e) {
            throw createException("HBase table put list failed", e, hTable);
        }
    }

    public static void performPut(HTableInterface hTable, Put put) {
        try {
            hTable.put(put);
        } catch (IOException e) {
            String msg = String.format("HBase table put failed for put %s", put.toString());
            throw createException(msg, e, hTable);

        }
    }

    public static void performDelete(HTableInterface hTable, List<Delete> deletes) {
        try {
            hTable.delete(deletes);
        } catch (IOException e) {
            throw createException("HBase table delete failed", e, hTable);
        }
    }

    public static void performFlush(HTableInterface hTable) {
        try {
            hTable.flushCommits();
        } catch (IOException e) {
            throw createException("HBase table flush failed", e, hTable);
        }
    }

    public static void closeTable(HTableInterface hTable) {
        try {
            hTable.close();
        } catch (IOException e) {
            throw createException("HBase close table failed", e, hTable);
        }
    }

    public static Result performGet(HTableInterface hTable, Get get) {
        try {
            return hTable.get(get);
        } catch (IOException e) {
            String msg = String.format("HBase table get failed for get %s", get.toString());
            throw createException(msg, e, hTable);
        }
    }

    public static long performIncrementColumnValue(HTableInterface hTable, byte[] row, byte[] columnFamily, byte[] identifier, long amount) {
        try {
            return hTable.incrementColumnValue(row, columnFamily, identifier, amount);
        } catch (IOException e) {
            String msg = String.format("HBase table increment column threw exception. Row (%s) / Column Family (%s) / Identifier (%s) / Amount (%d)",
                    Bytes.toStringBinary(row),
                    Bytes.toStringBinary(columnFamily),
                    Bytes.toStringBinary(identifier),
                    amount);
            throw createException(msg, e, hTable);
        }
    }

    public static ResultScanner getScanner(HTableInterface hTable, Scan scan) {
        try {
            return hTable.getScanner(scan);
        } catch (IOException e) {
            throw createException("HBase table get scanner failed", e, hTable);
        }
    }

    private static RuntimeException createException(String errorMessage, IOException e, HTableInterface hTable) {
        Configuration configuration = hTable.getConfiguration();
        String configSettings = Objects.toStringHelper(configuration)
                .add(HConstants.ZOOKEEPER_QUORUM, configuration.get(HConstants.ZOOKEEPER_QUORUM))
                .toString();
        logger.error(errorMessage + " " + configSettings, e);
        return new RuntimeIOException(errorMessage, e);
    }
}
