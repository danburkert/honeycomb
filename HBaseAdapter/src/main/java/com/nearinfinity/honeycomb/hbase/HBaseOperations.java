package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.HoneycombException;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class HBaseOperations {
    private static final Logger logger = Logger.getLogger(HBaseOperations.class);

    public static void performPut(HTableInterface hTable, List<Put> puts) {
        try {
            hTable.put(puts);
        } catch (IOException e) {
            logger.error(e);
            throw new HoneycombException("HBase table put list failed", e);
        }
    }

    public static void performPut(HTableInterface hTable, Put put) {
        try {
            hTable.put(put);
        } catch (IOException e) {
            logger.error(e);
            throw new HoneycombException("HBase table put failed", e);
        }
    }

    public static void performDelete(HTableInterface hTable, List<Delete> deletes) {
        try {
            hTable.delete(deletes);
        } catch (IOException e) {
            logger.error(e);
            throw new HoneycombException("HBase table delete failed", e);
        }
    }

    public static void performFlush(HTableInterface hTable) {
        try {
            hTable.flushCommits();
        } catch (IOException e) {
            logger.error(e);
            throw new HoneycombException("HBase table flush failed", e);
        }
    }

    public static void closeTable(HTableInterface hTable) {
        try {
            hTable.close();
        } catch (IOException e) {
            logger.error(e);
            throw new HoneycombException("Close table failed", e);
        }
    }

    public static Result performGet(HTableInterface hTable, Get get) {
        try {
            return hTable.get(get);
        } catch (IOException e) {
            logger.error(e);
            throw new HoneycombException("HBase table get failed", e);
        }
    }

    public static long performIncrementColumnValue(HTableInterface hTable, byte[] row, byte[] columnFamily, byte[] identifier, long amount) {
        try {
            return hTable.incrementColumnValue(row, columnFamily, identifier, amount);
        } catch (IOException e) {
            logger.error(e);
            throw new HoneycombException("HBase table increment column failed", e);
        }
    }

    public static ResultScanner getScanner(HTableInterface hTable, Scan scan) {
        try {
            return hTable.getScanner(scan);
        } catch (IOException e) {
            logger.error(e);
            throw new HoneycombException("HBase table get scanner failed", e);
        }
    }
}
