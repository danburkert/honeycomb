package com.nearinfinity.bulkloader;

import com.nearinfinity.hbaseclient.ColumnType;

import java.nio.ByteBuffer;

public class ValueTransformer {

    public static byte[] transform(String val, ColumnType t) {
        byte[] ret = null;
        switch (t) {
            case LONG:
            case ULONG:
                ret = ByteBuffer.allocate(8).putLong(Long.parseLong(val)).array();
                break;
            case DOUBLE:
                ret = ByteBuffer.allocate(8).putDouble(Double.parseDouble(val)).array();
                break;
            case TIME:
            case DATE:
            case DATETIME:
            case DECIMAL:
            case STRING:
            case BINARY:
            case NONE:
            default:
                ret = val.getBytes();
                break;
        }
        return ret;
    }
}