package com.nearinfinity.hbaseclient;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.UUID;

public final class Constants {
    public static final byte[] SQL = "sql".getBytes();

    public static final byte[] NIC = "nic".getBytes();

    public static final byte[] IS_DELETED = "isDeleted".getBytes();

    public static final byte[] DELETED_VAL = Bytes.toBytes(1L);

    public static final byte[] VALUE_MAP = "valueMap".getBytes();

    public static final byte[] VALUE_COLUMN = "value".getBytes();

    public static final byte[] METADATA = "metadata".getBytes();

    public static final UUID ZERO_UUID = new UUID(0L, 0L);

    public static final UUID FULL_UUID = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");

    public static final byte[] ROW_COUNT = "RowCount".getBytes();

    public static final byte[] MULTIPART_KEY = "MultipartKey".getBytes();
}
