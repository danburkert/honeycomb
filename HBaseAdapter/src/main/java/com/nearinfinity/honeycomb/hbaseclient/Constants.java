package com.nearinfinity.honeycomb.hbaseclient;

import java.util.UUID;

public final class Constants {
    public static final String HBASE_TABLE = "hbase_table_name";

    public static final String ZK_QUORUM = "zk_quorum";

    public static final byte[] NIC = "nic".getBytes();

    public static final byte[] VALUE_MAP = "valueMap".getBytes();

    public static final byte[] METADATA = "metadata".getBytes();

    public static final UUID ZERO_UUID = new UUID(0L, 0L);

    public static final UUID FULL_UUID = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");

    public static final byte[] ROW_COUNT = "RowCount".getBytes();

    public static final String INDEXES_STRING = "Indexes";

    public static final String UNIQUE_STRING = "UniqueConstraints";

    public static final byte[] UNIQUES = UNIQUE_STRING.getBytes();

    public static final byte[] INDEXES = INDEXES_STRING.getBytes();

    public static final int KEY_PART_COUNT = 4;
}
