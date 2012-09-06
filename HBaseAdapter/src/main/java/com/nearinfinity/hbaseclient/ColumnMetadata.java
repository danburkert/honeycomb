package com.nearinfinity.hbaseclient;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: acrute
 * Date: 8/15/12
 * Time: 4:49 PM
 * To change this template use File | Settings | File Templates.
 */
public enum ColumnMetadata {
    NONE("None".getBytes()),
    IS_NULLABLE("IsNullable".getBytes()),
    PRIMARY_KEY("PrimaryKey".getBytes()),
    MAX_LENGTH("MaxLength".getBytes()),
    COLUMN_TYPE("ColumnType".getBytes());

    private byte[] value;

    ColumnMetadata(byte[] value) {
        this.value = value;
    }

    public byte[] getValue() {
        return this.value;
    }

    public static ColumnMetadata getByValue(byte[] qualifier) {
        for(ColumnMetadata metadata : ColumnMetadata.values()) {
            if (Arrays.equals(qualifier, metadata.getValue())) {
                return metadata;
            }
        }
        return ColumnMetadata.NONE;
    }
}
