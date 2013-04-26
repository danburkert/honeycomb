package com.nearinfinity.honeycomb.mysql.schema.versioning;

import org.apache.avro.Schema;

import com.nearinfinity.honeycomb.mysql.gen.AvroRow;

/**
 *  Maintains all schema versioning information corresponding to {@link AvroRow}
 */
public final class RowSchemaInfo extends  BaseSchemaInfo {
    /**
     * The current version number associated with {@link AvroRow}
     */
    public static final int VER_CURRENT = 0;

    /**
     * Lookup table used to find the writer {@link Schema} used by the schema version
     * that corresponds to the position in the container
     */
    private static final Schema[] SCHEMA_CONTAINER = {
        new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"AvroRow\",\"namespace\":\"com.nearinfinity.honeycomb.mysql.gen\",\"fields\":[{\"name\":\"version\",\"type\":\"int\",\"doc\":\"Schema version number\",\"default\":0},{\"name\":\"uuid\",\"type\":{\"type\":\"fixed\",\"name\":\"UUIDContainer\",\"size\":16}},{\"name\":\"records\",\"type\":{\"type\":\"map\",\"values\":\"bytes\",\"avro.java.string\":\"String\"}}]}")
    };

    @Override
    Schema[] getSchemaContainer() {
        return SCHEMA_CONTAINER;
    }
}
