package com.nearinfinity.honeycomb.mysql.schema.versioning;

import org.apache.avro.Schema;

import com.nearinfinity.honeycomb.mysql.gen.AvroTableSchema;

/**
 *  Maintains all schema versioning information corresponding to {@link AvroTableSchema}
 */
public final class TableSchemaInfo extends BaseSchemaInfo {
    /**
     * The current version number associated with {@link AvroTableSchema}
     */
    public static final int VER_CURRENT = 0;

    /**
     * Lookup table used to find the writer {@link Schema} used by the schema version
     * that corresponds to the position in the container
     */
    private static final Schema[] SCHEMA_CONTAINER = {
        new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"AvroTableSchema\",\"namespace\":\"com.nearinfinity.honeycomb.mysql.gen\",\"fields\":[{\"name\":\"version\",\"type\":\"int\",\"doc\":\"Schema version number\",\"default\":0},{\"name\":\"columns\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"AvroColumnSchema\",\"fields\":[{\"name\":\"type\",\"type\":{\"type\":\"enum\",\"name\":\"ColumnType\",\"symbols\":[\"STRING\",\"BINARY\",\"ULONG\",\"LONG\",\"DOUBLE\",\"DECIMAL\",\"TIME\",\"DATE\",\"DATETIME\"]}},{\"name\":\"isNullable\",\"type\":\"boolean\",\"default\":true},{\"name\":\"isAutoIncrement\",\"type\":\"boolean\",\"default\":false},{\"name\":\"maxLength\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"scale\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"precision\",\"type\":[\"null\",\"int\"],\"default\":null}]},\"avro.java.string\":\"String\"}},{\"name\":\"indices\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"AvroIndexSchema\",\"fields\":[{\"name\":\"columns\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}},{\"name\":\"isUnique\",\"type\":\"boolean\",\"default\":false}]},\"avro.java.string\":\"String\"}}]}")
    };

    @Override
    Schema[] getSchemaContainer() {
        return SCHEMA_CONTAINER;
    }
}
