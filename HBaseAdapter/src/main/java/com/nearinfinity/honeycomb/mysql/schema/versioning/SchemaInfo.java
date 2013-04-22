package com.nearinfinity.honeycomb.mysql.schema.versioning;

import org.apache.avro.Schema;

/**
 * Interface that all serialized data types that require versioning support must
 * use to maintain schema version information over time
 */
public interface SchemaInfo {
    /**
     * Attempts to find the writer {@link Schema} used for the specified schema version
     *
     * @param version The version number of interest, must be non-negative and within the range of available schemas
     * @return The writer schema used for the specified version
     */
    public Schema findSchema(final int version);
}
