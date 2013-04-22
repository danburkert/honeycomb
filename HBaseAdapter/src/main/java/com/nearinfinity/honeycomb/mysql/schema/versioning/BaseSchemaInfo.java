package com.nearinfinity.honeycomb.mysql.schema.versioning;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.avro.Schema;

/**
 * Base class used by {@link SchemaInfo} implementations to provide standard
 * schema lookup behavior
 */
public abstract class BaseSchemaInfo implements SchemaInfo {

    /**
     * Default container used for versioned schema retrievals
     */
    private static final Schema[] EMPTY_CONTAINER = new Schema[0];

    /**
     * Provides the container of versioned {@link Schema} objects to use
     * during schema retrieval
     *
     * @return The schema storage container
     */
    Schema[] getSchemaContainer() {
        return EMPTY_CONTAINER;
    }

    @Override
    public Schema findSchema(final int version) {
        final Schema[] container = getSchemaContainer();

        checkArgument(version >= 0 && version < container.length);
        return container[version];
    }
}
