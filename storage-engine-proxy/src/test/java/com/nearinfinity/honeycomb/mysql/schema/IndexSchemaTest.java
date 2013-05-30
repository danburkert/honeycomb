package com.nearinfinity.honeycomb.mysql.schema;

import static org.junit.Assert.assertEquals;
import nl.jqno.equalsverifier.EqualsVerifier;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class IndexSchemaTest {

    private static final String INDEX_A = "INDEX_A";
    private static final String COLUMN_A = "colA";

    private static final IndexSchema TEST_INDEX_SCHEMA = new IndexSchema(INDEX_A,
            ImmutableList.<String>of(COLUMN_A), false);


    @Test(expected = NullPointerException.class)
    public void testIndexSchemaCreationInvalidColumns() {
        new IndexSchema(INDEX_A, null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testIndexSchemaCreationNullIndexName() {
        new IndexSchema(null, ImmutableList.<String>of(), true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIndexSchemaCreationEmptyIndexName() {
        new IndexSchema("", ImmutableList.<String>of(), true);
    }

    @Test
    public void testIndexSchemaCreation() {
        new IndexSchema(INDEX_A, ImmutableList.<String>of(), true);
    }

    @Test(expected = NullPointerException.class)
    public void testDeserializeNullSerializedSchema() {
        IndexSchema.deserialize(null, INDEX_A);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeserializeEmptyIndexName() {
        IndexSchema.deserialize(TEST_INDEX_SCHEMA.serialize(), "");
    }

    @Test(expected = NullPointerException.class)
    public void testDeserializeInvalidIndexName() {
        IndexSchema.deserialize(TEST_INDEX_SCHEMA.serialize(), null);
    }

    @Test
    public void testDeserializeValidSerializedSchemaAndIndexName() {
        final IndexSchema actualSchema = IndexSchema.deserialize(TEST_INDEX_SCHEMA.serialize(),
                TEST_INDEX_SCHEMA.getIndexName());

        assertEquals(TEST_INDEX_SCHEMA.getIndexName(), actualSchema.getIndexName());
        assertEquals(TEST_INDEX_SCHEMA, actualSchema);
    }

    @Test
    public void equalsContract() {
        EqualsVerifier.forClass(IndexSchema.class).verify();
    }
}
