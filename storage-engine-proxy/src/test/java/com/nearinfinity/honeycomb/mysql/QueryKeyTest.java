package com.nearinfinity.honeycomb.mysql;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.nearinfinity.honeycomb.mysql.gen.QueryType;

public class QueryKeyTest {

    private static final String TEST_INDEX = "testIndex";

    @Test(expected = NullPointerException.class)
    public void testCreationInvalidQueryFields() {
        new QueryKey(TEST_INDEX, QueryType.EXACT_KEY, null);
    }

    @Test(expected = NullPointerException.class)
    public void testCreationInvalidQueryType() {
        new QueryKey(TEST_INDEX, null, ImmutableMap.<String, ByteBuffer>of());
    }

    @Test(expected = NullPointerException.class)
    public void testCreationInvalidIndexName() {
        new QueryKey(null, QueryType.EXACT_KEY, ImmutableMap.<String, ByteBuffer>of());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIndexSchemaCreationEmptyIndexName() {
        new QueryKey("", QueryType.EXACT_KEY, ImmutableMap.<String, ByteBuffer>of());
    }

    @Test(expected = NullPointerException.class)
    public void testDeserializeInvalidSerializedSchema() {
        QueryKey.deserialize(null);
    }

    @Test
    public void testDeserializeValidSerializedSchemaAndIndexName() {
        final QueryKey key = new QueryKey(TEST_INDEX, QueryType.EXACT_KEY, ImmutableMap.<String, ByteBuffer>of());
        final QueryKey actualKey = QueryKey.deserialize(key.serialize());

        assertEquals(key.getIndexName(), actualKey.getIndexName());
        assertEquals(key.getQueryType(), actualKey.getQueryType());
        assertEquals(key.getKeys(), actualKey.getKeys());
    }
}
