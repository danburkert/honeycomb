package com.nearinfinity.honeycomb.util;

import java.util.Map;

import com.nearinfinity.honeycomb.ColumnSchemaFactory;
import com.nearinfinity.honeycomb.IndexSchemaFactory;
import com.nearinfinity.honeycomb.TableSchemaFactory;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;

public class VerifyTest {

    private static final String INDEX_NAME = "idxName";
    private static final String COLUMN_A = "columnA";
    private static final String COLUMN_B = "columnB";

    @Test
    public void testIsValidTableId() {
        Verify.isValidId(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsValidTableIdInvalidId() {
        Verify.isValidId(-1);
    }

    @Test(expected = NullPointerException.class)
    public void testIsNotNullOrEmptyNullValue() {
        Verify.isNotNullOrEmpty(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsNotNullOrEmptyEmptyValue() {
        Verify.isNotNullOrEmpty("");
    }

    @Test
    public void testIsNotNullOrEmpty() {
        Verify.isNotNullOrEmpty("foo");
    }

    @Test(expected = NullPointerException.class)
    public void testIsValidTableSchemaNullSchema() {
        Verify.isValidTableSchema(null);
    }

    @Test(expected = NullPointerException.class)
    public void testIsValidIndexSchemaNullIndices() {
        Verify.isValidIndexSchema(null, ImmutableMap.<String, ColumnSchema>of());
    }

    @Test(expected = NullPointerException.class)
    public void testIsValidIndexSchemaNullColumns() {
        Verify.isValidIndexSchema(ImmutableMap.<String, IndexSchema>of(), null);
    }

    @Test
    public void testIsValidIndexSchema() {
        final Map<String, IndexSchema> indices = ImmutableMap.<String, IndexSchema>of(
                INDEX_NAME, IndexSchemaFactory.createIndexSchema(ImmutableList.<String>of(COLUMN_A), false, INDEX_NAME));

        final Map<String, ColumnSchema> columns = ImmutableMap.<String, ColumnSchema>of(
                COLUMN_A, ColumnSchemaFactory.createColumnSchema(ColumnType.LONG, true, false, 8, 0, 0));

        Verify.isValidIndexSchema(indices, columns);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsValidIndexSchemaInvalidColumn() {
        final Map<String, IndexSchema> indices = ImmutableMap.<String, IndexSchema>of(
                INDEX_NAME, IndexSchemaFactory.createIndexSchema(ImmutableList.<String>of(COLUMN_B), false, INDEX_NAME));

        final Map<String, ColumnSchema> columns = ImmutableMap.<String, ColumnSchema>of(
                COLUMN_A, ColumnSchemaFactory.createColumnSchema(ColumnType.LONG, true, false, 8, 0, 0));

        Verify.isValidIndexSchema(indices, columns);
    }

    @Test
    public void testHasAutoIncrementColumn() {
        final Map<String, ColumnSchema> columns = ImmutableMap.<String, ColumnSchema>of(
                COLUMN_B, ColumnSchemaFactory.createColumnSchema(ColumnType.LONG, true, true, 8, 0, 0));

        final TableSchema tableSchema = TableSchemaFactory.createTableSchema(columns, ImmutableMap.<String, IndexSchema>of());

        Verify.hasAutoIncrementColumn(tableSchema);
    }

    @Test
    public void testHasAutoIncrementColumnNotAutoInc() {
        final Map<String, ColumnSchema> columns = ImmutableMap.<String, ColumnSchema>of(
                COLUMN_B, ColumnSchemaFactory.createColumnSchema(ColumnType.LONG, true, false, 8, 0, 0));

        final TableSchema tableSchema = TableSchemaFactory.createTableSchema(columns, ImmutableMap.<String, IndexSchema>of());

        Verify.hasAutoIncrementColumn(tableSchema);
    }
}
