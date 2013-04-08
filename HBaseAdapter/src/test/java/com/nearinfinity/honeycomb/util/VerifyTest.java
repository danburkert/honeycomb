package com.nearinfinity.honeycomb.util;

import com.google.common.collect.ImmutableList;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import org.junit.Test;

import java.util.List;

public class VerifyTest {

    private static final String COLUMN_A = "columnA";
    private static final String COLUMN_B = "columnB";
    private static final List<IndexSchema> INDICES = ImmutableList.of(
            new IndexSchema(ImmutableList.<String>of(COLUMN_A), false, "INDEX_A"),
            new IndexSchema(ImmutableList.<String>of(COLUMN_B), false, "INDEX_B"));
    private static final List<ColumnSchema> COLUMNS = ImmutableList.of(
            new ColumnSchema(COLUMN_A, ColumnType.LONG, true, false, null, null, null),
            new ColumnSchema(COLUMN_B, ColumnType.LONG, true, false, null, null, null));

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
        Verify.isValidIndexSchema(null, ImmutableList.<ColumnSchema>of());
    }

    @Test(expected = NullPointerException.class)
    public void testIsValidIndexSchemaNullColumns() {
        Verify.isValidIndexSchema(ImmutableList.<IndexSchema>of(), null);
    }

    @Test
    public void testIsValidIndexSchema() {
        Verify.isValidIndexSchema(INDICES, COLUMNS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsValidIndexSchemaInvalidColumn() {
        final List<IndexSchema> indices = ImmutableList.of(
                new IndexSchema(ImmutableList.of("invalid"), false, "index_name")
        );

        Verify.isValidIndexSchema(indices, COLUMNS);
    }

    @Test
    public void testHasAutoIncrementColumn() {
        final List<ColumnSchema> columns = ImmutableList.<ColumnSchema>of(
                new ColumnSchema(COLUMN_B, ColumnType.LONG, true, true, null, null, null));

        final TableSchema tableSchema = new TableSchema(columns, ImmutableList.<IndexSchema>of());

        Verify.hasAutoIncrementColumn(tableSchema);
    }

    @Test
    public void testHasAutoIncrementColumnNotAutoInc() {
        final List<ColumnSchema> columns = ImmutableList.<ColumnSchema>of(
                new ColumnSchema(COLUMN_B, ColumnType.LONG, true, false, null, null, null));

        final TableSchema tableSchema = new TableSchema(columns, ImmutableList.<IndexSchema>of());

        Verify.hasAutoIncrementColumn(tableSchema);
    }
}
