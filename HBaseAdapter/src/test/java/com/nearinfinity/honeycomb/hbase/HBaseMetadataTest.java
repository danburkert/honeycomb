package com.nearinfinity.honeycomb.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.PrimitiveGenerators;
import net.java.quickcheck.generator.iterable.Iterables;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import com.google.common.collect.ImmutableMap;
import com.nearinfinity.honeycomb.MockHTable;
import com.nearinfinity.honeycomb.TableNotFoundException;
import com.nearinfinity.honeycomb.mysql.ColumnSchemaGenerator;
import com.nearinfinity.honeycomb.mysql.TableSchemaGenerator;
import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;

public class HBaseMetadataTest {
    private static Generator<TableSchema> tableSchemaGen = new TableSchemaGenerator();
    private static Generator<ColumnSchema> columnSchemaGen = new ColumnSchemaGenerator();
    private static Generator<Long> longGen = PrimitiveGenerators.longs();

    static HBaseMetadata hbaseMetadata;
    static Map<String, TableSchema> tableSchemas;

    @BeforeClass
    public static void setUp() throws Exception {
        hbaseMetadata = new HBaseMetadata(MockHTable.create());
        tableSchemas = new HashMap<String, TableSchema>();
        for (TableSchema schema : Iterables.toIterable(tableSchemaGen)) {
            final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();
            tableSchemas.put(tableName, schema);
            hbaseMetadata.putSchema(tableName, schema);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        hbaseMetadata = null;
        tableSchemas = null;
    }

    @Test
    public void testUniqueTableIds() throws Exception {
        Set<Long> ids = new HashSet<Long>();
        Long id;
        for (String table : tableSchemas.keySet()) {
            id = hbaseMetadata.getTableId(table);
            Assert.assertFalse(ids.contains(id));
            ids.add(id);
        }
    }

    @Test
    public void testUniqueColumnIds() throws Exception {
        long tableId;
        for (String table : tableSchemas.keySet()) {
            // If there are duplicate column ids BiMap will throw an
            // IllegalArgumentException, so no assertion needed.
            tableId = hbaseMetadata.getTableId(table);
            hbaseMetadata.getColumnIds(tableId);
        }
    }

    @Test
    public void testSchemaGet() throws Exception {
        long tableId;
        for (String tableName : tableSchemas.keySet()) {
            TableSchema expected = tableSchemas.get(tableName);
            tableId = hbaseMetadata.getTableId(tableName);
            TableSchema actual = hbaseMetadata.getSchema(tableId);
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testSchemaDeleteRemovesAllRowIds() throws Exception {
        HTableInterface hTable = MockHTable.create();
        HBaseMetadata hbaseMetadata2 = new HBaseMetadata(hTable);
        TableSchema schema = tableSchemaGen.next();
        final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();
        hbaseMetadata2.putSchema(tableName, schema);
        long tableId = hbaseMetadata2.getTableId(tableName);
        TableSchema expected = hbaseMetadata2.getSchema(tableId);
        Assert.assertEquals(schema, expected);

        hbaseMetadata2.deleteSchema(tableName);
        ResultScanner results = hTable.getScanner(new Scan());
        Assert.assertTrue(results.next().getNoVersionMap().size() == 1); // Table id counter
        Assert.assertNull(results.next());
    }

    @Test(expected = TableNotFoundException.class)
    public void testSchemaDeleteRemovesTable() throws Exception {
        TableSchema schema = tableSchemaGen.next();
        final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();
        hbaseMetadata.putSchema(tableName, schema);
        long tableId = hbaseMetadata.getTableId(tableName);
        Assert.assertEquals(schema, hbaseMetadata.getSchema(tableId));
        hbaseMetadata.deleteSchema(tableName);
        hbaseMetadata.getSchema(tableId);
    }


    @Test(expected = TableNotFoundException.class)
    public void testRenameExistingTableNoAutoFlush() throws Exception {
        String originalName = "OriginalName";
        String newName = "NewName";

        TableSchema origSchema = tableSchemaGen.next();

        // Configure the table to disable auto flush
        HTableInterface hTableSpy = PowerMockito.spy(MockHTable.create());
        Mockito.when(hTableSpy.isAutoFlush()).thenReturn(false);

        HBaseMetadata hbaseMetadataNoFlush = new HBaseMetadata(hTableSpy);

        hbaseMetadataNoFlush.putSchema(originalName, origSchema);

        long origId = hbaseMetadataNoFlush.getTableId(originalName);
        hbaseMetadataNoFlush.renameExistingTable(originalName, newName);

        long newId = hbaseMetadataNoFlush.getTableId(newName);

        Assert.assertEquals(origId, newId);
        Assert.assertEquals(origSchema.getColumns(),
                hbaseMetadataNoFlush.getSchema(newId).getColumns());

        // Trying to access the id of the old table name will result in an exception
        hbaseMetadataNoFlush.getTableId(originalName);
    }

    @Test(expected = NullPointerException.class)
    public void testRenameExistingTableNullCurrentTableName() throws IOException, TableNotFoundException {
        hbaseMetadata.renameExistingTable(null, "foo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRenameExistingTableEmptyCurrentTableName() throws IOException, TableNotFoundException {
        hbaseMetadata.renameExistingTable("", "foo");
    }

    @Test(expected = NullPointerException.class)
    public void testRenameExistingTableNullNewTableName() throws IOException, TableNotFoundException {
        hbaseMetadata.renameExistingTable("foo", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRenameExistingTableEmptyNewTableName() throws IOException, TableNotFoundException {
        hbaseMetadata.renameExistingTable("foo", "");
    }

    @Test
    public void testUpdateSchemaDropColumn() throws Exception {
        TableSchema newSchema = tableSchemaGen.next();
        final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();
        Map<String, ColumnSchema> origColumns =
                ImmutableMap.copyOf(newSchema.getColumns());
        Map<String, ColumnSchema> newColumns = newSchema.getColumns();
        Map.Entry<String, ColumnSchema> removedColumn =
                newColumns.entrySet().iterator().next();
        newColumns.entrySet().remove(removedColumn);

        TableSchema origSchema = new TableSchema(origColumns,
                newSchema.getIndices());

        HTableInterface hTable = MockHTable.create();
        HBaseMetadata hbaseMetadata2 = new HBaseMetadata(hTable);
        hbaseMetadata2.putSchema(tableName, origSchema);

        long tableId = hbaseMetadata2.getTableId(tableName);
        hbaseMetadata2.updateSchema(tableId, origSchema, newSchema);

        TableSchema returnedSchema = hbaseMetadata2.getSchema(tableId);

        Assert.assertEquals(newSchema, returnedSchema);
        Assert.assertEquals(origSchema.getColumns().size() - 1,
                returnedSchema.getColumns().size());
        Assert.assertNull(returnedSchema.getColumns().get(removedColumn.getKey()));
    }

    @Test
    public void testUpdateSchemaAddColumn() throws Exception {
        TableSchema newSchema = tableSchemaGen.next();
        ColumnSchema newColumn = columnSchemaGen.next();
        String columnName = PrimitiveGenerators.strings().next();
        final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();
        Map<String, ColumnSchema> origColumns =
                ImmutableMap.copyOf(newSchema.getColumns());
        newSchema.getColumns().put(columnName, newColumn);

        TableSchema origSchema = new TableSchema(origColumns,
                newSchema.getIndices());

        HTableInterface hTable = MockHTable.create();
        HBaseMetadata hbaseMetadata2 = new HBaseMetadata(hTable);
        hbaseMetadata2.putSchema(tableName, origSchema);

        long tableId = hbaseMetadata2.getTableId(tableName);

        hbaseMetadata2.updateSchema(tableId, origSchema, newSchema);

        TableSchema returnedSchema = hbaseMetadata2.getSchema(tableId);

        Assert.assertEquals(newSchema, returnedSchema);
        Assert.assertEquals(origSchema.getColumns().size() + 1,
                returnedSchema.getColumns().size());
        Assert.assertEquals(newColumn, returnedSchema.getColumns().get(columnName));
    }

    @Test public void testAutoInc() throws Exception {
        TableSchema table = tableSchemaGen.next();
        final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();
        HTableInterface hTable = MockHTable.create();
        HBaseMetadata hbaseMetadata2 = new HBaseMetadata(hTable);
        hbaseMetadata2.putSchema(tableName, table);

        long tableId = hbaseMetadata2.getTableId(tableName);
        long value = longGen.next();
        Assert.assertEquals(hbaseMetadata2.getAutoInc(tableId), 0);
        Assert.assertEquals(hbaseMetadata2.incrementAutoInc(tableId, value), value);
        Assert.assertEquals(hbaseMetadata2.getAutoInc(tableId), value);

        hbaseMetadata2.truncateAutoInc(tableId);
        Assert.assertEquals(hbaseMetadata2.getAutoInc(tableId), 0);
    }

    @Test public void testRowCount() throws Exception {
        TableSchema table = tableSchemaGen.next();
        final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();
        HTableInterface hTable = MockHTable.create();
        HBaseMetadata hbaseMetadata2 = new HBaseMetadata(hTable);
        hbaseMetadata2.putSchema(tableName, table);

        long tableId = hbaseMetadata2.getTableId(tableName);
        long value = longGen.next();
        Assert.assertEquals(hbaseMetadata2.getRowCount(tableId), 0);
        Assert.assertEquals(hbaseMetadata2.incrementRowCount(tableId, value), value);
        Assert.assertEquals(hbaseMetadata2.getRowCount(tableId), value);

        hbaseMetadata2.truncateRowCount(tableId);
        Assert.assertEquals(hbaseMetadata2.getRowCount(tableId), 0);
    }
}
