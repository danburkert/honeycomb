package com.nearinfinity.honeycomb.hbase;

import com.google.common.collect.ImmutableMap;
import com.nearinfinity.honeycomb.MockHTable;
import com.nearinfinity.honeycomb.TableNotFoundException;
import com.nearinfinity.honeycomb.mysql.ColumnSchemaGenerator;
import com.nearinfinity.honeycomb.mysql.TableSchemaGenerator;
import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HBaseMetadataTest {
    static Generator<TableSchema> tableSchemaGen = new TableSchemaGenerator();
    static Generator<ColumnSchema> columnSchemaGen = new ColumnSchemaGenerator();
    static HBaseMetadata hbaseMetadata;
    static Map<String, TableSchema> tableSchemas;

    @BeforeClass
    public static void setUp() throws Exception {
        hbaseMetadata = new HBaseMetadata(MockHTable.create());
        tableSchemas = new HashMap<String, TableSchema>();
        for (TableSchema schema : Iterables.toIterable(tableSchemaGen)) {
            tableSchemas.put(schema.getName(), schema);
            hbaseMetadata.putSchema(schema);
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
        String tableName = schema.getName();
        hbaseMetadata2.putSchema(schema);
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
        String tableName = schema.getName();
        hbaseMetadata.putSchema(schema);
        long tableId = hbaseMetadata.getTableId(tableName);
        Assert.assertEquals(schema, hbaseMetadata.getSchema(tableId));
        hbaseMetadata.deleteSchema(tableName);
        hbaseMetadata.getSchema(tableId);
    }

    @Test
    public void testUpdateSchemaRenameTable() throws Exception {
        String originalName = "OriginalName";
        String newName = "NewName";
        TableSchema origSchema = tableSchemaGen.next();
        TableSchema newSchema = new TableSchema(newName, origSchema.getColumns());
        origSchema.setName(originalName);
        HTableInterface hTable = MockHTable.create();
        HBaseMetadata hbaseMetadata2 = new HBaseMetadata(hTable);
        hbaseMetadata2.putSchema(origSchema);
        long origId = hbaseMetadata2.getTableId(originalName);
        hbaseMetadata2.updateSchema(origSchema, newSchema);
        Assert.assertEquals(hbaseMetadata2.getTableId(newName), origId);
        Assert.assertEquals(origSchema.getColumns(),
                hbaseMetadata2.getSchema(origId).getColumns());
        Assert.assertEquals(hbaseMetadata2.getSchema(origId).getName(), newName);
    }

    @Test
    public void testUpdateSchemaDropColumn() throws Exception {
        TableSchema newSchema = tableSchemaGen.next();
        String tableName = newSchema.getName();
        Map<String, ColumnSchema> origColumns = ImmutableMap.copyOf(newSchema.getColumns());
        Map<String, ColumnSchema> newColumns = newSchema.getColumns();
        Map.Entry<String, ColumnSchema> removedColumn = newColumns.entrySet().iterator().next();
        newColumns.entrySet().remove(removedColumn);

        TableSchema origSchema = new TableSchema(tableName, origColumns);

        HTableInterface hTable = MockHTable.create();
        HBaseMetadata hbaseMetadata2 = new HBaseMetadata(hTable);
        hbaseMetadata2.putSchema(origSchema);
        hbaseMetadata2.updateSchema(origSchema, newSchema);

        long tableId = hbaseMetadata2.getTableId(tableName);
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
        String tableName = newSchema.getName();
        Map<String, ColumnSchema> origColumns = ImmutableMap.copyOf(newSchema.getColumns());
        newSchema.getColumns().put(columnName, newColumn);

        TableSchema origSchema = new TableSchema(tableName, origColumns);

        HTableInterface hTable = MockHTable.create();
        HBaseMetadata hbaseMetadata2 = new HBaseMetadata(hTable);
        hbaseMetadata2.putSchema(origSchema);
        hbaseMetadata2.updateSchema(origSchema, newSchema);

        long tableId = hbaseMetadata2.getTableId(tableName);
        TableSchema returnedSchema = hbaseMetadata2.getSchema(tableId);

        Assert.assertEquals(newSchema, returnedSchema);
        Assert.assertEquals(origSchema.getColumns().size() + 1,
                returnedSchema.getColumns().size());
        Assert.assertEquals(newColumn, returnedSchema.getColumns().get(columnName));
    }

    @Test public void testAutoInc() throws Exception {
        ColumnSchema column = new ColumnSchema(ColumnType.LONG, true, true, null, null, null);
        Map<String, ColumnSchema> columns = new HashMap<String, ColumnSchema>();
        columns.put("column1", column);
        TableSchema table = new TableSchema("table1", columns);

        HTableInterface hTable = MockHTable.create();
        HBaseMetadata hbaseMetadata2 = new HBaseMetadata(hTable);
        hbaseMetadata2.putSchema(table);

        long tableId = hbaseMetadata2.getTableId(table.getName());
        Assert.assertEquals(hbaseMetadata2.getAutoInc(tableId), 0);
        Assert.assertEquals(hbaseMetadata2.incrementAutoInc(tableId, 3), 3);
        Assert.assertEquals(hbaseMetadata2.getAutoInc(tableId), 3);

        hbaseMetadata2.truncateAutoInc(tableId);
        Assert.assertEquals(hbaseMetadata2.getAutoInc(tableId), 0);
    }
}