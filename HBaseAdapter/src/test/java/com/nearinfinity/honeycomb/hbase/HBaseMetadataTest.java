package com.nearinfinity.honeycomb.hbase;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Provider;
import com.nearinfinity.honeycomb.MockHTable;
import com.nearinfinity.honeycomb.TableNotFoundException;
import com.nearinfinity.honeycomb.mysql.ColumnSchemaGenerator;
import com.nearinfinity.honeycomb.mysql.TableSchemaGenerator;
import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.PrimitiveGenerators;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;

import java.util.Map;

import static org.mockito.Mockito.when;

public class HBaseMetadataTest {
    private static Generator<TableSchema> tableSchemaGen = new TableSchemaGenerator();
    private static Generator<ColumnSchema> columnSchemaGen = new ColumnSchemaGenerator();
    private static Generator<Long> longGen = PrimitiveGenerators.longs();
    @Mock
    Provider<HTableInterface> provider;
    MockHTable table;

    private HBaseMetadata getHBaseMetadata() {
        return new HBaseMetadata(provider);
    }

    @Before
    public void testSetup() {
        MockitoAnnotations.initMocks(this);
        table = MockHTable.create();
        when(provider.get()).thenReturn(table);
    }

    @Test
    public void testSchemaDeleteRemovesAllRowIds() throws Exception {
        HBaseMetadata hbaseMetadata2 = getHBaseMetadata();
        TableSchema schema = tableSchemaGen.next();
        final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();
        hbaseMetadata2.putSchema(tableName, schema);
        long tableId = hbaseMetadata2.getTableId(tableName);
        TableSchema expected = hbaseMetadata2.getSchema(tableId);
        Assert.assertEquals(schema, expected);

        hbaseMetadata2.deleteSchema(tableName);
        ResultScanner results = table.getScanner(new Scan());
        Assert.assertTrue(results.next().getNoVersionMap().size() == 1); // Table id counter
        Assert.assertNull(results.next());
    }

    @Test(expected = TableNotFoundException.class)
    public void testRenameExistingTableNoAutoFlush() throws Exception {
        String originalName = "OriginalName";
        String newName = "NewName";

        TableSchema origSchema = tableSchemaGen.next();

        // Configure the table to disable auto flush
        HTableInterface hTableSpy = PowerMockito.spy(MockHTable.create());
        Mockito.when(hTableSpy.isAutoFlush()).thenReturn(false);

        HBaseMetadata hbaseMetadataNoFlush = getHBaseMetadata();

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

        HBaseMetadata hbaseMetadata2 = getHBaseMetadata();
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

        HBaseMetadata hbaseMetadata2 = getHBaseMetadata();
        hbaseMetadata2.putSchema(tableName, origSchema);

        long tableId = hbaseMetadata2.getTableId(tableName);

        hbaseMetadata2.updateSchema(tableId, origSchema, newSchema);

        TableSchema returnedSchema = hbaseMetadata2.getSchema(tableId);

        Assert.assertEquals(newSchema, returnedSchema);
        Assert.assertEquals(origSchema.getColumns().size() + 1,
                returnedSchema.getColumns().size());
        Assert.assertEquals(newColumn, returnedSchema.getColumns().get(columnName));
    }

    @Test
    public void testAutoInc() throws Exception {
        TableSchema table = tableSchemaGen.next();
        final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();

        HBaseMetadata hbaseMetadata2 = getHBaseMetadata();
        hbaseMetadata2.putSchema(tableName, table);

        long tableId = hbaseMetadata2.getTableId(tableName);
        long value = longGen.next();
        Assert.assertEquals(hbaseMetadata2.getAutoInc(tableId), 0);
        Assert.assertEquals(hbaseMetadata2.incrementAutoInc(tableId, value), value);
        Assert.assertEquals(hbaseMetadata2.getAutoInc(tableId), value);

        hbaseMetadata2.truncateAutoInc(tableId);
        Assert.assertEquals(hbaseMetadata2.getAutoInc(tableId), 0);
    }

    @Test
    public void testRowCount() throws Exception {
        TableSchema table = tableSchemaGen.next();
        final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();
        HBaseMetadata hbaseMetadata2 = getHBaseMetadata();
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
