package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.MockHTable;
import com.nearinfinity.honeycomb.TableNotFoundException;
import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import com.nearinfinity.honeycomb.mysql.generators.ColumnSchemaGenerator;
import com.nearinfinity.honeycomb.mysql.generators.TableSchemaGenerator;
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

import java.io.IOException;

import static org.mockito.Mockito.when;

public class HBaseMetadataTest {
    private static final Generator<TableSchema> tableSchemaGen = new TableSchemaGenerator();
    private static final Generator<ColumnSchema> columnSchemaGen = new ColumnSchemaGenerator();
    private static final Generator<Long> longGen = PrimitiveGenerators.longs();

    private static final String DUMMY_TABLE_NAME = "foo";

    @Mock
    private HTableProvider provider;

    private MockHTable table;

    private HBaseMetadata hbaseMetadata;


    @Before
    public void testSetup() {
        MockitoAnnotations.initMocks(this);

        hbaseMetadata = new HBaseMetadata(provider);

        table = MockHTable.create();
        when(provider.get()).thenReturn(table);
    }

    @Test
    public void testSchemaDeleteRemovesAllRowIds() throws Exception {
        TableSchema schema = tableSchemaGen.next();
        final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();

        hbaseMetadata.createTable(tableName, schema);

        long tableId = hbaseMetadata.getTableId(tableName);
        TableSchema expected = hbaseMetadata.getSchema(tableId);
        Assert.assertEquals(schema, expected);

        hbaseMetadata.deleteTable(tableName);
        ResultScanner results = table.getScanner(new Scan());
        Assert.assertTrue(results.next().getNoVersionMap().size() == 1); // Table id counter
        Assert.assertNull(results.next());
        results.close();
    }

    @Test(expected = TableNotFoundException.class)
    public void testRenameExistingTableNoAutoFlush() throws Exception {
        String originalName = "OriginalName";
        String newName = "NewName";

        TableSchema origSchema = tableSchemaGen.next();

        // Configure the table to disable auto flush
        HTableInterface hTableSpy = PowerMockito.spy(MockHTable.create());
        Mockito.when(hTableSpy.isAutoFlush()).thenReturn(false);

        hbaseMetadata.createTable(originalName, origSchema);

        long origId = hbaseMetadata.getTableId(originalName);
        hbaseMetadata.renameExistingTable(originalName, newName);

        long newId = hbaseMetadata.getTableId(newName);

        Assert.assertEquals(origId, newId);
        Assert.assertEquals(origSchema.getColumns(), hbaseMetadata.getSchema(newId).getColumns());

        // Trying to access the id of the old table name will result in an exception
        hbaseMetadata.getTableId(originalName);

        hTableSpy.close();
    }

    @Test(expected = NullPointerException.class)
    public void testRenameExistingTableNullCurrentTableName() throws IOException, TableNotFoundException {
        hbaseMetadata.renameExistingTable(null, DUMMY_TABLE_NAME);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRenameExistingTableEmptyCurrentTableName() throws IOException, TableNotFoundException {
        hbaseMetadata.renameExistingTable("", DUMMY_TABLE_NAME);
    }

    @Test(expected = NullPointerException.class)
    public void testRenameExistingTableNullNewTableName() throws IOException, TableNotFoundException {
        hbaseMetadata.renameExistingTable(DUMMY_TABLE_NAME, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRenameExistingTableEmptyNewTableName() throws IOException, TableNotFoundException {
        hbaseMetadata.renameExistingTable(DUMMY_TABLE_NAME, "");
    }

    @Test
    public void testAutoInc() throws Exception {
        TableSchema table = tableSchemaGen.next();
        final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();

        hbaseMetadata.createTable(tableName, table);

        long tableId = hbaseMetadata.getTableId(tableName);
        long value = longGen.next();
        Assert.assertEquals(hbaseMetadata.getAutoInc(tableId), 0);
        Assert.assertEquals(hbaseMetadata.incrementAutoInc(tableId, value), value);
        Assert.assertEquals(hbaseMetadata.getAutoInc(tableId), value);

        hbaseMetadata.setAutoInc(tableId, 13);
        Assert.assertEquals(hbaseMetadata.getAutoInc(tableId), 13);
    }

    @Test
    public void testRowCount() throws Exception {
        TableSchema table = tableSchemaGen.next();
        final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();

        hbaseMetadata.createTable(tableName, table);

        long tableId = hbaseMetadata.getTableId(tableName);
        long value = longGen.next();
        Assert.assertEquals(hbaseMetadata.getRowCount(tableId), 0);
        Assert.assertEquals(hbaseMetadata.incrementRowCount(tableId, value), value);
        Assert.assertEquals(hbaseMetadata.getRowCount(tableId), value);

        hbaseMetadata.truncateRowCount(tableId);
        Assert.assertEquals(hbaseMetadata.getRowCount(tableId), 0);
    }
}
