package com.nearinfinity.honeycomb.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.PrimitiveGenerators;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.MockHTable;
import com.nearinfinity.honeycomb.exceptions.TableNotFoundException;
import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import com.nearinfinity.honeycomb.mysql.generators.TableSchemaGenerator;


public class HBaseMetadataTest {
    private static final Generator<TableSchema> TABLE_SCHEMA_GEN = new TableSchemaGenerator();
    private static final Generator<Long> LONG_GEN = PrimitiveGenerators.longs();

    private static final String TABLE_NAME = "foo";
    private static final String COLUMN_NAME = "columnA";
    private static final String INDEX_NAME = "indexA";

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
        TableSchema schema = TABLE_SCHEMA_GEN.next();
        final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();

        hbaseMetadata.createTable(tableName, schema);

        long tableId = hbaseMetadata.getTableId(tableName);
        TableSchema expected = hbaseMetadata.getSchema(tableId);
        assertEquals(schema, expected);

        hbaseMetadata.deleteTable(tableName);
        ResultScanner results = table.getScanner(new Scan());
        assertTrue(results.next().getNoVersionMap().size() == 1); // Table id counter
        assertNull(results.next());
        results.close();
    }

    @Test(expected = TableNotFoundException.class)
    public void testRenameExistingTableNoAutoFlush() throws Exception {
        String originalName = "OriginalName";
        String newName = "NewName";

        TableSchema origSchema = TABLE_SCHEMA_GEN.next();

        // Configure the table to disable auto flush
        HTableInterface hTableSpy = PowerMockito.spy(MockHTable.create());
        Mockito.when(hTableSpy.isAutoFlush()).thenReturn(false);

        hbaseMetadata.createTable(originalName, origSchema);

        long origId = hbaseMetadata.getTableId(originalName);
        hbaseMetadata.renameExistingTable(originalName, newName);

        long newId = hbaseMetadata.getTableId(newName);

        assertEquals(origId, newId);
        assertEquals(origSchema.getColumns(), hbaseMetadata.getSchema(newId).getColumns());

        // Trying to access the id of the old table name will result in an exception
        hbaseMetadata.getTableId(originalName);

        hTableSpy.close();
    }

    @Test(expected = NullPointerException.class)
    public void testRenameExistingTableNullCurrentTableName() throws TableNotFoundException {
        hbaseMetadata.renameExistingTable(null, TABLE_NAME);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRenameExistingTableEmptyCurrentTableName() throws TableNotFoundException {
        hbaseMetadata.renameExistingTable("", TABLE_NAME);
    }

    @Test(expected = NullPointerException.class)
    public void testRenameExistingTableNullNewTableName() throws TableNotFoundException {
        hbaseMetadata.renameExistingTable(TABLE_NAME, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRenameExistingTableEmptyNewTableName() throws TableNotFoundException {
        hbaseMetadata.renameExistingTable(TABLE_NAME, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateIndexInvalidTableId() {
        final long invalidTableId = -1;
        hbaseMetadata.createTableIndex(invalidTableId, INDEX_NAME, new IndexSchema());
    }

    @Test(expected = NullPointerException.class)
    public void testCreateIndexNullIndexName() {
        hbaseMetadata.createTableIndex(1, null, new IndexSchema());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateIndexEmptyIndexName() {
        hbaseMetadata.createTableIndex(1, "", new IndexSchema());
    }

    @Test(expected = NullPointerException.class)
    public void testCreateIndexNullIndexSchema() {
        hbaseMetadata.createTableIndex(1, INDEX_NAME, null);
    }

    @Test
    public void testCreateIndex() {
        final Map<String, ColumnSchema> columns = ImmutableMap.<String, ColumnSchema>of(
                COLUMN_NAME, new ColumnSchema(ColumnType.LONG, true, false, 8, 0, 0));

        final TableSchema tableSchema = new TableSchema(columns, ImmutableMap.<String, IndexSchema>of());

        // Create a new table with the configured details
        hbaseMetadata.createTable(TABLE_NAME, tableSchema);
        final long tableId = hbaseMetadata.getTableId(TABLE_NAME);

        // Verify that the table schema has no indices after creation
        final TableSchema schemaBefore = hbaseMetadata.getSchema(tableId);
        assertNotNull(schemaBefore);
        assertTrue(schemaBefore.getIndices().isEmpty());

        // Verify that no indices exist after table creation
        assertTrue(hbaseMetadata.getIndexIds(tableId).isEmpty());

        // Add a new index to the table
        hbaseMetadata.createTableIndex(tableId, INDEX_NAME, new IndexSchema(ImmutableList.<String> of(COLUMN_NAME), false));

        // Verify that the table schema has been correctly updated

        final TableSchema schemaAfter = hbaseMetadata.getSchema(tableId);
        assertNotNull(schemaAfter);

        final Map<String, IndexSchema> schemaIndices = schemaAfter.getIndices();
        assertEquals(1, schemaIndices.size());
        assertTrue(schemaIndices.containsKey(INDEX_NAME));

        final IndexSchema newIndexDetails = schemaIndices.get(INDEX_NAME);
        assertNotNull(newIndexDetails);

        final List<String> indexColumns = newIndexDetails.getColumns();
        assertEquals(1, indexColumns.size());
        assertEquals(COLUMN_NAME, indexColumns.get(0));

        assertEquals(false, newIndexDetails.getIsUnique());

        // Verify that the new index has been stored correctly
        final Map<String, Long> tableIndexInfo = hbaseMetadata.getIndexIds(tableId);
        assertEquals(1, tableIndexInfo.size());
        assertTrue(tableIndexInfo.containsKey(INDEX_NAME));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteIndexInvalidTableId() {
        final long invalidTableId = -1;
        hbaseMetadata.deleteTableIndex(invalidTableId, INDEX_NAME);
    }

    @Test(expected = NullPointerException.class)
    public void testDeleteIndexNullIndexName() {
        hbaseMetadata.deleteTableIndex(1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteIndexEmptyIndexName() {
        hbaseMetadata.deleteTableIndex(1, "");
    }

    @Test
    public void testDeleteIndex() {
        final Map<String, ColumnSchema> columns = ImmutableMap.<String, ColumnSchema>of(
                COLUMN_NAME, new ColumnSchema(ColumnType.LONG, true, false, 8, 0, 0));

        final TableSchema tableSchema = new TableSchema(columns, ImmutableMap.<String, IndexSchema>of(
                INDEX_NAME, new IndexSchema(Lists.newArrayList(COLUMN_NAME), false)));

        // Create a new table with the configured details
        hbaseMetadata.createTable(TABLE_NAME, tableSchema);
        final long tableId = hbaseMetadata.getTableId(TABLE_NAME);


        // Verify that the table schema contains indices after creation

        final TableSchema schemaBefore = hbaseMetadata.getSchema(tableId);
        assertNotNull(schemaBefore);

        final Map<String, IndexSchema> schemaIndices = schemaBefore.getIndices();
        assertEquals(1, schemaIndices.size());
        assertTrue(schemaIndices.containsKey(INDEX_NAME));

        final IndexSchema newIndexDetails = schemaIndices.get(INDEX_NAME);
        assertNotNull(newIndexDetails);

        final List<String> indexColumns = newIndexDetails.getColumns();
        assertEquals(1, indexColumns.size());
        assertEquals(COLUMN_NAME, indexColumns.get(0));

        // Verify that the index exists after table creation
        final Map<String, Long> tableIndexInfo = hbaseMetadata.getIndexIds(tableId);
        assertEquals(1, tableIndexInfo.size());
        assertTrue(tableIndexInfo.containsKey(INDEX_NAME));

        // Remove an existing index from the table
        hbaseMetadata.deleteTableIndex(tableId, INDEX_NAME);

        // Verify that the table schema has been correctly updated
        final TableSchema schemaAfter = hbaseMetadata.getSchema(tableId);
        assertNotNull(schemaAfter);
        assertTrue(schemaAfter.getIndices().isEmpty());

        // Verify that the index has been removed correctly
        assertTrue(hbaseMetadata.getIndexIds(tableId).isEmpty());
    }

    @Test
    public void testAutoInc() throws Exception {
        TableSchema table = TABLE_SCHEMA_GEN.next();
        final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();

        hbaseMetadata.createTable(tableName, table);

        long tableId = hbaseMetadata.getTableId(tableName);
        long value = LONG_GEN.next();
        assertEquals(hbaseMetadata.getAutoInc(tableId), 0);
        assertEquals(hbaseMetadata.incrementAutoInc(tableId, value), value);
        assertEquals(hbaseMetadata.getAutoInc(tableId), value);

        hbaseMetadata.setAutoInc(tableId, 13);
        assertEquals(hbaseMetadata.getAutoInc(tableId), 13);
    }

    @Test
    public void testRowCount() throws Exception {
        TableSchema table = TABLE_SCHEMA_GEN.next();
        final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();

        hbaseMetadata.createTable(tableName, table);

        long tableId = hbaseMetadata.getTableId(tableName);
        long value = LONG_GEN.next();
        assertEquals(hbaseMetadata.getRowCount(tableId), 0);
        assertEquals(hbaseMetadata.incrementRowCount(tableId, value), value);
        assertEquals(hbaseMetadata.getRowCount(tableId), value);

        hbaseMetadata.truncateRowCount(tableId);
        assertEquals(hbaseMetadata.getRowCount(tableId), 0);
    }
}
