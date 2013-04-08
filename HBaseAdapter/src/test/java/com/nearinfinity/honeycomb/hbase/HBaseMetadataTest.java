package com.nearinfinity.honeycomb.hbase;


import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.ColumnSchemaFactory;
import com.nearinfinity.honeycomb.IndexSchemaFactory;
import com.nearinfinity.honeycomb.MockHTable;
import com.nearinfinity.honeycomb.TableSchemaFactory;
import com.nearinfinity.honeycomb.exceptions.TableNotFoundException;
import com.nearinfinity.honeycomb.mysql.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.IndexSchema;
import com.nearinfinity.honeycomb.mysql.TableSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.generators.TableSchemaGenerator;
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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;


public class HBaseMetadataTest {
    private static final Generator<TableSchema> TABLE_SCHEMA_GEN = new TableSchemaGenerator();
    private static final Generator<Long> LONG_GEN = PrimitiveGenerators.longs();
    private static final String TABLE_NAME = "foo";
    private static final String COLUMN_NAME = "columnA";
    private static final String INDEX_NAME = "indexA";
    private static final long INVALID_TABLE_ID = -1;
    @Mock
    private HTableProvider provider;
    private MockHTable table;
    private HBaseMetadata hbaseMetadata;
    private Predicate<IndexSchema> indexPredicate = new Predicate<IndexSchema>() {
        @Override
        public boolean apply(IndexSchema input) {
            return input.getIndexName().equals(INDEX_NAME);
        }
    };

    @Before
    public void testSetup() {
        MockitoAnnotations.initMocks(this);

        hbaseMetadata = new HBaseMetadata(provider);

        table = MockHTable.create();
        when(provider.get()).thenReturn(table);
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructMetadataNullProvider() {
        new HBaseMetadata(null);
    }

    @Test(expected = NullPointerException.class)
    public void testLookupTableIdNullTableName() {
        hbaseMetadata.getTableId(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLookupTableIdEmptyTableName() {
        hbaseMetadata.getTableId("");
    }

    @Test(expected = TableNotFoundException.class)
    public void testLookupTableIdUnknownTableName() {
        hbaseMetadata.getTableId(TABLE_NAME);
    }

    @Test
    public void testLookupTableIdValidTableName() {
        final TableSchema schema = TABLE_SCHEMA_GEN.next();
        final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();

        hbaseMetadata.createTable(tableName, schema);
        assertEquals(1, hbaseMetadata.getTableId(tableName));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLookupIndexIdsInvalidTableId() {
        hbaseMetadata.getIndexIds(INVALID_TABLE_ID);
    }

    @Test(expected = TableNotFoundException.class)
    public void testLookupIndexIdsUnknownTableId() {
        hbaseMetadata.getIndexIds(132);
    }

    @Test
    public void testLookupIndexIdsValidTableId() {
        final TableSchema tableSchema = TableSchemaFactory.createTableSchema(ImmutableMap.<String, ColumnSchema>of(), ImmutableMap.<String, IndexSchema>of(
                INDEX_NAME, IndexSchemaFactory.createIndexSchema(Lists.newArrayList(COLUMN_NAME), false, INDEX_NAME)));

        hbaseMetadata.createTable(TABLE_NAME, tableSchema);
        final long tableId = hbaseMetadata.getTableId(TABLE_NAME);

        final Map<String, Long> tableIndices = hbaseMetadata.getIndexIds(tableId);
        assertEquals(1, tableIndices.size());
        assertTrue(tableIndices.containsKey(INDEX_NAME));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLookupColumnIdsInvalidTableId() {
        hbaseMetadata.getColumnIds(INVALID_TABLE_ID);
    }

    @Test(expected = TableNotFoundException.class)
    public void testLookupColumnIdsUnknownTableId() {
        hbaseMetadata.getColumnIds(132);
    }

    @Test
    public void testLookupColumnIdsValidTableId() {
        final Map<String, ColumnSchema> columns = ImmutableMap.<String, ColumnSchema>of(
                COLUMN_NAME, ColumnSchemaFactory.createColumnSchema(ColumnType.LONG, true, false, 8, 0, 0));

        final TableSchema tableSchema = TableSchemaFactory.createTableSchema(columns, ImmutableMap.<String, IndexSchema>of());

        hbaseMetadata.createTable(TABLE_NAME, tableSchema);
        final long tableId = hbaseMetadata.getTableId(TABLE_NAME);

        final Map<String, Long> tableColumns = hbaseMetadata.getColumnIds(tableId);
        assertEquals(1, tableColumns.size());
        assertTrue(tableColumns.containsKey(COLUMN_NAME));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLookupTableSchemaInvalidTableId() {
        hbaseMetadata.getSchema(INVALID_TABLE_ID);
    }

    @Test(expected = TableNotFoundException.class)
    public void testLookupTableSchemaUnknownTableId() {
        final long unknownTableId = 2;
        hbaseMetadata.getSchema(unknownTableId);
    }

    @Test
    public void testLookupTableSchemaValidTableId() {
        final Map<String, ColumnSchema> columns = ImmutableMap.of(
                COLUMN_NAME, ColumnSchemaFactory.createColumnSchema(ColumnType.LONG, true, false, 8, 0, 0));

        final TableSchema tableSchema = TableSchemaFactory.createTableSchema(columns, ImmutableMap.<String, IndexSchema>of());

        hbaseMetadata.createTable(TABLE_NAME, tableSchema);
        final long tableId = hbaseMetadata.getTableId(TABLE_NAME);

        final TableSchema schemaTwo = hbaseMetadata.getSchema(tableId);
        assertEquals(1, schemaTwo.getColumns().size());
        assertTrue(Iterables.any(schemaTwo.getColumns(), new Predicate<ColumnSchema>() {
            @Override
            public boolean apply(ColumnSchema input) {
                return input.getColumnName().equals(COLUMN_NAME);
            }
        }));
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
        Collection<ColumnSchema> origSchemaColumns = origSchema.getColumns();
        TableSchema newSchema = hbaseMetadata.getSchema(newId);
        for (ColumnSchema columnSchema : newSchema.getColumns()) {
            assertTrue(origSchemaColumns.contains(columnSchema));
        }

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
        hbaseMetadata.createTableIndex(invalidTableId, INDEX_NAME, getIndexEmpty());
    }

    @Test(expected = NullPointerException.class)
    public void testCreateIndexNullIndexName() {
        hbaseMetadata.createTableIndex(1, null, getIndexEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateIndexEmptyIndexName() {
        hbaseMetadata.createTableIndex(1, "", getIndexEmpty());
    }

    private IndexSchema getIndexEmpty() {
        return new IndexSchema(null, "");
    }

    @Test(expected = NullPointerException.class)
    public void testCreateIndexNullIndexSchema() {
        hbaseMetadata.createTableIndex(1, INDEX_NAME, null);
    }

    @Test
    public void testCreateIndex() {
        final Map<String, ColumnSchema> columns = ImmutableMap.of(
                COLUMN_NAME, ColumnSchemaFactory.createColumnSchema(ColumnType.LONG, true, false, 8, 0, 0));

        final TableSchema tableSchema = TableSchemaFactory.createTableSchema(columns, ImmutableMap.<String, IndexSchema>of());

        // Create a new table with the configured details
        hbaseMetadata.createTable(TABLE_NAME, tableSchema);
        final long tableId = hbaseMetadata.getTableId(TABLE_NAME);

        // Verify that the table schema has no indices after creation
        final TableSchema schemaBefore = hbaseMetadata.getSchema(tableId);
        assertNotNull(schemaBefore);
        assertTrue(schemaBefore.getIndices().isEmpty());

        // Add a new index to the table
        hbaseMetadata.createTableIndex(tableId, INDEX_NAME,
                IndexSchemaFactory.createIndexSchema(ImmutableList.<String>of(COLUMN_NAME), false, INDEX_NAME));

        // Verify that the table schema has been correctly updated

        final TableSchema schemaAfter = hbaseMetadata.getSchema(tableId);
        assertNotNull(schemaAfter);

        final Collection<IndexSchema> schemaIndices = schemaAfter.getIndices();
        assertEquals(1, schemaIndices.size());


        final IndexSchema newIndexDetails = Iterables.find(schemaIndices, indexPredicate);
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
                COLUMN_NAME, ColumnSchemaFactory.createColumnSchema(ColumnType.LONG, true, false, 8, 0, 0));

        final TableSchema tableSchema = TableSchemaFactory.createTableSchema(columns, ImmutableMap.<String, IndexSchema>of(
                INDEX_NAME, IndexSchemaFactory.createIndexSchema(Lists.newArrayList(COLUMN_NAME), false, INDEX_NAME)));

        // Create a new table with the configured details
        hbaseMetadata.createTable(TABLE_NAME, tableSchema);
        final long tableId = hbaseMetadata.getTableId(TABLE_NAME);


        // Verify that the table schema contains indices after creation

        final TableSchema schemaBefore = hbaseMetadata.getSchema(tableId);
        assertNotNull(schemaBefore);

        final Collection<IndexSchema> schemaIndices = schemaBefore.getIndices();
        assertEquals(1, schemaIndices.size());

        final IndexSchema newIndexDetails = Iterables.find(schemaIndices, indexPredicate);
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
