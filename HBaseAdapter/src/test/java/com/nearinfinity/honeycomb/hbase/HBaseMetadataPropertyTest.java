package com.nearinfinity.honeycomb.hbase;

import com.google.inject.Provider;
import com.nearinfinity.honeycomb.MockHTable;
import com.nearinfinity.honeycomb.TableNotFoundException;
import com.nearinfinity.honeycomb.mysql.TableSchemaGenerator;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import net.java.quickcheck.Generator;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HBaseMetadataPropertyTest {
    static HBaseMetadata hbaseMetadata;
    static Map<String, TableSchema> tableSchemas;
    static HTableProvider provider;
    static MockHTable table;
    private static Generator<TableSchema> tableSchemaGen = new TableSchemaGenerator();

    @AfterClass
    public static void tearDown() throws Exception {
        hbaseMetadata = null;
        tableSchemas = null;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        provider = mock(HTableProvider.class);
        table = MockHTable.create();
        when(provider.get()).thenReturn(table);
        hbaseMetadata = getHBaseMetadata();
        tableSchemas = new HashMap<String, TableSchema>();
        for (int i = 0; i < 20; i++) {
            TableSchema schema = tableSchemaGen.next();
            final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();
            tableSchemas.put(tableName, schema);
            hbaseMetadata.putSchema(tableName, schema);
        }
    }

    private static HBaseMetadata getHBaseMetadata() {
        return new HBaseMetadata(provider);
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
}
