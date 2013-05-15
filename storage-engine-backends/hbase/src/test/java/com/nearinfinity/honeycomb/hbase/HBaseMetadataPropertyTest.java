package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.MockHTable;
import com.nearinfinity.honeycomb.exceptions.TableNotFoundException;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import com.nearinfinity.honeycomb.mysql.generators.TableSchemaGenerator;
import net.java.quickcheck.Generator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HBaseMetadataPropertyTest {
    private static HBaseMetadata hbaseMetadata;
    private static Map<String, TableSchema> tableSchemas = new HashMap<String, TableSchema>();
    private static HTableProvider provider;
    private static MockHTable table = MockHTable.create();
    private static Generator<TableSchema> tableSchemaGen = new TableSchemaGenerator();

    @AfterClass
    public static void tearDown() throws Exception {
        hbaseMetadata = null;
        tableSchemas = null;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        provider = mock(HTableProvider.class);
        when(provider.get()).thenReturn(table);

        hbaseMetadata = new HBaseMetadata(provider);
        hbaseMetadata.setColumnFamily("nic");

        for (int i = 0; i < 20; i++) {
            TableSchema schema = tableSchemaGen.next();
            final String tableName = TableSchemaGenerator.MYSQL_NAME_GEN.next();
            tableSchemas.put(tableName, schema);
            hbaseMetadata.createTable(tableName, schema);
        }
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
        hbaseMetadata.createTable(tableName, schema);

        long tableId = hbaseMetadata.getTableId(tableName);
        Assert.assertEquals(schema, hbaseMetadata.getSchema(tableId));

        hbaseMetadata.deleteTable(tableName);
        hbaseMetadata.getSchema(tableId);
    }
}
