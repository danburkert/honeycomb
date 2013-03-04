package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.MockHTable;
import com.nearinfinity.honeycomb.TableNotFoundException;
import com.nearinfinity.honeycomb.mysql.TableMetadataGenerator;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import net.java.quickcheck.Generator;
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
    static Generator<TableSchema> tableMetadataGen = new TableMetadataGenerator();
    static HBaseMetadata hbaseMetadata;
    static Map<String, TableSchema> metadatas;

    @BeforeClass
    public static void setUp() throws Exception {
        hbaseMetadata = new HBaseMetadata(MockHTable.create());
        metadatas = new HashMap<String, TableSchema>();
        for (TableSchema schema : Iterables.toIterable(tableMetadataGen)) {
            metadatas.put(schema.getName(), schema);
            hbaseMetadata.putSchema(schema);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        hbaseMetadata = null;
        metadatas = null;
    }

    @Test
    public void testUniqueTableIds() throws Exception {
        Set<Long> ids = new HashSet<Long>();
        Long id;
        for (String table : metadatas.keySet()) {
            id = hbaseMetadata.getTableId(table);
            Assert.assertFalse(ids.contains(id));
            ids.add(id);
        }
    }

    @Test
    public void testUniqueColumnIds() throws Exception {
        for (String table : metadatas.keySet()) {
            // If there are duplicate column ids BiMap will throw an
            // IllegalArgumentException, so no assertion needed.
            hbaseMetadata.getColumnIds(table);
        }
    }

    @Test
    public void testSchemaGet() throws Exception {
        for (String tableName : metadatas.keySet()) {
            TableSchema expected = metadatas.get(tableName);
            TableSchema actual = hbaseMetadata.getSchema(tableName);
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testSchemaDeleteRemovesAllRowIds() throws Exception {
        HTableInterface hTable = MockHTable.create();
        HBaseMetadata hbaseMetadata2 = new HBaseMetadata(hTable);
        TableSchema schema = tableMetadataGen.next();
        String tableName = schema.getName();
        hbaseMetadata2.putSchema(schema);
        TableSchema expected = hbaseMetadata2.getSchema(tableName);
        Assert.assertEquals(schema, expected);

        hbaseMetadata2.deleteSchema(tableName);
        ResultScanner results = hTable.getScanner(new Scan());
        Assert.assertTrue(results.next().getNoVersionMap().size() == 1); // Table id counter
        Assert.assertNull(results.next());
    }

    @Test(expected = TableNotFoundException.class)
    public void testSchemaDeleteRemovesTable() throws Exception {
        TableSchema schema = tableMetadataGen.next();
        String tableName = schema.getName();
        hbaseMetadata.putSchema(schema);
        Assert.assertEquals(schema, hbaseMetadata.getSchema(tableName));
        hbaseMetadata.deleteSchema(tableName);
        hbaseMetadata.getSchema(tableName);
    }
}