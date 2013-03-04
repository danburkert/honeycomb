package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.MockHTable;
import com.nearinfinity.honeycomb.TableNotFoundException;
import com.nearinfinity.honeycomb.mysql.TableMetadataGenerator;
import com.nearinfinity.honeycomb.mysql.gen.TableMetadata;
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
    static Generator<TableMetadata> tableMetadataGen = new TableMetadataGenerator();
    static HBaseMetadata hbaseMetadata;
    static Map<String, TableMetadata> metadatas;

    @BeforeClass
    public static void setUp() throws Exception {
        hbaseMetadata = new HBaseMetadata(MockHTable.create());
        metadatas = new HashMap<String, TableMetadata>();
        for (TableMetadata metadata : Iterables.toIterable(tableMetadataGen)) {
            metadatas.put(metadata.getName(), metadata);
            hbaseMetadata.putTableMetadata(metadata);
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
    public void testTableMetadataGet() throws Exception {
        for (String tableName : metadatas.keySet()) {
            TableMetadata expected = metadatas.get(tableName);
            TableMetadata actual = hbaseMetadata.getTableMetadata(tableName);
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testTableMetadataDeleteRemovesAllRows() throws Exception {
        HTableInterface hTable = MockHTable.create();
        HBaseMetadata hbaseMetadata2 = new HBaseMetadata(hTable);
        TableMetadata metadata = tableMetadataGen.next();
        String tableName = metadata.getName();
        hbaseMetadata2.putTableMetadata(metadata);
        TableMetadata expected = hbaseMetadata2.getTableMetadata(tableName);
        Assert.assertEquals(metadata, expected);

        hbaseMetadata2.deleteColumnMetadata(tableName);
        ResultScanner results = hTable.getScanner(new Scan());
        Assert.assertTrue(results.next().getNoVersionMap().size() == 1); // Table id counter
        Assert.assertNull(results.next());
    }

    @Test(expected = TableNotFoundException.class)
    public void testTableMetadataDeleteRemovesTable() throws Exception {
        TableMetadata metadata = tableMetadataGen.next();
        String tableName = metadata.getName();
        hbaseMetadata.putTableMetadata(metadata);
        Assert.assertEquals(metadata, hbaseMetadata.getTableMetadata(tableName));
        hbaseMetadata.deleteColumnMetadata(tableName);
        hbaseMetadata.getTableMetadata(tableName);
    }
}