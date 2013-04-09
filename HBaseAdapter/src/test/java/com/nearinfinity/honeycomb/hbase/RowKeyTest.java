package com.nearinfinity.honeycomb.hbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedBytes;
import com.nearinfinity.honeycomb.hbase.generators.RowKeyGenerator;
import com.nearinfinity.honeycomb.hbase.rowkey.RowKey;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.iterable.Iterables;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RowKeyTest {
    Generator<RowKey> rowKeyGen = new RowKeyGenerator();

    @Test
    public void testRowKeyEncSort() {
        List<RowKey> rowKeys = new ArrayList<RowKey>();
        List<byte[]> encodedRowKeys = new ArrayList<byte[]>();

        for (RowKey rowKey : Iterables.toIterable(rowKeyGen)) {
            rowKeys.add(rowKey);
            encodedRowKeys.add(rowKey.encode());
        }

        Collections.sort(rowKeys);
        Collections.sort(encodedRowKeys, UnsignedBytes.lexicographicalComparator());

        for (int i = 0; i < rowKeys.size(); i++) {
            RowKey rowKey = rowKeys.get(i);
            byte[] encodedRowKey = encodedRowKeys.get(i);

            Assert.assertArrayEquals(encodedRowKey, rowKey.encode());
        }
    }

    @Test
    public void testIndexRowKeyStrings() {
        String columnName = "c1";
        String indexName = "i1";
        ColumnSchema columnSchema = ColumnSchema.builder("default", ColumnType.DATETIME).build();
        IndexSchema indexSchema = new IndexSchema(ImmutableList.of(columnName), false, indexName);
        TableSchema tableSchema = new TableSchema(ImmutableList.of(columnSchema), ImmutableList.of(indexSchema));

        Generator<RowKey> rowkeysGen = RowKeyGenerator.getAscIndexRowKeyGenerator(tableSchema);
        List<RowKey> rowkeys = Lists.newArrayList(Iterables.toIterable(rowkeysGen));
        Collections.sort(rowkeys);

        List<byte[]> encodedRowkeys = Lists.newArrayList();
        for (RowKey rowkey : rowkeys) {
            encodedRowkeys.add(rowkey.encode());
        }

        Collections.sort(encodedRowkeys, new Bytes.ByteArrayComparator());

        for (int i = 0; i < rowkeys.size(); i++) {
            RowKey rowKey = rowkeys.get(i);
            byte[] encodedRowKey = encodedRowkeys.get(i);

            Assert.assertArrayEquals(encodedRowKey, rowKey.encode());
        }
    }
}