package com.nearinfinity.honeycomb.hbase;

import com.google.common.primitives.UnsignedBytes;
import com.nearinfinity.honeycomb.hbase.gen.RowKey;
import com.nearinfinity.honeycomb.mysql.Util;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.iterable.Iterables;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RowKeyTest {
    Generator<RowKey> rowKeyGen = new RowKeyGenerator();

    /**
     * Test that RowKey serialization and deserialization is isomorphic in the
     * serialization direction.
     * @throws Exception
     */
    @Test
    public void testSerDe() throws Exception {
        for(RowKey rowKey : Iterables.toIterable(rowKeyGen)) {
            Assert.assertEquals(rowKey, HBaseUtil.deserializeRowKey(HBaseUtil.serializeRowKey(rowKey)));
        }
    }

    @Test
    public void testSort() throws Exception {

        List<RowKey> rowKeys = new ArrayList<RowKey>();
        List<byte[]> serializedRowKeys = new ArrayList<byte[]>();

        for(RowKey rowKey : Iterables.toIterable(rowKeyGen)) {
            rowKeys.add(rowKey);
            serializedRowKeys.add(HBaseUtil.serializeRowKey(rowKey));
        }

        Collections.sort(serializedRowKeys, UnsignedBytes.lexicographicalComparator());
        Collections.sort(rowKeys, new RowKeyComparator());

        for(int i = 0; i < rowKeys.size(); i++) {
            System.out.println("             Row: " + rowKeys.get(i));
            System.out.println("Deserialized Row: " + HBaseUtil.deserializeRowKey(serializedRowKeys.get(i)));
            System.out.println("Bytes:            " + Util.generateHexString(serializedRowKeys.get(i)));
            System.out.println();

        }



    }

}
