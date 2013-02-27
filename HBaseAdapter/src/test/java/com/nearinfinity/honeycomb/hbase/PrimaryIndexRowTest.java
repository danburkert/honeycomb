package com.nearinfinity.honeycomb.hbase;

import com.google.common.primitives.UnsignedBytes;
import com.nearinfinity.honeycomb.hbase.gen.PrimaryIndexRow;
import com.nearinfinity.honeycomb.hbase.gen.RowKey;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.iterable.Iterables;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PrimaryIndexRowTest {
    Generator<PrimaryIndexRow> primaryIndexRowGen = new PrimaryIndexRowGenerator();

    /**
     * Test that row serialization and deserialization is isomorphic in the
     * serialization direction.
     * @throws Exception
     */
    @Test
    public void testSerDe() throws Exception {
        for(PrimaryIndexRow indexRow : Iterables.toIterable(new PrimaryIndexRowGenerator())) {
            RowKey rowKey = new RowKey(indexRow);
            Assert.assertEquals(rowKey, HBaseUtil.deserializeRowKey(HBaseUtil.serializeRowKey(rowKey)));
        }
    }

    @Test
    public void testSort() throws Exception {

        List<RowKey> rowKeys = new ArrayList<RowKey>();
        List<byte[]> serializedRowKeys = new ArrayList<byte[]>();

        for(PrimaryIndexRow indexRow : Iterables.toIterable(new PrimaryIndexRowGenerator())) {
            RowKey rowKey = new RowKey(indexRow);
            rowKeys.add(rowKey);
            serializedRowKeys.add(HBaseUtil.serializeRowKey(rowKey));
        }

        Collections.sort(serializedRowKeys, UnsignedBytes.lexicographicalComparator());
        Collections.sort(rowKeys);

        for(int i = 0; i < 4; i++) {
            byte[] serializedRowKey = serializedRowKeys.get(i);
//            System.out.println("Serialized: " + Util.generateHexString(serializedRowKey));
            System.out.println("Row: " + HBaseUtil.deserializeRowKey(serializedRowKey));
        }

    }
}
