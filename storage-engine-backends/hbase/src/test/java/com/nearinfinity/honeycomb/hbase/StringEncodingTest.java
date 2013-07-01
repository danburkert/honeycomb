package com.nearinfinity.honeycomb.hbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedBytes;
import com.nearinfinity.honeycomb.hbase.generators.RowKeyGenerator;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRowKey;
import com.nearinfinity.honeycomb.mysql.QueryKey;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import net.java.quickcheck.collection.Pair;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class StringEncodingTest {
    private static final String COLUMN = "c1";
    private static TableSchema tableSchema =
            new TableSchema(
                    ImmutableList.of(ColumnSchema.builder(COLUMN, ColumnType.STRING).setMaxLength(32).build()),
                    ImmutableList.of(new IndexSchema("i1", ImmutableList.of(COLUMN), false))
            );

    @Test
    public void testAscendingCorrectlySortsStrings() {
        List<Pair<String, byte[]>> rows = Lists.newArrayList();
        RowKeyGenerator.IndexRowKeyGenerator rowKeyGen =
                RowKeyGenerator.getAscIndexRowKeyGenerator(tableSchema);
        addPairs(rows, rowKeyGen);

        Collections.sort(rows, new RowComparator());

        for (int i = 1; i < rows.size(); i++) {
            Pair<String, byte[]> previous = rows.get(i - 1);
            Pair<String, byte[]> current = rows.get(i);
            assertTrueWithPairs(previous.getFirst().compareTo(current.getFirst()) <= 0, previous, current);
        }
    }

    @Test
    public void testDescendingCorrectlySortsStrings() {
        List<Pair<String, byte[]>> rows = Lists.newArrayList();
        RowKeyGenerator.IndexRowKeyGenerator rowKeyGen =
                RowKeyGenerator.getDescIndexRowKeyGenerator(tableSchema);
        addPairs(rows, rowKeyGen);

        Collections.sort(rows, new RowComparator());

        for (int i = 1; i < rows.size(); i++) {
            Pair<String, byte[]> previous = rows.get(i - 1);
            Pair<String, byte[]> current = rows.get(i);
            assertTrueWithPairs(previous.getFirst().compareTo(current.getFirst()) >= 0, previous, current);
        }
    }

    private void addPairs(List<Pair<String, byte[]>> rows, RowKeyGenerator.IndexRowKeyGenerator rowKeyGen) {
        Pair<IndexRowKey, QueryKey> pair;
        for (int i = 0; i < 200; i++) {
            pair = rowKeyGen.nextWithQueryKey();
            if (pair.getSecond().getKeys().get(COLUMN) != null) {
                rows.add(new Pair<String, byte[]>(
                        new String(pair.getSecond().getKeys().get(COLUMN).array()),
                        pair.getFirst().encode()));
            } else {
                i--;
            }
        }
    }

    private void assertTrueWithPairs(boolean condition, Pair<String, byte[]> previous, Pair<String, byte[]> current) {
        assertTrue(previous.getFirst() + " / " +
                Bytes.toStringBinary(previous.getSecond()) + " : " +
                current.getFirst() + " / " +
                Bytes.toStringBinary(current.getSecond()), condition);
    }

    private class RowComparator implements Comparator<Pair<String, byte[]>> {
        private final Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
        @Override
        public int compare(Pair<String, byte[]> o1, Pair<String, byte[]> o2) {
            return comparator.compare(o1.getSecond(), o2.getSecond());
        }
    }
}
