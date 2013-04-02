package com.nearinfinity.honeycomb.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRowBuilder;
import com.nearinfinity.honeycomb.hbase.rowkey.SortOrder;
import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;
import net.java.quickcheck.Generator;
import net.java.quickcheck.collection.Pair;
import net.java.quickcheck.generator.PrimitiveGenerators;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertTrue;

public class EncodingTest {
    @Test
    public void testAscendingCorrectlySortsLongs() {
        List<Pair<Long, byte[]>> rows = getPairs(SortOrder.Ascending);

        Collections.sort(rows, new RowComparator());

        for (int i = 1; i < rows.size(); i++) {
            Pair<Long, byte[]> previous = rows.get(i - 1);
            Pair<Long, byte[]> current = rows.get(i);
            assertTrue(previous.getFirst() < current.getFirst());
        }
    }

    @Test
    public void testDescendingCorrectlySortsLong() {
        List<Pair<Long, byte[]>> rows = getPairs(SortOrder.Descending);

        Collections.sort(rows, new RowComparator());

        for (int i = 1; i < rows.size(); i++) {
            Pair<Long, byte[]> previous = rows.get(i - 1);
            Pair<Long, byte[]> current = rows.get(i);
            assertTrue(previous.getFirst() > current.getFirst());
        }
    }

    private List<Pair<Long, byte[]>> getPairs(SortOrder sortOrder) {
        IndexRowBuilder builder = IndexRowBuilder
                .newBuilder(1, 1)
                .withSortOrder(sortOrder);
        Set<Long> numbers = Sets.newHashSet();
        List<Pair<Long, byte[]>> rows = Lists.newArrayList();
        Generator<Long> longs = PrimitiveGenerators.longs();
        Map<String, ByteBuffer> records = Maps.newHashMap();
        Map<String, ColumnSchema> columnSchemas = Maps.newHashMap();
        columnSchemas.put("c1", ColumnSchema.newBuilder().setType(ColumnType.LONG).build());
        List<String> columnOrder = Lists.newArrayList();
        columnOrder.add("c1");

        for (int i = 0; i < 100; i++) {
            numbers.add(longs.next());
        }

        for (long number : numbers) {
            records.put("c1",
                    (ByteBuffer) ByteBuffer.wrap(Longs.toByteArray(number)));
            rows.add(new Pair<Long, byte[]>(number, builder
                    .withUUID(UUID.randomUUID())
                    .withRecords(records, new IndexSchema(columnOrder, false), columnSchemas)
                    .build().encode()));
        }
        return rows;
    }

    private class RowComparator implements Comparator<Pair<Long, byte[]>> {
        @Override
        public int compare(Pair<Long, byte[]> o1, Pair<Long, byte[]> o2) {
            return UnsignedBytes.lexicographicalComparator().compare(o1.getSecond(), o2.getSecond());
        }
    }
}
