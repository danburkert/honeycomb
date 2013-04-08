package com.nearinfinity.honeycomb.hbase;

import com.google.common.collect.*;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import com.nearinfinity.honeycomb.ColumnSchemaFactory;
import com.nearinfinity.honeycomb.IndexSchemaFactory;
import com.nearinfinity.honeycomb.TableSchemaFactory;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRowKeyBuilder;
import com.nearinfinity.honeycomb.hbase.rowkey.SortOrder;
import com.nearinfinity.honeycomb.mysql.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.IndexSchema;
import com.nearinfinity.honeycomb.mysql.TableSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
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
        IndexRowKeyBuilder builder = IndexRowKeyBuilder
                .newBuilder(1, 1)
                .withSortOrder(sortOrder);
        Set<Long> numbers = Sets.newHashSet();
        List<Pair<Long, byte[]>> rows = Lists.newArrayList();
        Generator<Long> longs = PrimitiveGenerators.longs();
        Map<String, ByteBuffer> records = Maps.newHashMap();
        Map<String, ColumnSchema> columnSchemas = Maps.newHashMap();
        ColumnSchema c1 = ColumnSchemaFactory.createColumnSchema();
        c1.setType(ColumnType.LONG);
        columnSchemas.put("c1", c1);
        IndexSchema indexSchema = IndexSchemaFactory.createIndexSchema(ImmutableList.<String>of("c1"), false);
        TableSchema tableSchema = TableSchemaFactory.createTableSchema(columnSchemas,
                ImmutableMap.<String, IndexSchema>of("c1", indexSchema));

        for (int i = 0; i < 100; i++) {
            numbers.add(longs.next());
        }

        for (long number : numbers) {
            records.put("c1",
                    (ByteBuffer) ByteBuffer.wrap(Longs.toByteArray(number)));
            rows.add(new Pair<Long, byte[]>(number, builder
                    .withUUID(UUID.randomUUID())
                    .withQueryValues(records, indexSchema.getColumns(), tableSchema)
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
