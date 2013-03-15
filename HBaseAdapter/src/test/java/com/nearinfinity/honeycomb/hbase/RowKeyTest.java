package com.nearinfinity.honeycomb.hbase;

import com.google.common.primitives.UnsignedBytes;
import com.nearinfinity.honeycomb.hbase.generators.RowKeyGenerator;
import com.nearinfinity.honeycomb.hbase.rowkey.*;
import com.nearinfinity.honeycomb.mysql.Util;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.iterable.Iterables;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

        Collections.sort(rowKeys, new RowKeyComparator());
        Collections.sort(encodedRowKeys, UnsignedBytes.lexicographicalComparator());

        for (int i = 0; i < rowKeys.size(); i++) {
            RowKey rowKey = rowKeys.get(i);
            byte[] encodedRowKey = encodedRowKeys.get(i);

            Assert.assertArrayEquals(encodedRowKey, rowKey.encode());
        }
    }

    private class RowKeyComparator implements Comparator<RowKey> {
        @Override
        public int compare(RowKey row1, RowKey row2) {
            Class row1Class = row1.getClass();
            int classCompare = row1.getPrefix() - row2.getPrefix();
            if (classCompare != 0) {
                return classCompare;
            }

            if (row1 instanceof PrefixRow) {
                return 0;
            } else if (row1 instanceof ColumnsRow) {
                return ColumnsRowCompare((ColumnsRow) row1, (ColumnsRow) row2);
            } else if (row1Class == DataRow.class) {
                return dataRowCompare((DataRow) row1, (DataRow) row2);
            } else if (row1Class == AscIndexRow.class) {
                return ascIndexRowCompare((AscIndexRow) row1, (AscIndexRow) row2);
            } else {
                return descIndexRowCompare((DescIndexRow) row1, (DescIndexRow) row2);
            }
        }

        private int ColumnsRowCompare(ColumnsRow row1, ColumnsRow row2) {
            return Long.signum(row1.getTableId() - row2.getTableId());
        }

        private int dataRowCompare(DataRow row1, DataRow row2) {
            int tableCompare = Long.signum(row1.getTableId() - row2.getTableId());
            if (tableCompare != 0) {
                return tableCompare;
            }
            return UnsignedBytes.lexicographicalComparator().compare(
                    Util.UUIDToBytes(row1.getUuid()),
                    Util.UUIDToBytes(row2.getUuid()));
        }

        private int ascIndexRowCompare(AscIndexRow row1, AscIndexRow row2) {
            return indexCompare(row1, row2, -1);
        }

        private int descIndexRowCompare(DescIndexRow row1, DescIndexRow row2) {
            return indexCompare(row1, row2, 1);
        }

        private int indexCompare(IndexRow row1, IndexRow row2, int nullOrder) {
            int compare;
            compare = Long.signum(row1.getTableId() - row2.getTableId());
            if (compare != 0) {
                return compare;
            }
            compare = Long.signum(row1.getIndexId() - row2.getIndexId());
            if (compare != 0) {
                return compare;
            }
            compare = recordsCompare(row1.getRecords(), row2.getRecords(), nullOrder);
            if (compare != 0) {
                return compare;
            }
            return UnsignedBytes.lexicographicalComparator().compare(
                    Util.UUIDToBytes(row1.getUuid()),
                    Util.UUIDToBytes(row2.getUuid()));
        }

        private int recordsCompare(List<byte[]> records1, List<byte[]> records2, int nullOrder) {
            byte[] value1, value2;
            int compare;
            if (records1.size() != records2.size()) {
                throw new IllegalArgumentException("Number of records in indices must match.");
            }
            for (int i = 0; i < records1.size(); i++) {
                value1 = records1.get(i);
                value2 = records2.get(i);
                if (value1 == value2) {
                    continue;
                }
                if (value1 == null) {
                    return nullOrder;
                }
                if (value2 == null) {
                    return nullOrder * -1;
                }
                compare = new ByteArrayComparator().compare(value1, value2);
                if (compare != 0) {
                    return compare;
                }
            }
            return 0;
        }
    }
}