package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.hbase.gen.ColumnMetadataRow;
import com.nearinfinity.honeycomb.hbase.gen.ColumnsRow;
import com.nearinfinity.honeycomb.hbase.gen.RootRow;
import com.nearinfinity.honeycomb.hbase.gen.RowKey;

import java.util.Comparator;

public class RowKeyComparator implements Comparator {
    @Override
    public int compare(Object o1, Object o2) {
        if (!(o1 instanceof RowKey) && !(o2 instanceof RowKey)) {throw new RuntimeException();}

        RowKey rowKey1 = (RowKey) o1;
        RowKey rowKey2 = (RowKey) o2;

        Object row1 = rowKey1.getContent();
        Object row2 = rowKey2.getContent();
        Class class1 = row1.getClass();
        Class class2 = row2.getClass();

        if (class1 == RootRow.class) {
            if (class2 == RootRow.class) return RootRowCompare((RootRow) row1, (RootRow) row2);
            else return -1;
        }
        else if (class1 == ColumnsRow.class) {
            if (class2 == RootRow.class) return 1;
            else if (class2 == ColumnsRow.class) return ColumnsRowCompare((ColumnsRow) row1, (ColumnsRow) row2);
            else return -1;
        }
        else if (class1 == ColumnMetadataRow.class) {
            if (class2 == RootRow.class || class2 == ColumnsRow.class) return 1;
            else if (class2 == ColumnMetadataRow.class) return ColumnMetadataRowCompare((ColumnMetadataRow) row1, (ColumnMetadataRow) row2);
            else return -1;
        }

        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private int RootRowCompare(RootRow row1, RootRow row2) {
        return 0;
    }
    private int ColumnsRowCompare(ColumnsRow row1, ColumnsRow row2) {
        return row1.getTableId().compareTo(row2.getTableId());
    }
    private int ColumnMetadataRowCompare(ColumnMetadataRow row1, ColumnMetadataRow row2) {
        int tableCompare = row1.getTableId().compareTo(row2.getTableId());
        int columnCompare = row1.getColumnId().compareTo(row2.getColumnId());

        if (tableCompare != 0) return tableCompare;
        else return columnCompare;
    }

}