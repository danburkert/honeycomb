package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.Scanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

public class HBaseScanner implements Scanner {
    private final ResultScanner scanner;
    private final Iterator<Result> resultIterator;
    private final byte[] columnFamily;

    public HBaseScanner(ResultScanner scanner, String columnFamily) {
        checkNotNull(scanner, "Result scanner cannot be null.");
        this.scanner = scanner;
        this.resultIterator = this.scanner.iterator();
        this.columnFamily = columnFamily.getBytes();
    }

    @Override
    public void close() throws IOException {
        scanner.close();
    }

    @Override
    public boolean hasNext() {
        return resultIterator.hasNext();
    }

    @Override
    public byte[] next() {
        Result next = resultIterator.next();
        if (next == null) {
            return null;
        }

        return next.getValue(columnFamily, new byte[0]);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
