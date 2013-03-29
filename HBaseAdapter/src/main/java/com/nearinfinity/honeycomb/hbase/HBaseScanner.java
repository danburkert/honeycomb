package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.Scanner;
import com.nearinfinity.honeycomb.config.Constants;
import com.nearinfinity.honeycomb.mysql.Row;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

public class HBaseScanner implements Scanner {
    private final ResultScanner scanner;
    private final Iterator<Result> resultIterator;

    public HBaseScanner(ResultScanner scanner) {
        checkNotNull(scanner, "Result scanner cannot be null.");
        this.scanner = scanner;
        this.resultIterator = this.scanner.iterator();
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
    public Row next() {
        return Row.deserialize(resultIterator.next().getValue(Constants.DEFAULT_COLUMN_FAMILY, new byte[0]));
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
