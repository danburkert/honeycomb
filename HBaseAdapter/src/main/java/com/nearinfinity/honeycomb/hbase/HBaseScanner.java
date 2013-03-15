package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.Scanner;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.mysql.Row;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Iterator;

public class HBaseScanner implements Scanner {
    private final ResultScanner scanner;
    private final Iterator<Result> resultIterator;

    public HBaseScanner(ResultScanner scanner) {
        this.scanner = scanner;
        this.resultIterator = this.scanner.iterator();
    }

    @Override
    public void close() throws IOException {
        scanner.close();
    }

    @Override
    public Iterator<Row> iterator() {
        return new Iterator<Row>() {
            @Override
            public boolean hasNext() {
                return resultIterator.hasNext();
            }

            @Override
            public Row next() {
                try {
                    return Row.deserialize(resultIterator.next().getValue(Constants.NIC, new byte[0]));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void remove() {
            }
        };
    }
}
