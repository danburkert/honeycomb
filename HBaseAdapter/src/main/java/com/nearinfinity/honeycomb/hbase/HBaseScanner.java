package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.Scanner;

import java.io.IOException;

public class HBaseScanner implements Scanner {
    @Override
    public void close() throws IOException {
        return;
    }

    @Override
    public Object next() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public void remove() {
        return;
    }
}
