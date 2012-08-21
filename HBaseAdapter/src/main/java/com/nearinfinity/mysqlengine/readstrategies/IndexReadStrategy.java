package com.nearinfinity.mysqlengine.readstrategies;

import com.nearinfinity.mysqlengine.IndexConnection;

import java.io.IOException;

public interface IndexReadStrategy {
    public void setupResultScannerForConnection(IndexConnection conn, byte[] value) throws IOException;
}
