/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 * Copyright 2013 Near Infinity Corporation.
 */


package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.Scanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Iterates through HBase rows
 */
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
