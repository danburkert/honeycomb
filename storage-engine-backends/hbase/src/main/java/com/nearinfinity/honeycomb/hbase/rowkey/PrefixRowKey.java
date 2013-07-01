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


package com.nearinfinity.honeycomb.hbase.rowkey;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Super class for rowkeys that only occur once, ie,
 * rowkeys that are shared across all tables.
 */
public abstract class PrefixRowKey implements RowKey {
    private final byte[] rowKey;

    /**
     * Creates a prefix rowkey with the provided rowkey content
     *
     * @param rowKey The rowkey content that this row represents, not null or empty
     */
    public PrefixRowKey(final byte[] rowKey) {
        checkNotNull(rowKey, "The rowkey is invalid");
        checkArgument(rowKey.length > 0, "The rowkey cannot be empty");
        this.rowKey = rowKey;
    }

    @Override
    public byte[] encode() {
        return rowKey;
    }

    @Override
    public byte getPrefix() {
        return rowKey[0];
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this.getClass())
                .add("Prefix", String.format("%02X", getPrefix()))
                .toString();
    }
}
