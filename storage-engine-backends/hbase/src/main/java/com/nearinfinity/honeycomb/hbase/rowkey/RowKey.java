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

/**
 * Implementing this interface allows an entity to represent a rowkey.  The rowkey
 * must always have an associated prefix for identification.
 */
public interface RowKey {
    /**
     * Performs the necessary encoding operations to generate the byte representation for this rowkey
     *
     * @return A byte array representing the encoded rowkey
     */
    public byte[] encode();

    /**
     * Retrieves the prefix associated with this rowkey for unique type identification
     *
     * @return The unique byte used to identify the rowkey
     */
    public byte getPrefix();
}
