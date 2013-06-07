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


package com.nearinfinity.honeycomb.exceptions;

import com.google.common.base.Objects;


/**
 * Runtime exception used to indicate that a table could not be found
 * for a specific table name or table identifier
 */
public class TableNotFoundException extends RuntimeException {
    private String name;
    private long id;

    public TableNotFoundException(String name) {
        this.name = name;
    }

    public TableNotFoundException(Long tableId) {
        id = tableId;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this.getClass())
                .add("Name", name)
                .add("Id", id)
                .toString();
    }
}
