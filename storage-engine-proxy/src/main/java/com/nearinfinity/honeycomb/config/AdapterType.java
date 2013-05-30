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
 * Copyright 2013 Altamira Corporation.
 */


package com.nearinfinity.honeycomb.config;

/**
 * Defines the available storage engine backend adapters that may be used
 * by the storage proxy component
 */
public enum AdapterType {
    /**
     * HBase adapter
     */
    HBASE("hbase", "com.nearinfinity.honeycomb.hbase.HBaseModule"),
    /**
     * In-memory adapter
     */
    MEMORY("memory", "com.nearinfinity.honeycomb.memory.MemoryModule");
    private String name;
    private String moduleClass;

    /**
     * Creates a storage engine backend adapter type
     *
     * @param name        The name used to represent this adapter
     * @param moduleClass The fully qualified path to the class used to load this adapter
     */
    AdapterType(String name, String moduleClass) {
        this.name = name;
        this.moduleClass = moduleClass;
    }

    /**
     * Retrieve the name of this adapter.
     *
     * @return Adapter name
     */
    public String getName() {
        return name;
    }

    /**
     * Retrieve the class path of this adapter
     *
     * @return Class path string
     */
    public String getModuleClass() {
        return moduleClass;
    }
}
