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


package com.nearinfinity.honeycomb.exceptions;

import static java.lang.String.format;

import com.google.common.base.Objects;

/**
 * Runtime exception used to represent the state in which an unknown schema version
 * for a serialized container type has been encountered
 */
public class UnknownSchemaVersionException extends RuntimeException {
    private final int unknownVersion;
    private final int supportedVersion;

    /**
     *
     *
     * @param invalidVersion The unknown schema version that was encountered
     * @param expectedVersion The supported schema version that was expected
     */
    public UnknownSchemaVersionException(final int invalidVersion, final int expectedVersion) {
        super(format("Unsupported serialized schema version encountered: %d, expected :%d", invalidVersion, expectedVersion));

        unknownVersion = invalidVersion;
        supportedVersion = expectedVersion;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this.getClass())
                .add("Unknown Version:", unknownVersion)
                .add("Supported Version:", supportedVersion)
                .toString();
    }
}
