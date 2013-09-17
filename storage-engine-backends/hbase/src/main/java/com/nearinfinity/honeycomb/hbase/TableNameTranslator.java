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

import java.security.MessageDigest;

/**
 * MySQL 5.5 allows table names to include characters from the entire Unicode
 * BMP except NUL (U+0000) (https://dev.mysql.com/doc/refman/5.5/en/identifiers.html).
 * HBase is much more restrictive; it allows only 'word characters': [a-zA-Z_0-9-.].
 * As a result, we need a unique  translation from MySQL table names
 * to HBase table names.
 * <p/>
 * To accomplish this we replace the slash separating the MySQL database and
 * table names with a '.' (dot) character, and all other non-word characters with
 * the '_' (underscore) character.  Finally, because this transformation is not
 * homomorphic, we append a dot separator and a unique identifier based on the
 * original table name.
 * <p/>
 * Examples:
 * <p/>
 * "database/table" -> "database.table...."
 * "dātabase/table" -> "d_tabase.table...."
 * "dĀtabase/table" -> "d_tabase.table...."
 * "my-database/table" -> "my-database.table...."
 * "my.database/table" -> "my.database.table...."
 * "my_database/table" -> "my_database.table...."
 */
public class TableNameTranslator {
    private static char HBASE_SEP = '.';
    private static String SQL_SEP = "/";
    private static String NON_WORD_CHARS = "[^a-z^A-Z^_^0-9^.^-]";
    private static String REPLACEMENT = "_";

    public static String tableId(String tableName) {
        String[] parts = tableName.split(SQL_SEP);
        if (parts.length != 2) {
            throw new IllegalArgumentException("MySQL table name " + tableName + " is malformed.");
        }
        return replaceNonWordChars(parts[0]) + HBASE_SEP + replaceNonWordChars(parts[1]) + HBASE_SEP + hash(tableName);
    }

    private static String replaceNonWordChars(String s) {
        return s.replaceAll(NON_WORD_CHARS, REPLACEMENT);
    }

    private static String hash(String s) {
        try {
            MessageDigest hasher = MessageDigest.getInstance("MD5");
            return new String(hasher.digest(s.getBytes("utf8")), "utf8");
        } catch (Exception e) { // this should never happen
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}