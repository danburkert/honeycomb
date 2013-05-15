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


package com.nearinfinity.honeycomb.hbase.bulkload;

import static com.google.common.base.Preconditions.checkNotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.primitives.Longs;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;

public class FieldParser {
    public static ByteBuffer parse(String val, ColumnSchema schema) throws ParseException {
        checkNotNull(val, "Should not be parsing null. Something went terribly wrong.");
        checkNotNull(schema, "Column metadata is null.");

        ColumnType type = schema.getType();

        if (val.length() == 0 && type != ColumnType.STRING
                && type != ColumnType.BINARY) {
            if (schema.getIsNullable()) {
                return null;
            }

            throw new IllegalArgumentException("Expected a value for a" +
                    " non-null SQL column, but no value was given.");
        }

        switch (type) {
            case LONG:
                return ByteBuffer.wrap(Longs.toByteArray(Long.parseLong(val)));
            case ULONG:
                BigInteger n = new BigInteger(val);
                if (n.compareTo(BigInteger.ZERO) == -1) {
                    throw new IllegalArgumentException("negative value provided for unsigned column. value: " + val);
                }
                return ByteBuffer.wrap(Longs.toByteArray(n.longValue()));
            case DOUBLE:
                return ByteBuffer.wrap(Bytes.toBytes(Double.parseDouble(val)));
            case DATE:
                return  extractDate(val, "yyyy-MM-dd",
                        "yyyy-MM-dd",
                        "yyyy/MM/dd",
                        "yyyy.MM.dd",
                        "yyyyMMdd");
            case TIME:
                return  extractDate(val, "HH:mm:ss",
                        "HH:mm:ss",
                        "HHmmss");
            case DATETIME:
                return extractDate(val, "yyyy-MM-dd HH:mm:ss",
                        "yyyy-MM-dd HH:mm:ss",
                        "yyyy/MM/dd HH:mm:ss",
                        "yyyy.MM.dd HH:mm:ss",
                        "yyyyMMdd HHmmss");
            case DECIMAL:
                return extractDecimal(val, schema);
            case STRING:
            case BINARY:
            default:
                return ByteBuffer.wrap(val.getBytes(Charset.forName("UTF-8")));
        }
    }

    private static ByteBuffer extractDate(String val, String dateFormat,
                                      String... parseFormats)
            throws ParseException {
        Date d = DateUtils.parseDateStrictly(val, parseFormats);
        SimpleDateFormat format = new SimpleDateFormat(dateFormat);
        return ByteBuffer.wrap(format.format(d).getBytes());
    }

    private static ByteBuffer extractDecimal(String val, ColumnSchema schema) {
        int precision = schema.getPrecision();
        int right_scale = schema.getScale();
        int left_scale = precision - 2;
        BigDecimal x = new BigDecimal(val);
        boolean is_negative = x.compareTo(BigDecimal.ZERO) == -1;
        x = x.abs();
        BigDecimal left = x.setScale(0, RoundingMode.DOWN);
        BigDecimal right = x.subtract(left).movePointRight(right_scale);
        int right_bytes_len = bytesFromDigits(right_scale);
        int left_bytes_len = bytesFromDigits(left_scale);
        byte[] left_bytes = left.toBigInteger().toByteArray();
        byte[] right_bytes = right.toBigInteger().toByteArray();
        // Bit twiddling is fun
        byte[] buff = new byte[left_bytes_len + right_bytes_len];

        System.arraycopy(left_bytes, 0, buff,
                left_bytes_len - left_bytes.length, left_bytes.length);
        System.arraycopy(right_bytes, 0, buff,
                right_bytes_len - right_bytes.length + left_bytes_len,
                right_bytes.length);

        buff[0] ^= -128; // Flip first bit, 0x80
        if (is_negative) { // Flip all bits
            for (int i = 0; i < buff.length; i++) {
                buff[i] ^= -1; // 0xff
            }
        }
        return ByteBuffer.wrap(buff);
    }

    public static int bytesFromDigits(int digits) {
        int ret = 0;
        ret += 4 * (digits / 9);
        ret += (digits % 9 + 1) / 2;
        return ret;
    }
}