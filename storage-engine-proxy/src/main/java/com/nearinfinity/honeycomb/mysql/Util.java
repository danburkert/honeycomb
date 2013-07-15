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


package com.nearinfinity.honeycomb.mysql;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.nearinfinity.honeycomb.exceptions.RuntimeIOException;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;

/**
 * Utility class containing helper functions.
 */
public class Util {
    public static final int UUID_WIDTH = 16;
    private static final Logger logger = Logger.getLogger(Util.class);

    /**
     * Returns a byte wide buffer from a {@link UUID}.
     *
     * @param uuid The {@link UUID} to convert
     * @return A byte array representation that is {@value #UUID_WIDTH} bytes wide
     */
    public static byte[] UUIDToBytes(UUID uuid) {
        checkNotNull(uuid, "uuid must not be null.");
        return ByteBuffer.allocate(UUID_WIDTH)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    /**
     * Create a {@link UUID} from the provided byte array.
     *
     * @param bytes A byte array that must be {@value #UUID_WIDTH} bytes wide, not null
     * @return A {@link UUID} representation
     */
    public static UUID bytesToUUID(byte[] bytes) {
        checkNotNull(bytes, "bytes must not be null.");
        checkArgument(bytes.length == UUID_WIDTH, "bytes must be of length " + UUID_WIDTH + ".");
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    /**
     * Combine many byte arrays into one
     *
     * @param arrays List of byte arrays
     * @return Combined byte array
     */
    public static byte[] appendByteArrays(final List<byte[]> arrays) {
        checkNotNull(arrays);

        int size = 0;
        for (final byte[] array : arrays) {
            size += array.length;
        }
        final ByteBuffer bb = ByteBuffer.allocate(size);
        for (final byte[] array : arrays) {
            bb.put(array);
        }
        return bb.array();
    }

    /**
     * Combine a variable number of byte arrays into one with a prefix at the beginning of the combined array.
     *
     * @param prefix Byte prefix
     * @param arrays Variable number of byte arrays
     * @return Combined byte array
     */
    public static byte[] appendByteArraysWithPrefix(final byte prefix, final byte[]... arrays) {
        final List<byte[]> elements = Lists.newArrayList(new byte[] {prefix});
        elements.addAll(Arrays.asList(arrays));

        return appendByteArrays(elements);
    }

    /**
     * Serialize an object to a byte array
     *
     * @param obj    The object to serialize
     * @param writer The datum writer for the class
     * @return Serialized row
     */
    public static <T> byte[] serializeAvroObject(T obj, DatumWriter<T> writer) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        try {
            writer.write(obj, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw serializationError(obj, e);
        }

        return out.toByteArray();
    }

    /**
     * Deserialize the provided serialized data into an instance of the specified class type
     *
     * @param serializedData a buffer containing the serialized data
     * @param reader         the datum reader for the class
     * @return A new instance of the specified class representing the deserialized data
     */
    public static <T> T deserializeAvroObject(byte[] serializedData, DatumReader<T> reader) {
        checkNotNull(serializedData);
        checkNotNull(reader);

        Decoder binaryDecoder = DecoderFactory.get().binaryDecoder(serializedData, null);
        try {
            return reader.read(null, binaryDecoder);
        } catch (IOException e) {
            throw deserializationError(serializedData, e, null);
        }
    }

    /**
     * Create a hex string for a byte string. The string will be formatted {@code "A2BE"}
     *
     * @param bytes Byte array to be formatted as a hex string
     * @return Hex string representation
     */
    public static String generateHexString(final byte[] bytes) {
        checkNotNull(bytes);

        final StringBuilder builder = new StringBuilder();
        for (final byte b : bytes) {
            builder.append(String.format("%02X", b));
        }

        return builder.toString();
    }

    /**
     * Quietly close a {@link Closeable} suppressing the IOException thrown
     *
     * @param closeable Closeable
     */
    public static void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            logger.error(String.format("IOException thrown while closing resource of type %s", closeable.getClass().getName()), e);
            throw new RuntimeIOException(e);
        }
    }

    /**
     * Retrieve from a list of indices which ones have been changed.
     *
     * @param indices    Table indices
     * @param oldRecords Old MySQL row
     * @param newRecords New MySQL row
     * @return List of changed indices
     */
    public static ImmutableList<IndexSchema> getChangedIndices(Collection<IndexSchema> indices,
                                                               Map<String, ByteBuffer> oldRecords,
                                                               Map<String, ByteBuffer> newRecords) {
        if (indices.isEmpty()) {
            return ImmutableList.of();
        }

        MapDifference<String, ByteBuffer> diff = Maps.difference(oldRecords,
                newRecords);

        Set<String> changedColumns = Sets.difference(
                Sets.union(newRecords.keySet(), oldRecords.keySet()),
                diff.entriesInCommon().keySet());

        ImmutableList.Builder<IndexSchema> changedIndices = ImmutableList.builder();

        for (IndexSchema index : indices) {
            Set<String> indexColumns = ImmutableSet.copyOf(index.getColumns());
            if (!Sets.intersection(changedColumns, indexColumns).isEmpty()) {
                changedIndices.add(index);
            }
        }

        return changedIndices.build();
    }

    private static <T> RuntimeException deserializationError(byte[] serializedData, IOException e, Class<T> clazz) {
        String clazzMessage = clazz == null ? "" : "of class type " + clazz.getName();
        String format = String.format("Deserialization failed for data (%s) " + clazzMessage,
                Util.generateHexString(serializedData));
        logger.error(format, e);
        return new RuntimeException(format, e);
    }

    private static <T> RuntimeException serializationError(T obj, IOException e) {
        String format = String.format("Serialization failed for data (%s) of class type %s",
                obj.toString(), obj.getClass().getName());
        logger.error(format, e);
        return new RuntimeException(format, e);
    }
}
