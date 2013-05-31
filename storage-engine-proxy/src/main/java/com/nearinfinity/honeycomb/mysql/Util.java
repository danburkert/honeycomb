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


package com.nearinfinity.honeycomb.mysql;

import com.google.common.collect.*;
import com.nearinfinity.honeycomb.exceptions.RuntimeIOException;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import org.apache.avro.io.*;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
     * @param bytes Byte string
     * @return Hex string
     */
    public static String generateHexString(final byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }

        return sb.toString();
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
