package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.mysql.gen.RowContainer;
import com.nearinfinity.honeycomb.mysql.gen.UUIDContainer;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public class Row {
    private RowContainer row;
    private static DatumWriter<RowContainer> writer =
            new SpecificDatumWriter<RowContainer>(RowContainer.class);
    private static DatumReader<RowContainer> reader =
            new SpecificDatumReader<RowContainer>(RowContainer.class);

    /**
     * Constructs a new Row with specified records and UUID.
     * @param records Map of column name to record value
     * @param uuid UUID representing the unique position of the Row
     */
    public Row(Map<String, ByteBuffer> records, UUID uuid) {
        checkNotNull(records, "records must not be null.");
        // uuid nullity will be checked by UUIDToBytes
        row = new RowContainer(new UUIDContainer(Util.UUIDToBytes(uuid)), records);
    }

    /**
     * Constructor called during deserialization.
     * @param row Avro RowContainer class
     */
    private Row(RowContainer row) {
        this.row = row;
    }

    /**
     * Returns the UUID of this Row.
     * @return UUID of this Row.
     */
    public UUID getUUID() {
        return Util.BytesToUUID(row.getUuid().bytes());
    }

    /**
     * Returns the a map of column names to records of this Row.
     * @return Map of column names to records
     */
    public Map<String, byte[]> getRecords() {
        // Currently the record is always either null or a byte array.  We
        // should move to more specific data types in the future in order to
        // take advantage of more efficient Avro encoding.

        // We should move away from explicitly using a TreeMap when we have
        // access to the Avro container in C++.  At this point the stuff below
        // will be unnecessary, and we can replace it with
        // return row.getRecords();
        Map<String, byte[]> retMap = new TreeMap<String, byte[]>();
        for (Map.Entry<String, ByteBuffer> entry : row.getRecords().entrySet()) {
            retMap.put(entry.getKey(), (entry.getValue() == null) ? null : entry.getValue().array());
        }
        return retMap;
    }

    /**
     * Serialize {@link Row} into {@link byte[]}.
     * @return Serialized row
     * @throws IOException when serialization fails
     */
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(row, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    /**
     * Deserialize {@link byte[]} into new {@link Row} instance
     * @param serializedRow byte buffer containing serialized Row
     * @return new Row instance from serializedRow
     * @throws IOException
     */
    public static Row deserialize(byte[] serializedRow) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(serializedRow);
        Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        return new Row(reader.read(null, decoder));
    }

    public boolean equals(Object oThat) {
        if (this == oThat) return true;
        if (!(oThat instanceof Row)) return false;
        Row that = (Row) oThat;
        return this.row.equals(that.row);
    }

    /**
     * The following methods are called through JNI.  They are a stop-gap until
     * we get access to the Avro container on the C++ side.  When that happens
     * they should be removed.
     */

    public byte[] getUUIDBuffer() {
        return row.getUuid().bytes();
    }

    public Map<String, byte[]> getRowMap() {
        return new TreeMap<String, byte[]>(getRecords());
    }

    public byte[][] getValues() {
        Map<String, byte[]> records = getRecords();
        return records.values().toArray(new byte[records.size()][]);
    }

    public String[] getKeys() {
        Map<String, ByteBuffer> records = row.getRecords();
        return records.keySet().toArray(new String[records.size()]);
    }
}
