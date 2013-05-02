package com.nearinfinity.honeycomb.mysql;


import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.nearinfinity.honeycomb.mysql.gen.AvroRow;
import com.nearinfinity.honeycomb.mysql.gen.UUIDContainer;
import com.nearinfinity.honeycomb.mysql.schema.versioning.RowSchemaInfo;
import com.nearinfinity.honeycomb.mysql.schema.versioning.SchemaVersionUtils;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class Row {
    private static final DatumWriter<AvroRow> writer =
            new SpecificDatumWriter<AvroRow>(AvroRow.class);
    private static final DatumReader<AvroRow> reader =
            new SpecificDatumReader<AvroRow>(AvroRow.class);
    private final AvroRow row;

    /**
     * Construct a new Row with specified records and UUID.
     *
     * @param records Map of column name to record value
     * @param uuid    UUID representing the unique position of the Row
     */
    public Row(Map<String, ByteBuffer> records, UUID uuid) {
        checkNotNull(records, "records must not be null.");
        // uuid nullity will be checked by UUIDToBytes

        row = AvroRow.newBuilder()
                .setUuid(new UUIDContainer(Util.UUIDToBytes(uuid)))
                .setRecords(records)
                .build();
    }

    /**
     * Constructor called during deserialization.
     *
     * @param row {@link AvroRow} the underlying content for this row
     */
    private Row(AvroRow row) {
        this.row = row;
    }

    /**
     * Deserialize the provided serialized row buffer to a new {@link Row} instance
     *
     * @param serializedRow byte buffer containing serialized Row
     * @return new Row instance from serializedRow
     */
    public static Row deserialize(byte[] serializedRow) {
        checkNotNull(serializedRow);
        checkArgument(serializedRow.length > 0);

        SchemaVersionUtils.processSchemaVersion(serializedRow[0], RowSchemaInfo.VER_CURRENT);

        return new Row(Util.deserializeAvroObject(serializedRow, reader));
    }

    public static byte[] updateSerializedSchema(byte[] row) {
        byte version = row[0];
        if (isMostRecentVersion(version)) {
            return row;
        }

        // Bring the row up to most recent version
        return Row.deserialize(row).serialize();
    }

    private static boolean isMostRecentVersion(byte version) {
        return SchemaVersionUtils.decodeAvroSchemaVersion(version) == RowSchemaInfo.VER_CURRENT;
    }

    /**
     * Returns the {@link UUID} of this Row.
     *
     * @return UUID of this Row.
     */
    public UUID getUUID() {
        return Util.bytesToUUID(row.getUuid().bytes());
    }

    public void setUUID(UUID uuid) {
        row.setUuid(new UUIDContainer(Util.UUIDToBytes(uuid)));
    }

    /**
     * Set UUID to a new random UUID
     */
    public void setRandomUUID() {
        row.setUuid(new UUIDContainer(Util.UUIDToBytes(UUID.randomUUID())));
    }

    /**
     * Returns the a map of column names to records of this Row.
     *
     * @return Map of column names to records
     */
    public Map<String, ByteBuffer> getRecords() {
        return row.getRecords();
    }

    /**
     * Serialize this {@link Row} instance to a byte array.
     *
     * @return Serialized row
     */
    public byte[] serialize() {
        return Util.serializeAvroObject(row, writer);
    }

    @Override
    public int hashCode() {
        return row.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        Row other = (Row) obj;
        if (row == null) {
            if (other.row != null) {
                return false;
            }
        } else if (!row.equals(other.row)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        final ToStringHelper toString = Objects.toStringHelper(this.getClass());

        toString.add("Version", row.getVersion())
                .add("UUID", getUUID());

        for (final Map.Entry<String, ByteBuffer> entry : getRecords().entrySet()) {
            toString.add("Record", format("%s: %s", entry.getKey(), entry.getValue()));
        }

        return toString.toString();
    }
}
