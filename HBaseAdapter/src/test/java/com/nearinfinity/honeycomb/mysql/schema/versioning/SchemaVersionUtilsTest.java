package com.nearinfinity.honeycomb.mysql.schema.versioning;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.nearinfinity.honeycomb.exceptions.UnknownSchemaVersionException;

public class SchemaVersionUtilsTest {

    @Test(expected = UnknownSchemaVersionException.class)
    public void testProcessSchemaVersionUnsupportedVersion() {
        SchemaVersionUtils.processSchemaVersion((byte)0x02, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testProcessSchemaVersionNegativeSupportedVersion() {
        SchemaVersionUtils.processSchemaVersion((byte)0x00, -1);
    }

    @Test
    public void testProcessSchemaVersionSupportedVersion() {
        assertTrue(SchemaVersionUtils.processSchemaVersion((byte)0x7E, 63));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecodeAvroSchemaVersionExceedMinimumValue() {
        SchemaVersionUtils.decodeAvroSchemaVersion((byte)-5);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecodeAvroSchemaVersionExceedMaximumValue() {
        SchemaVersionUtils.decodeAvroSchemaVersion((byte)0x7F);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecodeAvroSchemaVersionOddEncodedValue() {
        SchemaVersionUtils.decodeAvroSchemaVersion((byte)0x01);
    }
}
