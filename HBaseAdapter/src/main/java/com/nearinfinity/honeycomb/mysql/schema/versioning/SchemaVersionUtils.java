package com.nearinfinity.honeycomb.mysql.schema.versioning;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

import org.apache.log4j.Logger;

import com.nearinfinity.honeycomb.exceptions.UnknownSchemaVersionException;

/**
 *  Collection of helper tools used for schema versioning of serialized types
 */
public abstract class SchemaVersionUtils {
    /**
     * Minimum supported Avro binary encoded integer schema version value
     */
    private static final byte MIN_ENCODED_VERSION = 0x00;

    /**
     * Maximum supported Avro binary encoded integer schema version value
     */
    private static final byte MAX_ENCODED_VERSION = 0x7E;

    private static final Logger logger = Logger.getLogger(SchemaVersionUtils.class);


    private SchemaVersionUtils() {
        throw new AssertionError();
    }


    /**
     * Processes the provided version byte to ensure that it represents a supported
     * schema version for a serialized container type.
     *
     * @param versionByte The byte representation of the encoded version, not null
     * @param supportedVersion The currently supported schema version, non-negative
     * @return True if and only if the processed schema version matches the supported schema version
     * @throws UnknownSchemaVersionException Thrown if the processed schema version is not supported
     */
    public static boolean processSchemaVersion(final byte versionByte, final int supportedVersion) {
        checkNotNull(versionByte);
        checkArgument(supportedVersion >= 0, "The supported schema version is invalid");

        final int writerSchemaVersion = decodeAvroSchemaVersion(versionByte);

        if( writerSchemaVersion != supportedVersion ) {
            logger.warn(format("Cannot process unknown schema version %d, expected: %d ", writerSchemaVersion, supportedVersion));

            throw new UnknownSchemaVersionException(writerSchemaVersion, supportedVersion);
        }

        return true;
    }


    /**
     * Attempts to decode the provided schema version byte represented by an Avro
     * binary encoded integer
     *
     * @param versionByte The byte representation of the encoded version, not null and an even value in the range of [0x00,0x7E]
     * @return The decoded version number, in the valid range of 0 to 63
     *
     * @see <a href="https://avro.apache.org/docs/current/spec.html#binary_encoding">Avro Binary Encoding</a>
     */
    private static int decodeAvroSchemaVersion(final byte versionByte) {
        checkNotNull(versionByte);
        checkArgument((versionByte >= MIN_ENCODED_VERSION && versionByte <= MAX_ENCODED_VERSION)
                       && versionByte % 2 == 0);
        /*
         * Shift right by one to perform a "divide by 2" operation on the Avro zig-zag
         * encoded integer to obtain our schema version. The version value will be decoded
         * if it is an encoded integer represented by an even version byte value
         *
         * This will work for version values from 0 to 63
         *
         * Examples:
         * Valid schema version:    17 => Encoded: 0x22
         * Invalid schema version: -17 => Encoded: 0x21
         */
        return versionByte >> 1;
    }
}
