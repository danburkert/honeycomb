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
