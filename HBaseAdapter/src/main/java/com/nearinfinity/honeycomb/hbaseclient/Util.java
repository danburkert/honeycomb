package com.nearinfinity.honeycomb.hbaseclient;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Type;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.text.MessageFormat.format;

public final class Util {
    private static Type listType = new TypeToken<List<List<String>>>() {
    }.getType();
    private static Gson gson = new Gson();

    /**
     * Merges many byte arrays into a single large one.
     * Requires: size >= sum (length pieces)
     *
     * @param pieces Multiple byte arrays
     * @param size   Total length of the byte arrays merged
     * @return Merged byte array
     */
    public static byte[] mergeByteArrays(final Iterable<byte[]> pieces, final int size) {
        checkNotNull(pieces, "pieces");
        checkArgument(size >= 0, format("size must be positive. {0}", size));

        int offset = 0;
        final byte[] mergedArray = new byte[size];
        for (final byte[] piece : pieces) {
            int totalLength = offset + piece.length;
            if (totalLength > size) {
                throw new IllegalStateException(format("Merging byte arrays would exceed allocated size. Size {0}/Total {1}", size, totalLength));
            }

            System.arraycopy(piece, 0, mergedArray, offset, piece.length);
            offset += piece.length;
        }

        return mergedArray;
    }

    /**
     * Takes SQL column IDs in HBase format and increments the last column ID.
     * For example:
     * incrementColumn([0,0,0,0,0,0,0,1], 8) => [0,0,0,0,0,0,0,2]
     *
     * @param columnIds SQL column IDs
     * @param offset    Position of the last column ID
     * @return Incremented SQL column IDs
     */
    public static byte[] incrementColumn(final byte[] columnIds, final int offset) {
        checkNotNull(columnIds, "columnIds");
        checkArgument(offset >= 0, "Offset must be positive");
        checkArgument(offset <= (columnIds.length - Bytes.SIZEOF_LONG), "offset must be less than the length of columnIds");

        final byte[] nextColumn = new byte[columnIds.length];
        final long nextColumnId = Bytes.toLong(columnIds, offset) + 1;
        final int finalOffset = offset + Bytes.SIZEOF_LONG;

        System.arraycopy(columnIds, 0, nextColumn, 0, offset);
        System.arraycopy(Bytes.toBytes(nextColumnId), 0, nextColumn, offset, Bytes.SIZEOF_LONG);
        System.arraycopy(columnIds, finalOffset, nextColumn, finalOffset, columnIds.length - finalOffset);

        return nextColumn;
    }

    /**
     * Serialize a list into a byte array
     *
     * @param list List to serialize
     * @return Serialized list
     */
    public static byte[] serializeList(final List<List<String>> list) {
        checkNotNull(list, "list");
        return gson.toJson(list, listType).getBytes();
    }

    /**
     * Deserialize a byte array into a list
     *
     * @param bytes Bytes of a list
     * @return Deserialized list
     */
    public static List<List<String>> deserializeList(byte[] bytes) {
        checkNotNull(bytes, "bytes");
        return gson.fromJson(new String(bytes), listType);
    }
}
