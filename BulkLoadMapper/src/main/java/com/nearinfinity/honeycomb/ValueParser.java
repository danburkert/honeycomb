package com.nearinfinity.honeycomb;

import com.nearinfinity.hbaseclient.ColumnMetadata;
import com.nearinfinity.hbaseclient.ColumnType;
import org.apache.commons.lang.time.DateUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;

public class ValueParser {
    public static byte[] parse(String val, ColumnMetadata meta) throws ParseException {
        checkNotNull(val, "Should not be parsing null. Something went terribly wrong.");
        ColumnType type = meta.getType();

        if (val.length() == 0 && type != ColumnType.STRING
                && type != ColumnType.BINARY) {
            if(meta.isNullable()) {
                return null;
            } else {
                throw new IllegalArgumentException("Attempt to parse invalid field value: " + val);
            }
        }

        byte[] ret;

        switch (type) {
            case LONG:
                ret = ByteBuffer.allocate(8).putLong(Long.parseLong(val)).array();
                break;
            case ULONG:
                BigInteger n = new BigInteger(val);
                if (n.compareTo(BigInteger.ZERO) == -1) {
                    throw new IllegalArgumentException("negative value provided for unsigned column. value: " + val);
                }
                ret = ByteBuffer.allocate(8).putLong(n.longValue()).array();
                break;
            case DOUBLE:
                ret = ByteBuffer.allocate(8).putDouble(Double.parseDouble(val)).array();
                break;
            case DATE:
                ret = extractDate(val, "yyyy-MM-dd",
                        "yyyy-MM-dd",
                        "yyyy/MM/dd",
                        "yyyy.MM.dd",
                        "yyyyMMdd");
                break;
            case TIME:
                ret = extractDate(val, "HH:mm:ss",
                        "HH:mm:ss",
                        "HHmmss");
                break;
            case DATETIME:
                ret = extractDate(val, "yyyy-MM-dd HH:mm:ss",
                        "yyyy-MM-dd HH:mm:ss",
                        "yyyy/MM/dd HH:mm:ss",
                        "yyyy.MM.dd HH:mm:ss",
                        "yyyyMMdd HHmmss");
                break;
            case DECIMAL:
                ret = extractDecimal(val, meta);
                break;
            case STRING:
            case BINARY:
            case NONE:
            default:
                ret = val.getBytes(Charset.forName("UTF-8"));
                break;
        }
        return ret;
    }

    private static byte[] extractDate(String val, String dateFormat,
                                      String... parseFormats)
            throws ParseException {
        Date d = DateUtils.parseDateStrictly(val, parseFormats);
        SimpleDateFormat format = new SimpleDateFormat(dateFormat);
        return format.format(d).getBytes();
    }

    private static byte[] extractDecimal(String val, ColumnMetadata meta) {
        byte[] ret;
        int precision = meta.getPrecision();
        int right_scale = meta.getScale();
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
        ret = buff;
        return ret;
    }

    public static int bytesFromDigits(int digits) {
        int ret = 0;
        ret += 4 * (digits / 9);
        ret += (digits % 9 + 1) / 2;
        return ret;
    }
}