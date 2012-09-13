package com.nearinfinity.bulkloader;

import com.nearinfinity.hbaseclient.ColumnMetadata;
import com.nearinfinity.hbaseclient.ColumnType;
import org.apache.commons.lang3.time.DateUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ValueParser {

    public static byte[] parse(String val, ColumnMetadata meta) throws Exception {
        byte[] ret = null;
        ColumnType t = meta.getType();
        switch (t) {
            case LONG:
                ret = ByteBuffer.allocate(8).putLong(Long.parseLong(val)).array();
                break;
            case ULONG:
                BigInteger n = new BigInteger(val);
                if (n.compareTo(BigInteger.ZERO) == -1) {
                    throw new Exception("negative value provided for unsigned column.  value: ".concat(val));
                }
                ret = ByteBuffer.allocate(8).putLong(new BigInteger(val).longValue()).array();
                break;
            case DOUBLE:
                ret = ByteBuffer.allocate(8).putDouble(Double.parseDouble(val)).array();
                break;
            case DATE:
                Date d = DateUtils.parseDateStrictly(val,
                        "yyyy-MM-dd",
                        "yyyy/MM/dd",
                        "yyyy.MM.dd",
                        "yyyyMMdd");
                SimpleDateFormat time_formatter = new SimpleDateFormat("yyyy-MM-dd");
                ret = time_formatter.format(d).getBytes();
                break;
            case TIME:
                d = DateUtils.parseDateStrictly(val,
                        "HH:mm:ss",
                        "HHmmss");
                time_formatter = new SimpleDateFormat("HH:mm:ss");
                ret = time_formatter.format(d).getBytes();
                break;
            case DATETIME:
                d = DateUtils.parseDateStrictly(val,
                        "yyyy-MM-dd HH:mm:ss",
                        "yyyy/MM/dd HH:mm:ss",
                        "yyyy.MM.dd HH:mm:ss",
                        "yyyyMMdd HHmmss");
                time_formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                ret = time_formatter.format(d).getBytes();
                break;
            case DECIMAL:
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
                for (int i = 0; i < left_bytes.length; i++) {
                    buff[i + left_bytes_len - left_bytes.length] = left_bytes[i];
                }
                for (int i = 0; i < right_bytes.length; i++) {
                    buff[i + right_bytes_len - right_bytes.length + left_bytes_len] = right_bytes[i];
                }
                buff[0] ^= -128; // Flip first bit, 0x80
                if (is_negative) { // Flip all bits
                    for (int i = 0; i < buff.length; i++) {
                        buff[i] ^= -1; // 0xff
                    }
                }
                ret = buff;
                break;
            case STRING:
            case BINARY:
            case NONE:
            default:
                ret = val.getBytes();
                break;
        }
        return ret;
    }

    public static int bytesFromDigits(int digits) {
        int ret = 0;
        ret += 4 * (digits / 9);
        ret += (digits % 9 + 1) / 2;
        return ret;
    }
}