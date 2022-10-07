/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util;

public class ByteUtils {
    private static final char[] HEX_LOOKUP = new char[] {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    public static String byteArrToHex(byte[] bytes) {
        // our output size will be exactly 2x byte-array length
        final char[] buffer = new char[bytes.length * 2];

        for (int ii = 0; ii < bytes.length; ii++) {
            // extract the upper 4 bits
            buffer[ii << 1] = HEX_LOOKUP[bytes[ii] >>> 4];
            // extract the lower 4 bits
            buffer[(ii << 1) + 1] = HEX_LOOKUP[bytes[ii] & 0xF];
        }

        return new String(buffer);
    }
}
