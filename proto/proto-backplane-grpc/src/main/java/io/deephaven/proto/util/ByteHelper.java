//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.proto.util;

import java.nio.ByteBuffer;

public class ByteHelper {
    public static String byteBufToHex(final ByteBuffer buffer) {
        StringBuilder sb = new StringBuilder();
        for (int i = buffer.position(); i < buffer.limit(); ++i) {
            sb.append(String.format("%02x", buffer.get(i)));
        }
        return sb.toString();
    }
}
