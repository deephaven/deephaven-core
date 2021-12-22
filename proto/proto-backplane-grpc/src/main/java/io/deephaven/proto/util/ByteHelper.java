package io.deephaven.proto.util;

import java.nio.ByteBuffer;

public class ByteHelper {
    public static String byteBufToHex(final ByteBuffer ticket) {
        StringBuilder sb = new StringBuilder();
        for (int i = ticket.position(); i < ticket.limit(); ++i) {
            sb.append(String.format("%02x", ticket.get(i)));
        }
        return sb.toString();
    }
}
