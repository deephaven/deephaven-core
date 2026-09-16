//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

/**
 * GWT super-source replacement for {@link LittleEndianCodec}. {@code java.lang.invoke.VarHandle} is unavailable in GWT,
 * so this implementation uses only GWT-safe arithmetic. It must stay in sync with the JVM implementation in
 * {@code extensions/barrage/.../chunk/LittleEndianCodec.java}.
 */
final class LittleEndianCodec {
    private LittleEndianCodec() {}

    static long getLong(final byte[] b, final int o) {
        return (b[o] & 0xFFL)
                | (b[o + 1] & 0xFFL) << 8
                | (b[o + 2] & 0xFFL) << 16
                | (b[o + 3] & 0xFFL) << 24
                | (b[o + 4] & 0xFFL) << 32
                | (b[o + 5] & 0xFFL) << 40
                | (b[o + 6] & 0xFFL) << 48
                | (b[o + 7] & 0xFFL) << 56;
    }

    static int getInt(final byte[] b, final int o) {
        return (b[o] & 0xFF)
                | (b[o + 1] & 0xFF) << 8
                | (b[o + 2] & 0xFF) << 16
                | (b[o + 3] & 0xFF) << 24;
    }

    static short getShort(final byte[] b, final int o) {
        return (short) ((b[o] & 0xFF) | (b[o + 1] & 0xFF) << 8);
    }

    static char getChar(final byte[] b, final int o) {
        return (char) ((b[o] & 0xFF) | (b[o + 1] & 0xFF) << 8);
    }

    /** A single byte has no byte order; present so the replicated readers/writers can share one shape. */
    static byte getByte(final byte[] b, final int o) {
        return b[o];
    }

    static double getDouble(final byte[] b, final int o) {
        return Double.longBitsToDouble(getLong(b, o));
    }

    static float getFloat(final byte[] b, final int o) {
        return Float.intBitsToFloat(getInt(b, o));
    }

    static void putLong(final byte[] b, final int o, final long v) {
        b[o] = (byte) v;
        b[o + 1] = (byte) (v >> 8);
        b[o + 2] = (byte) (v >> 16);
        b[o + 3] = (byte) (v >> 24);
        b[o + 4] = (byte) (v >> 32);
        b[o + 5] = (byte) (v >> 40);
        b[o + 6] = (byte) (v >> 48);
        b[o + 7] = (byte) (v >> 56);
    }

    static void putInt(final byte[] b, final int o, final int v) {
        b[o] = (byte) v;
        b[o + 1] = (byte) (v >> 8);
        b[o + 2] = (byte) (v >> 16);
        b[o + 3] = (byte) (v >> 24);
    }

    static void putShort(final byte[] b, final int o, final short v) {
        b[o] = (byte) v;
        b[o + 1] = (byte) (v >> 8);
    }

    static void putChar(final byte[] b, final int o, final char v) {
        b[o] = (byte) v;
        b[o + 1] = (byte) (v >> 8);
    }

    /** A single byte has no byte order; present so the replicated readers/writers can share one shape. */
    static void putByte(final byte[] b, final int o, final byte v) {
        b[o] = v;
    }

    static void putDouble(final byte[] b, final int o, final double v) {
        putLong(b, o, Double.doubleToLongBits(v));
    }

    static void putFloat(final byte[] b, final int o, final float v) {
        putInt(b, o, Float.floatToIntBits(v));
    }
}
