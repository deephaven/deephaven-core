//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Little-endian codec between fixed-width primitive values and a {@code byte[]} payload window, used by the barrage
 * fixed-width chunk readers/writers to bulk-decode/encode a window of values.
 * <p>
 * This JVM implementation uses {@link VarHandle} for a single (possibly unaligned) load/store per value. These readers
 * and writers are also cross-compiled to JavaScript for the web client, where {@code java.lang.invoke} is unavailable;
 * a GWT super-source replacement of this class (see {@code web/client-api/.../super}) provides an equivalent
 * implementation using only GWT-safe arithmetic. The two must be kept in sync.
 */
public final class LittleEndianCodec {
    private LittleEndianCodec() {}

    private static final VarHandle LONG =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT =
            MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle SHORT =
            MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle DOUBLE =
            MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle FLOAT =
            MethodHandles.byteArrayViewVarHandle(float[].class, ByteOrder.LITTLE_ENDIAN);

    public static long getLong(final byte[] b, final int o) {
        return (long) LONG.get(b, o);
    }

    public static int getInt(final byte[] b, final int o) {
        return (int) INT.get(b, o);
    }

    public static short getShort(final byte[] b, final int o) {
        return (short) SHORT.get(b, o);
    }

    public static double getDouble(final byte[] b, final int o) {
        return (double) DOUBLE.get(b, o);
    }

    public static float getFloat(final byte[] b, final int o) {
        return (float) FLOAT.get(b, o);
    }

    public static void putLong(final byte[] b, final int o, final long v) {
        LONG.set(b, o, v);
    }

    public static void putInt(final byte[] b, final int o, final int v) {
        INT.set(b, o, v);
    }

    public static void putShort(final byte[] b, final int o, final short v) {
        SHORT.set(b, o, v);
    }

    public static void putDouble(final byte[] b, final int o, final double v) {
        DOUBLE.set(b, o, v);
    }

    public static void putFloat(final byte[] b, final int o, final float v) {
        FLOAT.set(b, o, v);
    }
}
