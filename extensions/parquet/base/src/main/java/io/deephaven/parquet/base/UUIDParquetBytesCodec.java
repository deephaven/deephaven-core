//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.codec.ObjectCodec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.UUID;

/**
 * Codec for the parquet {@code UUID} logical type: a 16-byte {@code FIXED_LEN_BYTE_ARRAY} holding the UUID in
 * big-endian order, most significant bits first.
 */
public final class UUIDParquetBytesCodec implements ObjectCodec<UUID> {

    public static final UUIDParquetBytesCodec INSTANCE = new UUIDParquetBytesCodec();

    /**
     * The same encoding, viewed as the canonical {@link UUID#toString() string form}.
     */
    public static final ObjectCodec<String> AS_STRING = new ObjectCodec<>() {
        @Override
        public boolean isNullable() {
            return INSTANCE.isNullable();
        }

        @Override
        public int getPrecision() {
            return INSTANCE.getPrecision();
        }

        @Override
        public int getScale() {
            return INSTANCE.getScale();
        }

        @Override
        public int expectedObjectWidth() {
            return INSTANCE.expectedObjectWidth();
        }

        @Override
        public byte @NotNull [] encode(@Nullable final String input) {
            return INSTANCE.encode(input == null ? null : UUID.fromString(input));
        }

        @NotNull
        @Override
        public String decode(final byte @NotNull [] input, final int offset, final int length) {
            return INSTANCE.decode(input, offset, length).toString();
        }
    };

    private static final int WIDTH = 16;
    private static final VarHandle BIG_ENDIAN_LONG =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    private UUIDParquetBytesCodec() {}

    // Given how parquet encoding works for nulls, the actual value provided for a null is irrelevant.
    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public int expectedObjectWidth() {
        return WIDTH;
    }

    @Override
    public byte @NotNull [] encode(@Nullable final UUID input) {
        final byte[] encoded = new byte[WIDTH];
        if (input != null) {
            putLong(encoded, 0, input.getMostSignificantBits());
            putLong(encoded, 8, input.getLeastSignificantBits());
        }
        return encoded;
    }

    // Parquet nulls are definition levels, so the reader never passes one here.
    @NotNull
    @Override
    public UUID decode(final byte @NotNull [] input, final int offset, final int length) {
        checkWidth(length);
        return new UUID(getLong(input, offset), getLong(input, offset + 8));
    }

    private static long getLong(final byte[] input, final int offset) {
        return (long) BIG_ENDIAN_LONG.get(input, offset);
    }

    private static void putLong(final byte[] output, final int offset, final long value) {
        BIG_ENDIAN_LONG.set(output, offset, value);
    }
}
