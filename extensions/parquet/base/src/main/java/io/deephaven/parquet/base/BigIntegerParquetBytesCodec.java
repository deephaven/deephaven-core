//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.codec.ObjectCodec;
import io.deephaven.util.type.ArrayTypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;
import java.nio.ByteBuffer;

public class BigIntegerParquetBytesCodec implements ObjectCodec<BigInteger> {
    private final int encodedSizeInBytes;
    private final byte[] nullBytes;

    /**
     *
     * @param encodedSizeInBytes encoded size in bytes, if fixed size, or -1 if variable size. note that according to
     *        the parquet spec, the minimum number of bytes required to represent the unscaled value should be used for
     *        a variable sized (binary) encoding; in any case, the maximum encoded bytes is implicitly limited by
     *        precision.
     */
    public BigIntegerParquetBytesCodec(final int encodedSizeInBytes) {
        this.encodedSizeInBytes = encodedSizeInBytes;
        if (encodedSizeInBytes > 0) {
            nullBytes = new byte[encodedSizeInBytes];
            for (int i = 0; i < encodedSizeInBytes; ++i) {
                nullBytes[i] = (byte) 0xff;
            }
        } else {
            nullBytes = ArrayTypeUtils.EMPTY_BYTE_ARRAY;
        }
    }

    public BigIntegerParquetBytesCodec() {
        this(-1);
    }

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
        return 1;
    }

    @Override
    public int expectedObjectWidth() {
        return encodedSizeInBytes <= 0 ? VARIABLE_WIDTH_SENTINEL : encodedSizeInBytes;
    }

    @NotNull
    @Override
    public byte[] encode(@Nullable final BigInteger input) {
        if (input == null) {
            return nullBytes;
        }

        return input.toByteArray();
    }

    @Nullable
    @Override
    public BigInteger decode(@NotNull final byte[] input, final int offset, final int length) {
        if (length <= 0) {
            return null;
        }

        if (length == encodedSizeInBytes) {
            boolean allPreviousBitsSet = true;
            for (int i = 0; i < encodedSizeInBytes; ++i) {
                if (input[offset + i] != (byte) 0xff) {
                    allPreviousBitsSet = false;
                    break;
                }
            }
            if (allPreviousBitsSet) {
                return null;
            }
        }

        final ByteBuffer buffer = ByteBuffer.wrap(input, offset, length);
        final byte[] unscaledValueBytes = new byte[length];
        buffer.get(unscaledValueBytes);
        return new BigInteger(unscaledValueBytes);
    }
}
