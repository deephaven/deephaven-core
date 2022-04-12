package io.deephaven.parquet.table;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.util.codec.ObjectCodec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;

public class BigDecimalParquetBytesCodec implements ObjectCodec<BigDecimal> {
    private final int precision;
    private final int scale;
    private final int encodedSizeInBytes;
    private final RoundingMode roundingMode;
    private final byte[] nullBytes;

    /**
     *
     * @param precision
     * @param scale
     * @param encodedSizeInBytes encoded size in bytes, if fixed size, or -1 if variable size. note that according to
     *        the parquet spec, the minimum number of bytes required to represent the unscaled value should be used for
     *        a variable sized (binary) encoding; in any case, the maximum encoded bytes is implicitly limited by
     *        precision.
     */
    public BigDecimalParquetBytesCodec(final int precision, final int scale, final int encodedSizeInBytes,
            final RoundingMode roundingMode) {
        if (precision <= 0) {
            throw new IllegalArgumentException("precision (=" + precision + ") should be > 0");
        }
        if (scale < 0) {
            throw new IllegalArgumentException("scale (=" + scale + ") should be >= 0");
        }
        if (scale > precision) {
            throw new IllegalArgumentException("scale (=" + scale + ") is greater than precision (=" + precision + ")");
        }
        this.precision = precision;
        this.scale = scale;
        this.encodedSizeInBytes = encodedSizeInBytes;
        this.roundingMode = roundingMode;
        if (encodedSizeInBytes > 0) {
            nullBytes = new byte[encodedSizeInBytes];
            for (int i = 0; i < encodedSizeInBytes; ++i) {
                nullBytes[i] = (byte) 0xff;
            }
        } else {
            nullBytes = CollectionUtil.ZERO_LENGTH_BYTE_ARRAY;
        }
    }

    public BigDecimalParquetBytesCodec(final int precision, final int scale, final int encodedSizeInBytes) {
        this(precision, scale, encodedSizeInBytes, RoundingMode.HALF_UP);
    }

    // Given how parquet encoding works for nulls, the actual value provided for a null is irrelevant.
    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public int getPrecision() {
        return precision;
    }

    @Override
    public int getScale() {
        return scale;
    }

    @NotNull
    @Override
    public byte[] encode(@Nullable final BigDecimal input) {
        if (input == null) {
            return nullBytes;
        }

        final BigDecimal value = (input.scale() == scale) ? input : input.setScale(scale, roundingMode);
        final BigInteger unscaledValue = value.unscaledValue();

        final byte[] bytes = unscaledValue.toByteArray();
        return bytes;
    }

    @Nullable
    @Override
    public BigDecimal decode(@NotNull final byte[] input, final int offset, final int length) {
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
        final BigInteger unscaledValue = new BigInteger(unscaledValueBytes);
        return new BigDecimal(unscaledValue, scale);
    }

    @Override
    public int expectedObjectWidth() {
        return encodedSizeInBytes <= 0 ? VARIABLE_WIDTH_SENTINEL : encodedSizeInBytes;
    }
}
