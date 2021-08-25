package io.deephaven.util.codec;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Simple ObjectCodec for BigIntegers.
 *
 * For now this just wraps BigDecimalCodec with scale=0. At some point we might reimplement this directly but the
 * encoded format is likely the same.
 */
@SuppressWarnings("unused")
public class BigIntegerCodec implements ObjectCodec<BigInteger> {

    private final BigDecimalCodec codec;

    public static final int MAX_FIXED_PRECISION = BigDecimalCodec.MAX_FIXED_PRECISION;

    @SuppressWarnings("unused")
    public BigIntegerCodec(final int precision) {
        this.codec = new BigDecimalCodec(precision, 0, true);
    }

    @SuppressWarnings("WeakerAccess")
    public BigIntegerCodec(@Nullable String arguments) {
        // noinspection ConstantConditions
        try {
            int _precision = 0; // zero indicates unlimited precision, variable width encoding
            if (arguments != null && arguments.trim().length() > 0) {
                _precision = Integer.parseInt(arguments.trim());
                if (_precision < 1) {
                    throw new IllegalArgumentException("Specified precision must be >= 1");
                }
            }
            // we pass strict=true, rounding isn't relevant for BigInteger and we always want exceptions if precision is
            // exceeded
            this.codec = new BigDecimalCodec(_precision, 0, true);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Error parsing codec argument(s): " + ex.getMessage(), ex);
        }
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public int getPrecision() {
        return codec.getPrecision();
    }

    @Override
    public int getScale() {
        return 0;
    }

    @NotNull
    @Override
    public byte[] encode(@Nullable final BigInteger input) {
        return input == null
                ? codec.encodedNullValue()
                : codec.encode(new BigDecimal(input));
    }

    @Nullable
    @Override
    public BigInteger decode(@NotNull final byte[] input, final int offset, final int length) {
        final BigDecimal bd = codec.decode(input, offset, length);
        return bd == null ? null : bd.toBigInteger();
    }

    @Override
    public int expectedObjectWidth() {
        return codec.expectedObjectWidth();
    }
}
