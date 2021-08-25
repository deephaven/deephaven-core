package io.deephaven.util.codec;

import io.deephaven.datastructures.util.CollectionUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * BigDecimal encoder, with fixed and variable width support.
 *
 * We use 1's complement to store fixed precision values so that they may be ordered in binary
 * without decoding. There is no practical limit on the precision we can store this way but we limit
 * it to 1000 decimal digits for sanity.
 *
 * Variable width values are stored raw as BigDecimal scale followed by the unscaled byte array.
 */
public class BigDecimalCodec implements ObjectCodec<BigDecimal> {

    private final int precision;
    private final int scale;
    private final boolean strict;

    // encoded size, if fixed size
    private int encodedSize;

    // the encoded version of the zero value for this codec
    private byte[] zeroBytes;

    // encoded version of null for this precision
    private byte[] nullBytes;

    // arbitrary max
    public static final int MAX_FIXED_PRECISION = 1000;

    @SuppressWarnings("unused")
    public BigDecimalCodec(final int precision, final int scale, final boolean strict) {
        this.precision = precision;
        this.scale = scale;
        this.strict = strict;

        init();
    }

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

    public byte[] encodedNullValue() {
        return nullBytes;
    }

    @SuppressWarnings("WeakerAccess")
    public BigDecimalCodec(@Nullable String arguments) {
        // noinspection ConstantConditions
        try {
            int _precision = 0, _scale = 0; // zero indicates unlimited precision/scale, variable
                                            // width encoding
            boolean _strict = true;
            if (arguments != null && arguments.trim().length() > 0) {
                final String[] tokens = arguments.split(",");
                if (tokens.length > 0 && tokens[0].trim().length() > 0) {
                    _precision = Integer.parseInt(tokens[0].trim());
                    if (_precision < 1) {
                        throw new IllegalArgumentException("Specified precision must be >= 1");
                    }
                }
                if (tokens.length > 1 && tokens[1].trim().length() > 0) {
                    _scale = Integer.parseInt(tokens[1].trim());
                }
                if (tokens.length > 2 && tokens[2].trim().length() > 0) {
                    String mode = tokens[2].trim();
                    switch (mode.toLowerCase()) {
                        case "allowrounding":
                            _strict = false;
                            break;
                        case "norounding":
                            _strict = true;
                            break;
                        default:
                            throw new IllegalArgumentException(
                                "Unexpected rounding mode (legal values are \"allowRounding\" or \"noRounding\"): "
                                    + mode);
                    }
                }
            }
            if (_precision < _scale) {
                throw new IllegalArgumentException("Precision must be >= scale");
            }
            this.precision = _precision;
            this.scale = _scale;
            this.strict = _strict;
        } catch (Exception ex) {
            throw new IllegalArgumentException(
                "Error parsing codec argument(s): " + ex.getMessage(), ex);
        }

        init();
    }

    private void init() {

        if (precision < 0 || precision > MAX_FIXED_PRECISION) {
            throw new IllegalArgumentException(
                "Precision out of legal range (0-" + MAX_FIXED_PRECISION + ")");
        }
        if (scale < 0) {
            throw new IllegalArgumentException("Scale must be non-negative");
        }

        if (precision > 0) {
            // fixed size encoding
            // figure how many bytes we need for the required decimal precision
            // plus one leading byte for the sign (0 for negative, 1 for positive numbers)
            encodedSize = (int) Math.ceil(Math.log(10) / Math.log(2) * precision / Byte.SIZE) + 1;
            zeroBytes = new byte[encodedSize];
            // there are two possible ways to represent zero in our schema,
            // we choose all zeros ("positive zero") for zero and "negative zero" for null. This is
            // arbitrary convention
            Arrays.fill(zeroBytes, (byte) 0);
            zeroBytes[0] = (byte) 1;
            nullBytes = new byte[encodedSize];
            Arrays.fill(nullBytes, (byte) 0xff);
            nullBytes[0] = 0;
        } else {
            // variable size encoding
            encodedSize = 0;
            final byte[] unscaledZero = BigDecimal.ZERO.unscaledValue().toByteArray();
            zeroBytes = new byte[Integer.BYTES + unscaledZero.length];
            Arrays.fill(zeroBytes, (byte) 0);
            nullBytes = CollectionUtil.ZERO_LENGTH_BYTE_ARRAY;
        }
    }

    @NotNull
    @Override
    public byte[] encode(@Nullable final BigDecimal input) {
        if (input == null) {
            return nullBytes;
        }

        // normalize first so we don't think something like 0.0010 has scale of 4
        BigDecimal value = input.stripTrailingZeros();

        // VARIABLE SIZE
        // for variable size, we write the scale followed by unscaled bytes
        // scale implies size in bytes, so we don't need to write size directly
        if (precision == 0) {
            final byte[] unscaledValue = value.unscaledValue().toByteArray();
            final ByteBuffer buffer = ByteBuffer.allocate(unscaledValue.length + Integer.BYTES);
            buffer.putInt(value.scale());
            buffer.put(unscaledValue);
            return buffer.array();
        }

        // FIXED SIZE

        // round if necessary
        // we need to make sure we adjust for both precision and scale since we are encoding with a
        // fixed scale
        // (i.e. too high a scale requires reducing precision to "make room")
        if ((value.precision() > this.precision || value.scale() > scale)) {
            if (strict) {
                throw new IllegalArgumentException(
                    "Unable to encode value " + value.toString() + " with precision "
                        + precision + " scale " + scale);
            }
            final int targetPrecision =
                Math.min(precision, value.precision() - Math.max(0, value.scale() - scale));
            if (targetPrecision > 0) {
                value = value.round(new MathContext(targetPrecision));
            } else {
                return zeroBytes;
            }
        }

        final byte[] bytes = new byte[encodedSize];
        bytes[0] = value.signum() >= 0 ? (byte) 1 : (byte) 0; // set sign bit

        // we should not ever have to round here, that is taken care of above
        // we store everything as an unscaled integer value (the smallest non-zero value we can
        // store is "1")
        value = value.movePointRight(scale).setScale(0).abs();

        // copy unscaled bytes to proper size array
        final byte[] unscaledValue = value.unscaledValue().toByteArray();
        if (unscaledValue.length >= bytes.length) { // unscaled value must be at most one less than
                                                    // length of our buffer
            throw new IllegalArgumentException(
                "Value " + input.toString() + " is too large to encode with precision "
                    + precision + " and scale " + scale);
        }

        // try and be efficient about generating a 1's complement encoded version
        Arrays.fill(bytes, 1, bytes.length, bytes[0] == 0 ? (byte) 0xff : (byte) 0);
        if (bytes[0] == 0) {
            for (int i = 0; i < unscaledValue.length; i++) {
                bytes[bytes.length - unscaledValue.length + i] = (byte) ~((int) unscaledValue[i]);
            }
        } else {
            System.arraycopy(unscaledValue, 0, bytes, bytes.length - unscaledValue.length,
                unscaledValue.length);
        }

        return bytes;
    }

    @Nullable
    @Override
    public BigDecimal decode(@NotNull final byte[] input, final int offset, final int length) {

        // variable size value
        if (precision == 0) {
            if (length == 0)
                return null;
            final ByteBuffer buffer = ByteBuffer.wrap(input, offset, length);
            final int scale = buffer.getInt();
            final byte[] unscaledValue = new byte[length - Integer.BYTES];
            buffer.get(unscaledValue);
            return new BigDecimal(new BigInteger(unscaledValue), scale);
        }

        final byte[] unscaled = new byte[length - 1];
        final boolean neg = (input[offset] == 0); // first byte is zero for negative values
        boolean notNull = !neg;
        if (neg) {
            for (int i = 0; i < unscaled.length; i++) {
                // for negatives, reverse the 1's complement encoding to get the positive equivalent
                unscaled[i] = (byte) (~(int) input[offset + i + 1]);
                notNull = notNull || (unscaled[i] != (byte) 0);
            }
        } else {
            System.arraycopy(input, offset + 1, unscaled, 0, length - 1);
        }

        // we have an encoded null
        if (!notNull) {
            return null;
        }

        final BigInteger bi = neg ? new BigInteger(unscaled).negate() : new BigInteger(unscaled);
        return new BigDecimal(bi, scale, new MathContext(precision)).stripTrailingZeros();
    }

    @Override
    public int expectedObjectWidth() {
        return precision == 0 ? VARIABLE_WIDTH_SENTINEL : encodedSize;
    }
}
