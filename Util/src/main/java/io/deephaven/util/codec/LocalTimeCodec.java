package io.deephaven.util.codec;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.LocalTime;

/**
 * LocalTime codec, with support for nanosecond, millisecond, or second precision. This codec always
 * uses a fixed-width encoding, the size of which depends on the desired precision.
 *
 * The Nanos (default) encoding is a 6-byte packed-integer format that can represent the full range
 * of times that a LocalTime can represent.
 *
 * The Millis encoding is a 4-byte packed-integer format. Sub-millisecond values are truncated.
 *
 * The Seconds encoding is a 3-byte packed-integer format. Sub-second values are truncated.
 *
 * All encodings are "nullable". Since every encoding leaves room for at least one "extra" bit, the
 * leading bit always indicates null - non-null values will always contain zero for the 1st bit.
 */
public class LocalTimeCodec implements ObjectCodec<LocalTime> {
    // decimal precision of fractional second
    private final int precision;
    // how many bits we need to store fractional second portion
    private final int fractionalBits;
    // total encoded size in bytes
    private final int encodedSize;

    // the encoded version of null value (only not-null if column is nullable)
    private final byte[] nullBytes;

    private final byte NULL_INDICATOR = (byte) 0x80;

    @SuppressWarnings("WeakerAccess")
    public LocalTimeCodec(@Nullable String arguments) {

        boolean nullable = true;
        if (arguments != null && !arguments.trim().isEmpty()) {
            try {
                final String[] tokens = arguments.split(",");
                precision = Integer.parseInt(tokens[0].trim());
                if (tokens.length > 1) {
                    final String nullability = tokens[1].trim();
                    switch (nullability.toLowerCase()) {
                        case "nullable":
                            nullable = true;
                            break;
                        case "notnull":
                            nullable = false;
                            break;
                        default:
                            throw new IllegalArgumentException(
                                "Unexpected value for nullability (legal values are \"nullable\" or \"notNull\"): "
                                    + nullability);
                    }
                }
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException(
                    "Error fractional second precision: " + ex.getMessage(), ex);
            }
            if (precision < 0 || precision > 9) {
                throw new IllegalArgumentException(
                    "Invalid fractional second precision: " + precision);
            }
        } else {
            precision = 9;
        }

        // the # of bits we need to store the specified number of digits of decimal precision
        fractionalBits = (int) Math.ceil(Math.log(10) / Math.log(2) * precision);
        // 5 bits for hour, 6 bits for minute, 6 bits for second plus whatever we need for
        // fractional seconds
        int encodedBits = 17 + fractionalBits;
        // add a bit for null indicator if needed
        if (nullable) {
            encodedBits++;
        }
        encodedSize = (int) Math.ceil((double) encodedBits / (double) Byte.SIZE);
        if (nullable) {
            nullBytes = new byte[encodedSize];
            nullBytes[0] = NULL_INDICATOR;
        } else {
            nullBytes = null;
        }
    }

    public boolean isNullable() {
        return (nullBytes != null);
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @NotNull
    @Override
    public byte[] encode(@Nullable final LocalTime input) {

        if (input == null) {
            if (nullBytes != null) {
                return nullBytes;
            } else {
                throw new IllegalArgumentException("Codec cannot encode null time values");
            }
        }
        final byte[] encodedValue = new byte[encodedSize];

        // calculate the fraction of a second we can store (truncates)
        final long fractionalSecond = (long) (input.getNano() / Math.pow(10, 9 - precision));

        // 5 bits for hour, 6 bits for minute, 6 bits for second, 0-30 bits for fractional second
        // so we never need more than 47 bits (48 with null indicator)
        long packedValue = ((long) input.getHour() << 12 + fractionalBits)
            | ((long) input.getMinute() << 6 + fractionalBits)
            | ((long) input.getSecond() << fractionalBits)
            | fractionalSecond;
        for (int i = encodedSize - 1; i >= 0; i--) {
            encodedValue[i] = (byte) (packedValue & 0xFF);
            packedValue >>= 8;
        }
        return encodedValue;
    }

    @Nullable
    @Override
    public LocalTime decode(@NotNull final byte[] input, final int offset, final int length) {
        // test for null indicator (leading bit)
        if ((input[offset] & NULL_INDICATOR) != 0) {
            return null;
        }

        long packedValue = 0;
        for (int i = 0; i < encodedSize; i++) {
            packedValue <<= 8;
            packedValue |= (input[i + offset] & 0xFF);
        }

        // unpack the parts
        final int hour = (int) (packedValue >> 6 + 6 + fractionalBits);
        final int minute = (int) ((packedValue >> 6 + fractionalBits) & 0x3F);
        final int second = (int) ((packedValue >> fractionalBits) & 0x3F);
        final int fractionalSecond = (int) (packedValue & (int) Math.pow(2, fractionalBits) - 1);
        final int nano = fractionalSecond * (int) Math.pow(10, 9 - precision);

        return LocalTime.of(hour, minute, second, nano);
    }

    @Override
    public int expectedObjectWidth() {
        return encodedSize;
    }
}
