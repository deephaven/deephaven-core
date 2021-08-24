package io.deephaven.util.codec;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.LocalDate;
import java.time.Year;

/**
 * LocalDate codec, with support for "full" and "compact" encodings.
 *
 * The full (default) encoding is a 5-byte packed integer format that can represent the full range
 * of dates that a LocalDate object can hold (years -999,999,999 to 999,999,999).
 *
 * The compact encoding is a 3-byte packed-integer. This format is constrained to represent dates in
 * the range 0000-01-01 to 9999-01-01. This encoding covers the range supported by many SQL
 * databases, so is often a good candidate for imported data sets.
 *
 * Both encodings are "nullable", indicated by setting the most significant byte to 0xFF (this can
 * never represent a valid value in the ranges specified for each encoding).
 */
public class LocalDateCodec implements ObjectCodec<LocalDate> {

    public enum Domain {
        Full, Compact
    }

    private final Domain domain;
    private final byte[] nullBytes;
    private final int encodedSize;
    private final int minYear, maxYear;

    private final byte NULL_INDICATOR = (byte) 0xFF;

    @SuppressWarnings("WeakerAccess")
    public LocalDateCodec(@Nullable String arguments) {
        // noinspection ConstantConditions
        boolean nullable = true;
        if (arguments != null && !arguments.trim().isEmpty()) {
            final String[] tokens = arguments.split(",");
            final String domainStr = tokens[0].trim();
            try {
                domain = Domain.valueOf(domainStr);
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException(
                    "Unexpected value for LocalDate domain: " + domainStr);
            }
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
        } else {
            domain = Domain.Full;
        }

        switch (domain) {
            case Compact:
                encodedSize = 3;
                minYear = 0;
                maxYear = 9999;
                break;
            case Full:
                encodedSize = 5;
                minYear = Year.MIN_VALUE;
                maxYear = Year.MAX_VALUE;
                break;
            default:
                throw new IllegalArgumentException("Unsupported domain: " + domain);
        }

        if (nullable) {
            nullBytes = new byte[encodedSize];
            nullBytes[0] = NULL_INDICATOR;
        } else {
            nullBytes = null;
        }
    }

    @Override
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
    public byte[] encode(@Nullable final LocalDate input) {
        if (input == null) {
            if (nullBytes != null) {
                return nullBytes;
            } else {
                throw new IllegalArgumentException("Codec cannot encode null LocalDate values");
            }
        } else {
            if (input.getYear() < minYear || input.getYear() > maxYear) {
                throw new IllegalArgumentException(
                    "Year out of legal range [" + minYear + "," + maxYear + "] for "
                        + domain.toString() + " encoder: " + input.getYear());
            }
            final byte[] encodedValue = new byte[encodedSize];
            switch (domain) {
                case Compact: {
                    // 5 bits for day of month, 4 for month, and 14 for the year (leading bit is for
                    // null indicator)
                    // this totals 23 so we have one extra bit for the null indicator in 3 bytes
                    int packedValue = (input.getYear() << 4 + 5)
                        | (input.getMonthValue() << 5)
                        | (input.getDayOfMonth());
                    for (int i = encodedSize - 1; i >= 0; i--) {
                        encodedValue[i] = (byte) (packedValue & 0xFF);
                        packedValue >>= 8;
                    }
                }
                    break;
                case Full: {
                    // 5 bits for day of month, 4 for month, and 31 for the year
                    // this uses every available bit, but since our legal year range is just
                    // -999,999,999 to 999,999,999
                    // a leading 0xFF byte still represents an illegal value
                    // we want to represent negative years, so we convert to a positive value to
                    // avoid messing with the high bit
                    final long year = (long) input.getYear() - minYear; // offset the year to avoid
                                                                        // negative values
                    long packedValue = (year << 4 + 5)
                        | ((long) input.getMonthValue() << 5)
                        | ((long) input.getDayOfMonth());
                    for (int i = encodedSize - 1; i >= 0; i--) {
                        encodedValue[i] = (byte) (packedValue & 0xFF);
                        packedValue >>= 8;
                    }
                }
                    break;
                default:
                    throw new IllegalStateException("Unsupported LocalDate encoding: " + domain);
            }
            return encodedValue;
        }
    }

    @Nullable
    @Override
    public LocalDate decode(@NotNull final byte[] input, final int offset, final int length) {
        final int year, month, dayOfMonth;
        if (input[offset] == NULL_INDICATOR) {
            return null;
        }
        switch (domain) {
            case Compact: {
                int packedValue = 0;
                for (int i = 0; i < encodedSize; i++) {
                    packedValue <<= 8;
                    packedValue |= (input[offset + i] & 0xFF);
                }
                // unpack the parts
                year = (packedValue >> 9);
                month = ((packedValue >> 5) & 15);
                dayOfMonth = ((packedValue) & 31);
            }
                break;
            case Full: {
                long packedValue = 0;
                for (int i = 0; i < encodedSize; i++) {
                    packedValue <<= 8;
                    packedValue |= (input[offset + i] & 0xFF);
                }
                // unpack the parts
                year = (int) (packedValue >> 9) + minYear;
                month = (int) ((packedValue >> 5) & 15);
                dayOfMonth = (int) ((packedValue) & 31);
            }
                break;
            default:
                throw new IllegalStateException("Unsupported LocalDate encoding: " + domain);

        }
        return LocalDate.of(year, month, dayOfMonth);
    }

    @Override
    public int expectedObjectWidth() {
        return encodedSize;
    }
}
