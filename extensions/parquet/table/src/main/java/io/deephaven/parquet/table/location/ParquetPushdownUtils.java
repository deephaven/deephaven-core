//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.ByteComparisons;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

/**
 * Utility methods for Parquet pushdown operations.
 */
abstract class ParquetPushdownUtils {
    private static final long NANOS_PER_SECOND = DateTimeUtils.SECOND;
    private static final long NANOS_PER_MILLI = DateTimeUtils.MILLI;
    private static final long NANOS_PER_MICRO = DateTimeUtils.MICRO;
    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final long MILLIS_PER_SECOND = 1_000L;

    /**
     * Check if the range derived from the parquet statistics contains deephaven's null byte value. Note that we don't
     * use {@link ByteComparisons} here because that has special handling for the {@link QueryConstants#NULL_BYTE}
     * whereas here we are just checking if the range contains the null byte value.
     */
    static boolean containsDeephavenNullByte(final byte min, final byte max) {
        return min <= QueryConstants.NULL_BYTE && QueryConstants.NULL_BYTE <= max;
    }

    static boolean containsDeephavenNullChar(final char min, final char max) {
        return min <= QueryConstants.NULL_CHAR && QueryConstants.NULL_CHAR <= max;
    }

    static boolean containsDeephavenNullDouble(final double min, final double max) {
        // Assuming no NaN values in the range, we can use a simple comparison.
        return min <= QueryConstants.NULL_DOUBLE && QueryConstants.NULL_DOUBLE <= max;
    }

    static boolean containsDeephavenNullFloat(final float min, final float max) {
        return min <= QueryConstants.NULL_FLOAT && QueryConstants.NULL_FLOAT <= max;
    }

    static boolean containsDeephavenNullInt(final int min, final int max) {
        return min <= QueryConstants.NULL_INT && QueryConstants.NULL_INT <= max;
    }

    static boolean containsDeephavenNullLong(final long min, final long max) {
        return min <= QueryConstants.NULL_LONG && QueryConstants.NULL_LONG <= max;
    }

    static boolean containsDeephavenNullShort(final short min, final short max) {
        return min <= QueryConstants.NULL_SHORT && QueryConstants.NULL_SHORT <= max;
    }

    // TODO There is some duplication here with DateTimeUtils, need to fix that.

    /**
     * Converts nanoseconds from the Epoch to an {@link Instant}.
     */
    static Instant epochNanosToInstant(final long nanos) {
        return Instant.ofEpochSecond(nanos / NANOS_PER_SECOND, nanos % NANOS_PER_SECOND);
    }

    /**
     * Converts microseconds from the Epoch to an {@link Instant}.
     */
    static Instant epochMicrosToInstant(final long micros) {
        return Instant.ofEpochSecond(micros / MICROS_PER_SECOND, (micros % MICROS_PER_SECOND) * NANOS_PER_MICRO);
    }

    /**
     * Converts milliseconds from the Epoch to an {@link Instant}.
     */
    static Instant epochMillisToInstant(final long millis) {
        return Instant.ofEpochMilli(millis);
    }

    /**
     * Converts nanoseconds from the Epoch to a {@link LocalDateTime} in UTC timezone.
     */
    static LocalDateTime epochNanosToLocalDateTimeUTC(final long nanos) {
        return LocalDateTime.ofEpochSecond(nanos / NANOS_PER_SECOND, (int) (nanos % NANOS_PER_SECOND), ZoneOffset.UTC);
    }

    /**
     * Converts microseconds from the Epoch to a {@link LocalDateTime} in UTC timezone.
     */
    static LocalDateTime epochMicrosToLocalDateTimeUTC(final long micros) {
        return LocalDateTime.ofEpochSecond(micros / MICROS_PER_SECOND,
                (int) ((micros % MICROS_PER_SECOND) * NANOS_PER_MICRO),
                ZoneOffset.UTC);
    }

    /**
     * Converts milliseconds from the Epoch to a {@link LocalDateTime} in UTC timezone.
     */
    static LocalDateTime epochMillisToLocalDateTimeUTC(final long millis) {
        return LocalDateTime.ofEpochSecond(millis / MILLIS_PER_SECOND,
                (int) ((millis % MILLIS_PER_SECOND) * NANOS_PER_MILLI),
                ZoneOffset.UTC);
    }

    /**
     * Converts the number of milliseconds from midnight to a {@link LocalTime}
     */
    static LocalTime millisOfDayToLocalTime(final int millis) {
        return LocalTime.ofNanoOfDay(millis * NANOS_PER_MILLI);
    }

    /**
     * Converts the number of microseconds from midnight to a {@link LocalTime}
     */
    static LocalTime microsOfDayToLocalTime(final long micros) {
        return LocalTime.ofNanoOfDay(micros * NANOS_PER_MICRO);
    }

    /**
     * Converts the number of nanoseconds from midnight to a {@link LocalTime}
     */
    static LocalTime nanosOfDayToLocalTime(final long nanos) {
        return LocalTime.ofNanoOfDay(nanos);
    }
}
