//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.util.compare.LongComparisons;

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

    static boolean containsDeephavenNullByte(final byte min, final byte max) {
        return LongComparisons.geq(QueryConstants.NULL_BYTE, min) &&
                LongComparisons.leq(QueryConstants.NULL_BYTE, max);
    }

    static boolean containsDeephavenNullChar(final char min, final char max) {
        return LongComparisons.geq(QueryConstants.NULL_CHAR, min) &&
                LongComparisons.leq(QueryConstants.NULL_CHAR, max);
    }

    static boolean containsDeephavenNullDouble(final double min, final double max) {
        return DoubleComparisons.geq(QueryConstants.NULL_DOUBLE, min) &&
                DoubleComparisons.leq(QueryConstants.NULL_DOUBLE, max);
    }

    static boolean containsDeephavenNullFloat(final float min, final float max) {
        return FloatComparisons.geq(QueryConstants.NULL_FLOAT, min) &&
                FloatComparisons.leq(QueryConstants.NULL_FLOAT, max);
    }

    static boolean containsDeephavenNullInt(final int min, final int max) {
        return LongComparisons.geq(QueryConstants.NULL_INT, min) &&
                LongComparisons.leq(QueryConstants.NULL_INT, max);
    }

    static boolean containsDeephavenNullLong(final long min, final long max) {
        return LongComparisons.geq(QueryConstants.NULL_LONG, min) &&
                LongComparisons.leq(QueryConstants.NULL_LONG, max);
    }

    static boolean containsDeephavenNullShort(final short min, final short max) {
        return LongComparisons.geq(QueryConstants.NULL_SHORT, min) &&
                LongComparisons.leq(QueryConstants.NULL_SHORT, max);
    }

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
