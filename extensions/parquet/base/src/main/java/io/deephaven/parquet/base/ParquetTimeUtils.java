//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.Nullable;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Internal library with utility methods for converting time data between Deephaven and Parquet.
 */
public class ParquetTimeUtils {
    /**
     * Returns nanoseconds from the Epoch for a {@link LocalDateTime} value in UTC timezone.
     *
     * @param localDateTime the local date time to compute the Epoch offset for
     * @return nanoseconds since Epoch, or a NULL_LONG value if the local date time is null
     */
    public static long epochNanosUTC(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return QueryConstants.NULL_LONG;
        }
        return DateTimeUtils.secondsToNanos(localDateTime.toEpochSecond(ZoneOffset.UTC))
                + localDateTime.toLocalTime().getNano();
    }

    /**
     * Converts nanoseconds from the Epoch to a {@link LocalDateTime} in UTC timezone.
     *
     * @param nanos nanoseconds since Epoch
     * @return {@code null} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input nanoseconds from the
     *         Epoch converted to a {@link LocalDateTime} in UTC timezone
     */
    @Nullable
    public static LocalDateTime epochNanosToLocalDateTimeUTC(final long nanos) {
        return nanos == QueryConstants.NULL_LONG ? null
                : LocalDateTime.ofEpochSecond(nanos / 1_000_000_000L, (int) (nanos % 1_000_000_000L), ZoneOffset.UTC);
    }

    /**
     * Converts microseconds from the Epoch to a {@link LocalDateTime} in UTC timezone.
     *
     * @param micros microseconds since Epoch
     * @return {@code null} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input microseconds from the
     *         Epoch converted to a {@link LocalDateTime} in UTC timezone
     */
    @Nullable
    public static LocalDateTime epochMicrosToLocalDateTimeUTC(final long micros) {
        return micros == QueryConstants.NULL_LONG ? null
                : LocalDateTime.ofEpochSecond(micros / 1_000_000L, (int) ((micros % 1_000_000L) * DateTimeUtils.MICRO),
                        ZoneOffset.UTC);
    }

    /**
     * Converts milliseconds from the Epoch to a {@link LocalDateTime} in UTC timezone.
     *
     * @param millis milliseconds since Epoch
     * @return {@code null} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input milliseconds from the
     *         Epoch converted to a {@link LocalDateTime} in UTC timezone
     */
    @Nullable
    public static LocalDateTime epochMillisToLocalDateTimeUTC(final long millis) {
        return millis == QueryConstants.NULL_LONG ? null
                : LocalDateTime.ofEpochSecond(millis / 1_000L, (int) ((millis % 1_000L) * DateTimeUtils.MILLI),
                        ZoneOffset.UTC);
    }

}
