//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;

import java.time.*;

/**
 * An interface for {@link ColumnSource}s that indicate that it both represents a time value, and may be converted
 * between other common time values efficiently.
 */
public interface ConvertibleTimeSource {
    /**
     * Convert this source to a {@link ZonedDateTime} source at the specified {@link ZoneId zone}.
     *
     * @param zone the time zone
     * @return a view of this source as a {@link ZonedDateTime}
     */
    ColumnSource<ZonedDateTime> toZonedDateTime(ZoneId zone);

    /**
     * Convert this source to a {@link LocalDate} source at the specified {@link ZoneId zone}.
     *
     * @param zone the time zone
     * @return a view of this source as a {@link LocalDate}
     */
    ColumnSource<LocalDate> toLocalDate(ZoneId zone);

    /**
     * Convert this source to a {@link LocalTime} source at the specified {@link ZoneId zone}.
     *
     * @param zone the time zone
     * @return a view of this source as a {@link LocalTime}
     */
    ColumnSource<LocalTime> toLocalTime(ZoneId zone);

    /**
     * Convert this source to an {@link Instant} source.
     *
     * @return a view of this source asan {@link Instant}
     */
    ColumnSource<Instant> toInstant();

    /**
     * Convert this source to a {@code long} source of nanoseconds of epoch.
     *
     * @return a view of this source as a {@link ZonedDateTime}
     */
    ColumnSource<Long> toEpochNano();

    /**
     * Check if this class supports time conversion. If false, all other methods will fail.
     *
     * @return true if time conversion is supported.
     */
    boolean supportsTimeConversion();

    interface Zoned {
        ZoneId getZone();
    }
}
