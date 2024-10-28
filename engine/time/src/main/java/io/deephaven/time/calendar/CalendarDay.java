//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.base.verify.Require;
import io.deephaven.base.verify.RequirementFailure;
import org.jetbrains.annotations.NotNull;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.*;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Schedule for a single calendar day. The schedule contains a list of business time ranges, which are the ranges of
 * time during which businesses are open.
 * <p>
 * A business day may contain multiple business time ranges. For example, some financial exchanges have a morning and an
 * afternoon trading session. This would be represented by a business day with two business time ranges.
 *
 * @param <T> time type
 */
public class CalendarDay<T extends Comparable<T> & Temporal> {

    /**
     * A holiday with no business time ranges.
     */
    public static final CalendarDay<LocalTime> HOLIDAY = new CalendarDay<>();

    private final TimeRange<T>[] businessTimeRanges;
    private final long businessNanos;

    /**
     * Creates a CalendarDay instance.
     *
     * @param businessTimeRanges array of business time ranges for the day.
     * @throws IllegalArgumentException if {@code businessTimeRanges} overlaps.
     * @throws RequirementFailure if {@code businessTimeRanges} or any constituent business time ranges are
     *         {@code null}.
     */
    CalendarDay(@NotNull final TimeRange<T>[] businessTimeRanges) {
        Require.neqNull(businessTimeRanges, "businessTimeRanges");

        for (int i = 0; i < businessTimeRanges.length; i++) {
            Require.neqNull(businessTimeRanges[i], "businessTimeRanges[" + i + "]");
        }

        final TimeRange<T>[] ranges = businessTimeRanges.clone();

        // Sort the ranges
        Arrays.sort(ranges, Comparator.comparing(TimeRange::start));

        // make sure the ranges don't overlap
        for (int i = 1; i < ranges.length; i++) {
            final TimeRange<T> p0 = ranges[i - 1];
            final TimeRange<T> p1 = ranges[i];
            final int cmp = p1.start().compareTo(p0.end());

            if (cmp < 0 || (cmp == 0 && p0.isInclusiveEnd())) {
                throw new IllegalArgumentException("Business time ranges overlap.");
            }
        }

        this.businessTimeRanges = ranges;
        this.businessNanos = Arrays.stream(businessTimeRanges).mapToLong(TimeRange::nanos).sum();
    }

    /**
     * Creates a BusinessDay instance with no business time ranges. This is a holiday.
     */
    CalendarDay() {
        // noinspection unchecked
        this(new TimeRange[0]);
    }

    /**
     * Business time ranges for the day.
     *
     * @return business time ranges for the day
     */
    public List<TimeRange<T>> businessTimeRanges() {
        return Arrays.asList(businessTimeRanges);
    }

    /**
     * Start of the business day. Equivalent to the start of the first business time range.
     *
     * @return start of the business day, or {@code null} for a holiday schedule
     */
    public T businessStart() {
        return businessTimeRanges.length > 0 ? businessTimeRanges[0].start() : null;
    }

    /**
     * End of the business day. Equivalent to the end of the last business time range.
     *
     * @return end of the business day, or {@code null} for a holiday schedule
     */
    public T businessEnd() {
        return businessTimeRanges.length > 0 ? businessTimeRanges[businessTimeRanges.length - 1].end() : null;
    }

    /**
     * Is the end of the business day inclusive? Equivalent to the end of the last business time range.
     *
     * @return is the end of the business day inclusive?
     */
    public boolean isInclusiveEnd() {
        return businessTimeRanges.length == 0 || businessTimeRanges[businessTimeRanges.length - 1].isInclusiveEnd();
    }

    /**
     * Gets the length of the business day in nanoseconds. If the business day has multiple business time ranges, only
     * the time during the ranges is counted.
     *
     * @return length of the day in nanoseconds
     */
    public long businessNanos() {
        return businessNanos;
    }

    /**
     * Gets the length of the business day. If the business day has multiple business time ranges, only the time during
     * the ranges is counted.
     *
     * @return length of the day
     */
    public Duration businessDuration() {
        return Duration.ofNanos(businessNanos());
    }

    /**
     * Amount of business time in nanoseconds that has elapsed on the given day by the specified time.
     *
     * @param time time
     * @return business time in nanoseconds that has elapsed on the given day by the specified time, or
     *         {@link io.deephaven.util.QueryConstants#NULL_LONG} if the input is {@code null}.
     */
    public long businessNanosElapsed(final T time) {
        if (time == null) {
            return NULL_LONG;
        }

        long elapsed = 0;

        for (TimeRange<T> btr : businessTimeRanges) {
            if (time.compareTo(btr.start()) < 0) {
                return elapsed;
            } else if (time.compareTo(btr.end()) > 0) {
                elapsed += btr.nanos();
            } else {
                elapsed += btr.start().until(time, ChronoUnit.NANOS);
                return elapsed;
            }
        }

        return elapsed;
    }

    /**
     * Amount of business time that has elapsed on the given day by the specified time.
     *
     * @param time time
     * @return business time that has elapsed on the given day by the specified time, or {@code null} if the input is
     *         {@code null}.
     */
    public Duration businessDurationElapsed(final T time) {
        if (time == null) {
            return null;
        }

        return Duration.ofNanos(businessNanosElapsed(time));
    }

    /**
     * Amount of business time in nanoseconds that remains until the end of the day.
     *
     * @param time time
     * @return business time in nanoseconds that remains until the end of the day, or
     *         {@link io.deephaven.util.QueryConstants#NULL_LONG} if the input is {@code null}.
     */
    public long businessNanosRemaining(final T time) {
        if (time == null) {
            return NULL_LONG;
        }

        return businessNanos() - businessNanosElapsed(time);
    }

    /**
     * Amount of business time that remains until the end of the day.
     *
     * @param time time
     * @return business time that remains until the end of the day, or {@code null} if the input is {@code null}.
     */
    public Duration businessDurationRemaining(final T time) {
        if (time == null) {
            return null;
        }

        return Duration.ofNanos(businessNanosRemaining(time));
    }

    /**
     * Is this day a business day? A business day is a day that contains at least some business time.
     *
     * @return true if it is a business day; false otherwise.
     */
    public boolean isBusinessDay() {
        return businessNanos() > 0;
    }

    /**
     * Determines if the specified time is a business time for the day.
     *
     * @param time time.
     * @return true if the time is a business time for the day; otherwise, false. If the input is {@code null}, returns
     *         false.
     */
    public boolean isBusinessTime(final T time) {
        if (time == null) {
            return false;
        }

        for (TimeRange<T> p : businessTimeRanges) {
            if (p.contains(time)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof CalendarDay))
            return false;
        CalendarDay<?> that = (CalendarDay<?>) o;
        return Arrays.equals(businessTimeRanges, that.businessTimeRanges);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(businessTimeRanges);
    }

    @Override
    public String toString() {
        return "CalendarDay{" +
                "businessTimeRanges=" + Arrays.toString(businessTimeRanges) +
                '}';
    }

    /**
     * Converts a business schedule in local time to a specific date and time zone.
     *
     * @param s business schedule in local time
     * @param date date for the new business schedule
     * @param timeZone time zone for the new business schedule
     * @return new business schedule in the specified date and time zone
     */
    public static CalendarDay<Instant> toInstant(final CalendarDay<LocalTime> s, final LocalDate date,
            final ZoneId timeZone) {
        // noinspection unchecked
        return new CalendarDay<>(s.businessTimeRanges().stream().map(p -> TimeRange.toInstant(p, date, timeZone))
                .toArray(TimeRange[]::new));
    }
}
