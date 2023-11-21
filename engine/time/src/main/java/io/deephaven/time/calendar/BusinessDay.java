/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import io.deephaven.base.verify.Require;
import io.deephaven.base.verify.RequirementFailure;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

/**
 * Schedule for a single business day.
 *
 * A business day may contain multiple business periods. For example, some financial exchanges have a morning and an
 * afternoon trading session. This would be represented by a business day with two business periods.
 *
 * @param <T> time type
 */
public class BusinessDay<T extends Comparable<T> & Temporal> {

    /**
     * A holiday with no business periods.
     */
    public static final BusinessDay<LocalTime> HOLIDAY = new BusinessDay<>();

    private final BusinessPeriod<T>[] openPeriods;
    private final T businessStart;
    private final T businessEnd;
    private final long businessNanos;

    /**
     * Creates a BusinessDay instance.
     *
     * @param businessPeriods array of business periods
     * @throws IllegalArgumentException if {@code businessPeriods} overlaps.
     * @throws RequirementFailure if {@code businessPeriods} or any constituent business periods are null.
     */
    BusinessDay(@NotNull final BusinessPeriod<T>[] businessPeriods) {
        Require.neqNull(businessPeriods, "businessPeriods");

        for (int i = 0; i < businessPeriods.length; i++) {
            Require.neqNull(businessPeriods[i], "businessPeriods[" + i + "]");
        }

        this.openPeriods = businessPeriods;

        // Sort the periods
        Arrays.sort(this.openPeriods, Comparator.comparing(BusinessPeriod::start));

        if (businessPeriods.length > 0) {
            this.businessStart = openPeriods[0].start();
            this.businessEnd = openPeriods[openPeriods.length - 1].end();
        } else {
            this.businessStart = null;
            this.businessEnd = null;
        }

        this.businessNanos = Arrays.stream(businessPeriods).map(BusinessPeriod::nanos).reduce(0L, Long::sum);

        // make sure the periods don't overlap
        for (int i = 1; i < this.openPeriods.length; i++) {
            final BusinessPeriod<T> p0 = this.openPeriods[i - 1];
            final BusinessPeriod<T> p1 = this.openPeriods[i];

            if (p1.start().compareTo(p0.end()) < 0) {
                throw new IllegalArgumentException("Periods overlap.");
            }
        }
    }

    /**
     * Creates a BusinessDay instance with no business periods. THis is a holiday.
     */
    BusinessDay() {
        // noinspection unchecked
        this(new BusinessPeriod[0]);
    }

    /**
     * Business periods for the day.
     *
     * @return business periods for the day
     */
    public BusinessPeriod<T>[] periods() {
        return openPeriods;
    }

    /**
     * Start of the business day. Equivalent to the start of the first business period.
     *
     * @return start of the business day, or null for a holiday schedule
     */
    public T businessStart() {
        return businessStart;
    }

    /**
     * End of the business day. Equivalent to the end of the last business period.
     *
     * @return end of the business day, or null for a holiday schedule
     */
    public T businessEnd() {
        return businessEnd;
    }

    /**
     * Gets the length of the business day in nanoseconds. If the business day has multiple periods, only the time
     * during the periods is counted.
     *
     * @return length of the day in nanoseconds
     */
    public long businessNanos() {
        return businessNanos;
    }

    /**
     * Amount of business time in nanoseconds that has elapsed on the given day by the specified time.
     *
     * @param time time
     * @return business time in nanoseconds that has elapsed on the given day by the specified time
     */
    public long businessNanosElapsed(final T time) {
        Require.neqNull(time, "time");
        long elapsed = 0;

        for (BusinessPeriod<T> businessPeriod : openPeriods) {
            if (time.compareTo(businessPeriod.start()) < 0) {
                return elapsed;
            } else if (time.compareTo(businessPeriod.end()) > 0) {
                elapsed += businessPeriod.nanos();
            } else {
                elapsed += businessPeriod.start().until(time, ChronoUnit.NANOS);
                return elapsed;
            }
        }

        return elapsed;
    }

    /**
     * Amount of business time in nanoseconds that remains until the end of the day.
     *
     * @param time time
     * @return business time in nanoseconds that remains until the end of the day.
     */
    public long businessNanosRemaining(final T time) {
        Require.neqNull(time, "time");
        return businessNanos - businessNanosElapsed(time);
    }

    /**
     * Is this day a business day?
     *
     * @return true if it is a business day; false otherwise.
     */
    public boolean isBusinessDay() {
        return businessNanos > 0;
    }

    /**
     * Determines if the specified time is a business time for the day.
     *
     * @param time time.
     * @return true if the time is a business time for the day; otherwise, false.
     */
    public boolean isBusinessTime(final T time) {
        for (BusinessPeriod<T> p : openPeriods) {
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
        if (!(o instanceof BusinessDay))
            return false;
        BusinessDay<?> that = (BusinessDay<?>) o;
        return businessNanos == that.businessNanos && Arrays.equals(openPeriods, that.openPeriods)
                && Objects.equals(businessStart, that.businessStart) && Objects.equals(businessEnd, that.businessEnd);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(businessStart, businessEnd, businessNanos);
        result = 31 * result + Arrays.hashCode(openPeriods);
        return result;
    }

    @Override
    public String toString() {
        return "BusinessDay{" +
                "openPeriods=" + Arrays.toString(openPeriods) +
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
    public static BusinessDay<Instant> toInstant(final BusinessDay<LocalTime> s, final LocalDate date,
            final ZoneId timeZone) {
        // noinspection unchecked
        return new BusinessDay<>(Arrays.stream(s.periods()).map(p -> BusinessPeriod.toInstant(p, date, timeZone))
                .toArray(BusinessPeriod[]::new));
    }
}
