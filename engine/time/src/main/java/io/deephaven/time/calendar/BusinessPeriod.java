/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;

/**
 * A period of business time during a business day.
 *
 * @param <T> time type
 */
public class BusinessPeriod<T extends Comparable<T> & Temporal> {
    private final T start;
    private final T end;
    private final long nanos;

    /**
     * Create a new business period.
     *
     * @param startTime start of the business period.
     * @param endTime end of the business period.
     */
    BusinessPeriod(final T startTime, final T endTime) {
        this.start = startTime;
        this.end = endTime;

        if (startTime == null || endTime == null) {
            throw new IllegalArgumentException("Null argument: startTime=" + startTime + " endTime=" + endTime);
        }

        if(startTime.compareTo(endTime) > 0) {
            throw new IllegalArgumentException("Start is after end: startTime=" + startTime + " endTime=" + endTime);
        }

        this.nanos = start.until(end, ChronoUnit.NANOS);
    }

    /**
     * start of the period.
     *
     * @return start of the period
     */
    public T start() {
        return start;
    }

    /**
     * End of the period.
     *
     * @return End of the period
     */
    public T end() {
        return end;
    }

    /**
     * Length of the period in nanoseconds.
     *
     * @return length of the period in nanoseconds
     */
    public long nanos() {
        return nanos;
    }

    /**
     * Determines if the specified time is within the business period.
     *
     * @param time time.
     * @return true if the time is in this period; otherwise, false.
     */
    public boolean contains(final T time) {
        return time != null
                && start.compareTo(time) <= 0
                && time.compareTo(end) <= 0;
    }

    /**
     * Converts a business period in local time to a specific date and time zone.
     *
     * @param p business period in local time
     * @param date date for the new business period
     * @param timeZone time zone for the new business period
     * @return new business period in the specified date and time zone
     */
    public static BusinessPeriod<Instant> toInstant(final BusinessPeriod<LocalTime> p, final LocalDate date, final ZoneId timeZone){
        return new BusinessPeriod<>(DateTimeUtils.toInstant(date, p.start, timeZone), DateTimeUtils.toInstant(date, p.end, timeZone));
    }

}
