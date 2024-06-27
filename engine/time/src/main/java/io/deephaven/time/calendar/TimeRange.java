//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.time.DateTimeUtils;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.Objects;

/**
 * A range of time.
 *
 * @param <T> time type
 */
public class TimeRange<T extends Comparable<T> & Temporal> {
    private final T start;
    private final T end;
    private final boolean inclusiveEnd;

    /**
     * Create a new time range.
     *
     * @param startTime start of the time range.
     * @param endTime end of the time range.
     * @param inclusiveEnd is the end time inclusive?
     */
    TimeRange(final T startTime, final T endTime, final boolean inclusiveEnd) {
        this.start = startTime;
        this.end = endTime;
        this.inclusiveEnd = inclusiveEnd;

        if (startTime == null || endTime == null) {
            throw new IllegalArgumentException("Null argument: startTime=" + startTime + " endTime=" + endTime);
        }

        final int cmp = startTime.compareTo(endTime);

        if (cmp > 0) {
            throw new IllegalArgumentException("Start is after end: startTime=" + startTime + " endTime=" + endTime);
        }

        if (cmp == 0) {
            throw new IllegalArgumentException(
                    "Start is the same as end: startTime=" + startTime + " endTime=" + endTime);
        }
    }

    /**
     * Start of the range.
     *
     * @return start of the range
     */
    public T start() {
        return start;
    }

    /**
     * End of the range.
     *
     * @return End of the range
     */
    public T end() {
        return end;
    }

    /**
     * Is the end time inclusive?
     *
     * @return is the end time inclusive?
     */
    public boolean isInclusiveEnd() {
        return inclusiveEnd;
    }

    /**
     * Length of the range in nanoseconds.
     *
     * @return length of the range in nanoseconds
     */
    public long nanos() {
        return start.until(end, ChronoUnit.NANOS) + (inclusiveEnd ? 1 : 0);
    }

    /**
     * Duration of the range.
     *
     * @return duration of the range
     */
    public Duration duration() {
        return Duration.ofNanos(nanos());
    }

    /**
     * Determines if the specified time is within the time range.
     *
     * @param time time.
     * @return true if the time is in this range; otherwise, false.
     */
    public boolean contains(final T time) {
        if (inclusiveEnd) {
            return time != null
                    && start.compareTo(time) <= 0
                    && time.compareTo(end) <= 0;
        } else {
            return time != null
                    && start.compareTo(time) <= 0
                    && time.compareTo(end) < 0;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof TimeRange))
            return false;
        TimeRange<?> that = (TimeRange<?>) o;
        return start.equals(that.start) && end.equals(that.end) && inclusiveEnd == that.inclusiveEnd;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end, inclusiveEnd);
    }

    @Override
    public String toString() {
        return "TimeRange{" +
                "start=" + start +
                ", end=" + end +
                ", inclusiveEnd=" + inclusiveEnd +
                '}';
    }

    /**
     * Converts a time range in local time to a specific date and time zone.
     *
     * @param p time range in local time
     * @param date date for the new time range
     * @param timeZone time zone for the new time range
     * @return new time range in the specified date and time zone
     */
    public static TimeRange<Instant> toInstant(final TimeRange<LocalTime> p, final LocalDate date,
            final ZoneId timeZone) {
        return new TimeRange<>(DateTimeUtils.toInstant(date, p.start, timeZone),
                DateTimeUtils.toInstant(date, p.end, timeZone), p.inclusiveEnd);
    }

}
