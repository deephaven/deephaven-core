/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import io.deephaven.time.DateTimeUtils;

import java.io.Serializable;
import java.time.Instant;

/**
 * A period of business time during a business day.
 */
public class BusinessPeriod implements Serializable {
    private static final long serialVersionUID = 8196837269495115196L;
    private final Instant startTime;
    private final Instant endTime;

    @SuppressWarnings("ConstantConditions")
    BusinessPeriod(final Instant startTime, final Instant endTime) {
        this.startTime = startTime;
        this.endTime = endTime;

        if (startTime == null || endTime == null) {
            throw new IllegalArgumentException("Null argument: startTime=" + startTime + " endTime=" + endTime);
        }

        if (DateTimeUtils.epochNanos(startTime) > DateTimeUtils.epochNanos(endTime)) {
            throw new IllegalArgumentException("Start is after end: startTime=" + startTime + " endTime=" + endTime);
        }
    }

    /**
     * Returns the start of the period.
     *
     * @return the start of the period
     */
    public Instant getStartTime() {
        return startTime;
    }

    /**
     * Returns the end of the period.
     *
     * @return the end of the period
     */
    public Instant getEndTime() {
        return endTime;
    }

    /**
     * Returns the length of the period in nanoseconds.
     *
     * @return length of the period in nanoseconds
     */
    public long getLength() {
        return DateTimeUtils.minus(endTime, startTime);
    }

    /**
     * Determines if the specified time is within the business period.
     *
     * @param time time.
     * @return true if the time is in this period; otherwise, false.
     */
    public boolean contains(final Instant time) {
        return time != null && (DateTimeUtils.epochNanos(startTime) <= DateTimeUtils.epochNanos(time) && DateTimeUtils.epochNanos(time) <= DateTimeUtils.epochNanos(endTime));
    }
}
