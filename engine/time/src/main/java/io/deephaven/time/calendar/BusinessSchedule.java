/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.time.calendar;

import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Description of a single business day.
 */
public class BusinessSchedule implements Serializable {

    private static final long serialVersionUID = 1118129010491637735L;
    private final BusinessPeriod[] openPeriods;
    private final DateTime startOfDay;
    private final DateTime endOfDay;
    private final long lengthOfDay;

    /**
     * Creates the BusinessSchedule instance
     *
     * @param businessPeriods array of {@link BusinessPeriod}
     * @throws IllegalArgumentException if {@code businessPeriods} overlaps.
     */
    BusinessSchedule(@NotNull final BusinessPeriod... businessPeriods) {
        this.openPeriods = businessPeriods.clone();

        // make sure the periods are in order
        Arrays.sort(this.openPeriods, (o1, o2) -> {
            final long compared = o2.getStartTime().getNanos() - o1.getStartTime().getNanos();
            if (compared > 0) {
                return -1;
            } else if (compared == 0) {
                return 0;
            } else {
                return 1;
            }
        });

        if (businessPeriods.length > 0) {
            this.startOfDay = openPeriods[0].getStartTime();
            this.endOfDay = openPeriods[openPeriods.length - 1].getEndTime();
        } else {
            this.startOfDay = null;
            this.endOfDay = null;
        }

        long lod = 0;

        for (BusinessPeriod businessPeriod : openPeriods) {
            if (businessPeriod == null) {
                throw new IllegalArgumentException("Null period.");
            }

            lod += DateTimeUtils.minus(businessPeriod.getEndTime(), businessPeriod.getStartTime());
        }


        this.lengthOfDay = lod;

        // make sure the periods don't overlap
        for (int i = 1; i < this.openPeriods.length; i++) {
            final BusinessPeriod p0 = this.openPeriods[i - 1];
            final BusinessPeriod p1 = this.openPeriods[i];

            if (p1.getStartTime().getNanos() < p0.getEndTime().getNanos()) {
                throw new IllegalArgumentException("Periods overlap.");
            }
        }
    }

    /**
     * Gets the business periods for the day.
     *
     * @return the BusinessPeriods for the day
     */
    public BusinessPeriod[] getBusinessPeriods() {
        return openPeriods;
    }

    /**
     * Gets the start of the business day.
     *
     * @return start of the business day
     */
    public DateTime getSOBD() {
        return startOfDay;
    }

    /**
     * Gets the start of the business day.
     *
     * @return start of the business day
     */
    public DateTime getStartOfBusinessDay() {
        return getSOBD();
    }

    /**
     * Gets the end of the business day.
     *
     * @return end of the business day
     */
    public DateTime getEOBD() {
        return endOfDay;
    }

    /**
     * Gets the end of the business day.
     *
     * @return end of the business day
     */
    public DateTime getEndOfBusinessDay() {
        return getEOBD();
    }

    /**
     * Gets the length of the business day in nanoseconds. If the business day has multiple periods, only the time
     * during the periods is counted.
     *
     * @return length of the day in nanoseconds
     */
    public long getLOBD() {
        return lengthOfDay;
    }

    /**
     * Gets the length of the business day in nanoseconds. If the business day has multiple periods, only the time
     * during the periods is counted.
     *
     * @return length of the day in nanoseconds
     */
    public long getLengthOfBusinessDay() {
        return getLOBD();
    }

    /**
     * Is this day a business day?
     *
     * @return true if it is a business day; false otherwise.
     */
    public boolean isBusinessDay() {
        return openPeriods.length > 0;
    }

    /**
     * Determines if the specified time is a business time for the day.
     *
     * @param time time.
     * @return true if the time is a business time for the day; otherwise, false.
     */
    public boolean isBusinessTime(final DateTime time) {
        for (BusinessPeriod p : openPeriods) {
            if (p.contains(time)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns the amount of business time in nanoseconds that has elapsed on the given day by the specified time.
     *
     * @param time time
     * @return business time in nanoseconds that has elapsed on the given day by the specified time
     */
    public long businessTimeElapsed(final DateTime time) {
        long elapsed = 0;

        for (BusinessPeriod businessPeriod : openPeriods) {
            if (DateTimeUtils.isBefore(time, businessPeriod.getStartTime())) {
                return elapsed;
            } else if (DateTimeUtils.isAfter(time, businessPeriod.getEndTime())) {
                elapsed += businessPeriod.getLength();
            } else {
                elapsed += DateTimeUtils.minus(time, businessPeriod.getStartTime());
                return elapsed;
            }
        }

        return elapsed;
    }
}
