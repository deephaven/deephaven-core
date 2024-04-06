//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.axistransformations;

import io.deephaven.base.verify.Require;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.calendar.BusinessCalendar;
import io.deephaven.time.calendar.CalendarDay;
import io.deephaven.time.calendar.TimeRange;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * AxisTransform into business time. Useful for plotting time series data with large gaps in non-business hours.
 * <p>
 * The forward transform takes a data value from standard epoch time and translates it into the cumulative business time
 * for the dataset. The inverse transform takes it back to standard epoch time.
 * <p>
 * Data values outside of business hours are not visible.
 */
public class AxisTransformBusinessCalendar implements AxisTransform, Serializable {

    private static final long serialVersionUID = -8648623559661981847L;

    private static class Nugget {
        private final CalendarDay<Instant> calendarDay;
        private final long cumulativeBusinessTimeNanosAtStartOfDay;

        private Nugget(CalendarDay<Instant> day, long cumulativeBusinessTimeNanosAtStartOfDay) {
            this.calendarDay = day;
            this.cumulativeBusinessTimeNanosAtStartOfDay = cumulativeBusinessTimeNanosAtStartOfDay;
        }
    }

    private final BusinessCalendar busCal;
    private final List<Nugget> nuggets = new ArrayList<>();

    /**
     * Creates an AxisTransformBusinessCalendar instance with the specified {@link BusinessCalendar}.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code busCal} can not be null
     * @param busCal business calendar
     */
    public AxisTransformBusinessCalendar(final BusinessCalendar busCal) {
        Require.neqNull(busCal, "BusinessCalendar");
        this.busCal = busCal;
    }

    public BusinessCalendar getBusinessCalendar() {
        return busCal;
    }

    private Nugget getNuggetByTime(final double timeNanos) {
        if (Double.isInfinite(timeNanos) || Double.isNaN(timeNanos)) {
            throw new IllegalStateException("Invalid input: " + timeNanos);
        }

        Nugget nMin = nuggets.size() == 0 ? null : nuggets.get(0);
        Nugget nMax = nuggets.size() == 0 ? null : nuggets.get(nuggets.size() - 1);

        if (nMin == null) {
            final Instant t = DateTimeUtils.epochNanosToInstant((long) timeNanos);
            nMin = new Nugget(busCal.calendarDay(busCal.minusBusinessDays(t, 1)), 0);
            nMax = nMin;
            nuggets.add(nMin);
        }

        while (timeNanos < DateTimeUtils.epochNanos(nMin.calendarDay.businessStart())) {
            final CalendarDay<Instant> d =
                    busCal.calendarDay(busCal.minusBusinessDays(nMin.calendarDay.businessStart(), 1));
            final Nugget n = new Nugget(d, nMin.cumulativeBusinessTimeNanosAtStartOfDay - d.businessNanos());
            nuggets.add(0, n);

            nMin = n;
        }

        // noinspection ConstantConditions nMax can't cause NPE (for now! Don't add nulls to nuggets!)
        while (timeNanos > DateTimeUtils.epochNanos(nMax.calendarDay.businessEnd())) {
            final CalendarDay<Instant> d =
                    busCal.calendarDay(busCal.plusBusinessDays(nMax.calendarDay.businessEnd(), 1));
            final Nugget n =
                    new Nugget(d, nMax.cumulativeBusinessTimeNanosAtStartOfDay + nMax.calendarDay.businessNanos());
            nuggets.add(n);

            nMax = n;
        }

        return findNugget(n -> timeNanos <= DateTimeUtils.epochNanos(n.calendarDay.businessEnd()));
    }

    private Nugget getNuggetByValue(final double value) {
        if (Double.isInfinite(value) || Double.isNaN(value)) {
            throw new IllegalStateException("Invalid input: " + value);
        }

        Nugget nMin = nuggets.size() == 0 ? null : nuggets.get(0);
        Nugget nMax = nuggets.size() == 0 ? null : nuggets.get(nuggets.size() - 1);

        if (nMin == null) {
            return null;
        }

        while (value < nMin.cumulativeBusinessTimeNanosAtStartOfDay) {
            final CalendarDay<Instant> d =
                    busCal.calendarDay(busCal.minusBusinessDays(nMin.calendarDay.businessStart(), 1));
            final Nugget n = new Nugget(d, nMin.cumulativeBusinessTimeNanosAtStartOfDay - d.businessNanos());
            nuggets.add(0, n);

            nMin = n;
        }

        if (nMax == null) {
            return null;
        }

        while (value > nMax.cumulativeBusinessTimeNanosAtStartOfDay + nMax.calendarDay.businessNanos()) {
            final CalendarDay<Instant> d =
                    busCal.calendarDay(busCal.plusBusinessDays(nMax.calendarDay.businessEnd(), 1));
            final Nugget n =
                    new Nugget(d, nMax.cumulativeBusinessTimeNanosAtStartOfDay + nMax.calendarDay.businessNanos());
            nuggets.add(n);

            nMax = n;
        }

        return findNugget(n -> value < n.cumulativeBusinessTimeNanosAtStartOfDay + n.calendarDay.businessNanos());
    }

    // only getNuggetByTime or getNuggetByValue should call this to ensure that the desired value is in range
    private Nugget findNugget(final Predicate<Nugget> lessThanEqual) {
        int iMin = 0, iMax = nuggets.size() - 1;

        while (iMax - iMin > 1) {
            final int iMid = iMin + (iMax - iMin) / 2;
            final Nugget nMid = nuggets.get(iMid);

            if (lessThanEqual.test(nMid)) {
                iMax = iMid;
            } else {
                iMin = iMid;
            }
        }

        final int iMatch;
        if (lessThanEqual.test(nuggets.get(iMin))) {
            iMatch = iMin;
        } else {
            iMatch = lessThanEqual.test(nuggets.get(iMax)) ? iMax : iMin;
        }
        return nuggets.get(iMatch);
    }


    @Override
    public boolean isVisible(final double timeNanos) {
        return !(Double.isInfinite(timeNanos) || Double.isNaN(timeNanos))
                && busCal.isBusinessTime(DateTimeUtils.epochNanosToInstant((long) timeNanos));

    }

    @Override
    public double inverseTransform(final double value) {
        if (Double.isInfinite(value) || Double.isNaN(value)) {
            return Double.NaN;
        }

        final Nugget n = getNuggetByValue(value);

        if (n == null) {
            return Double.NaN;
        }

        double busDayNanos = value - n.cumulativeBusinessTimeNanosAtStartOfDay;
        double timeNanos = DateTimeUtils.epochNanos(n.calendarDay.businessStart());

        for (TimeRange<Instant> period : n.calendarDay.businessTimeRanges()) {
            final double start = DateTimeUtils.epochNanos(period.start());
            final double end = DateTimeUtils.epochNanos(period.end());
            final double length = end - start;

            if (busDayNanos > 0 && length > 0) {
                if (busDayNanos > length) {
                    timeNanos = end;
                    busDayNanos -= length;
                } else {
                    timeNanos = start + busDayNanos;
                    busDayNanos = 0;
                }
            }
        }

        return timeNanos;
    }

    @Override
    public double transform(final double timeNanos) {
        if (Double.isInfinite(timeNanos) || Double.isNaN(timeNanos)) {
            return Double.NaN;
        }

        final Nugget n = getNuggetByTime(timeNanos);

        double value = n.cumulativeBusinessTimeNanosAtStartOfDay;

        for (TimeRange<Instant> period : n.calendarDay.businessTimeRanges()) {
            final double start = DateTimeUtils.epochNanos(period.start());
            final double end = DateTimeUtils.epochNanos(period.end());

            if (timeNanos > start) {
                if (timeNanos < end) {
                    value += timeNanos - start;
                } else {
                    value += end - start;
                }
            }
        }

        return value;
    }

}
