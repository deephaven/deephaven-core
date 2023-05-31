/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractCalendar implements Calendar {

    public String previousDay(final Instant time) {
        return previousDay(time, 1);
    }

    public String previousDay(final Instant time, final int days) {
        if (time == null) {
            return null;
        }

        final LocalDate t = DateTimeUtils.toZonedDateTime(time, timeZone()).toLocalDate().minusDays(days);

        return DateStringUtils.format(t);
    }

    public String previousDay(final String date) {
        return previousDay(date, 1);
    }

    public String previousDay(final String date, final int days) {
        if (date == null) {
            return null;
        }

        final LocalDate t = DateStringUtils.parseLocalDate(date).minusDays(days);
        return DateStringUtils.format(t);
    }

    public String nextDay(final Instant time) {
        return nextDay(time, 1);
    }

    public String nextDay(final Instant time, final int days) {
        if (time == null) {
            return null;
        }

        final LocalDate t = DateTimeUtils.toZonedDateTime(time, timeZone()).toLocalDate().plusDays(days);

        return DateStringUtils.format(t);
    }

    public String nextDay(final String date) {
        return nextDay(date, 1);
    }

    public String nextDay(final String date, final int days) {
        if (date == null) {
            return null;
        }

        final LocalDate t = DateStringUtils.parseLocalDate(date).plusDays(days);
        return DateStringUtils.format(t);
    }

    public String[] daysInRange(Instant start, Instant end) {
        if (start == null || end == null) {
            return new String[0];
        }
        LocalDate day = DateTimeUtils.toZonedDateTime(start, timeZone()).toLocalDate();
        final LocalDate day2 = DateTimeUtils.toZonedDateTime(end, timeZone()).toLocalDate();

        List<String> dateList = new ArrayList<>();
        while (!day.isAfter(day2)) {
            dateList.add(DateStringUtils.format(day));
            day = day.plusDays(1);
        }

        return dateList.toArray(new String[dateList.size()]);
    }

    public String[] daysInRange(final String start, final String end) {
        try {
            DateStringUtils.parseLocalDate(start);
            DateStringUtils.parseLocalDate(end);
        } catch (IllegalArgumentException e) {
            return new String[0];
        }

        List<String> dateList = new ArrayList<>();
        String date = start;
        while (!DateStringUtils.isAfterQuiet(date, end)) {
            dateList.add(date);
            date = DateStringUtils.plusDaysQuiet(date, 1);
        }

        return dateList.toArray(new String[dateList.size()]);
    }

    public int numberOfDays(final Instant start, final Instant end) {
        return numberOfDays(start, end, false);
    }

    public int numberOfDays(final Instant start, final Instant end, final boolean endInclusive) {
        return numberOfDays(
                start == null ? null : DateTimeUtils.formatDate(start, timeZone()),
                end == null ? null : DateTimeUtils.formatDate(end, timeZone()),
                endInclusive);
    }

    public int numberOfDays(final String start, final String end) {
        return numberOfDays(start, end, false);
    }

    public int numberOfDays(final String start, final String end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        LocalDate startDay = DateStringUtils.parseLocalDate(start);
        LocalDate endDay = DateStringUtils.parseLocalDate(end);

        int days = (int) ChronoUnit.DAYS.between(startDay, endDay);
        if (days < 0) {
            days = days - (endInclusive ? 1 : 0);
        } else {
            days = days + (endInclusive ? 1 : 0);
        }
        return days;
    }

    public long diffNanos(final Instant start, final Instant end) {
        return DateTimeUtils.minus(end, start);
    }

    public double diffDay(final Instant start, final Instant end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / (double) DateTimeUtils.DAY;
    }

    public double diffYear365(Instant start, Instant end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / (double) DateTimeUtils.YEAR_365;
    }

    public double diffYearAvg(Instant start, Instant end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / (double) DateTimeUtils.YEAR_AVG;
    }

    public DayOfWeek dayOfWeek(final Instant time) {
        if (time == null) {
            return null;
        }
        return DayOfWeek.of(DateTimeUtils.dayOfWeek(time, timeZone()));
    }

    public DayOfWeek dayOfWeek(final String date) {
        if (date == null) {
            return null;
        }
        LocalDate localDate = LocalDate.parse(date);
        return localDate.getDayOfWeek();
    }

}
