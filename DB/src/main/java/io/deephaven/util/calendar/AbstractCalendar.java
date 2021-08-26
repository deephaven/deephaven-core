/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.calendar;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.util.QueryConstants;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractCalendar implements Calendar {

    public String previousDay(final DBDateTime time) {
        return previousDay(time, 1);
    }

    public String previousDay(final DBDateTime time, final int days) {
        if (time == null) {
            return null;
        }

        final LocalDate t = DBTimeUtils.getZonedDateTime(time, timeZone()).toLocalDate().minusDays(days);

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

    public String nextDay(final DBDateTime time) {
        return nextDay(time, 1);
    }

    public String nextDay(final DBDateTime time, final int days) {
        if (time == null) {
            return null;
        }

        final LocalDate t = DBTimeUtils.getZonedDateTime(time, timeZone()).toLocalDate().plusDays(days);

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

    public String[] daysInRange(DBDateTime start, DBDateTime end) {
        if (start == null || end == null) {
            return new String[0];
        }
        LocalDate day = DBTimeUtils.getZonedDateTime(start, timeZone()).toLocalDate();
        final LocalDate day2 = DBTimeUtils.getZonedDateTime(end, timeZone()).toLocalDate();

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

    public int numberOfDays(final DBDateTime start, final DBDateTime end) {
        return numberOfDays(start, end, false);
    }

    public int numberOfDays(final DBDateTime start, final DBDateTime end, final boolean endInclusive) {
        return numberOfDays(start == null ? null : start.toDateString(timeZone()),
                end == null ? null : end.toDateString(timeZone()), endInclusive);
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

    public long diffNanos(final DBDateTime start, final DBDateTime end) {
        return DBTimeUtils.minus(end, start);
    }

    public double diffDay(final DBDateTime start, final DBDateTime end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / (double) DBTimeUtils.DAY;
    }

    public double diffYear(DBDateTime start, DBDateTime end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / (double) DBTimeUtils.YEAR;
    }

    public DayOfWeek dayOfWeek(final DBDateTime time) {
        if (time == null) {
            return null;
        }
        return DayOfWeek.of(DBTimeUtils.dayOfWeek(time, timeZone()));
    }

    public DayOfWeek dayOfWeek(final String date) {
        if (date == null) {
            return null;
        }
        LocalDate localDate = LocalDate.parse(date);
        return localDate.getDayOfWeek();
    }

}
