/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

//TODO: review all docs

//TODO: future_day / past_day
//TODO: add regions

/**
 * A calendar.
 *
 * A calendar is associated with a specific time zone.
 *
 * Date strings must be in a format that can be parsed by {@code DateTimeUtils#parseDate}.  Methods that accept
 * strings can be slower than methods written explicitly for {@code Instant}, {@code ZonedDateTime}, or {@code LocalDate}.
 */
public class Calendar {
    
    private final String name;
    private final String description;
    private final ZoneId timeZone;

    /**
     * Creates a new calendar.
     * 
     * @param name calendar name.
     * @param description calendar description.
     * @param timeZone calendar time zone.
     */
    public Calendar(String name, String description, ZoneId timeZone) {
        this.name = name;
        this.description = description;
        this.timeZone = timeZone;
    }

    /**
     * Gets the name of the calendar.
     *
     * @return the name of the calendar
     */
    public String name() {
        return name;
    }

    /**
     * Gets the description of the calendar.
     *
     * @return the description of the calendar
     */
    public String description() {
        return description;
    }
    
    /**
     * Gets the timezone of the calendar.
     *
     * @return the time zone of the calendar
     */
    public ZoneId timeZone() {
        return timeZone;
    }

    /**
     * Gets the current date.
     *
     * @return the current day
     */
    public LocalDate currentDay() {
        return DateTimeUtils.today(timeZone());
    }

        ***today /tomorrow /yesterday

    /**
     * Gets the date the specified number of days prior to the input date.
     *
     * @param date date; if null, return null
     * @param days number of days;
     * @return the date {@code days} before {@code date}
     */
    public LocalDate previousDay(final LocalDate date, final int days) {
        if (date == null) {
            return null;
        }

        return date.minusDays(days);
    }

    /**
     * Gets the previous date.
     *
     * @param date date; if null, return null
     * @return the day before {@code date}
     */
    public LocalDate previousDay(final LocalDate date) {
        return previousDay(date, 1);
    }

    /**
     * Gets the date the specified number of days prior to the input date.
     *
     * @param time time; if null, return null
     * @param days number of days;
     * @return the date {@code days} before {@code date}
     */
    public LocalDate previousDay(final ZonedDateTime time, final int days) {
        if (time == null) {
            return null;
        }

        return previousDay(time.withZoneSameInstant(timeZone()), days);
    }

    /**
     * Gets the previous date.
     *
     * @param time time; if null, return null
     * @return the day before {@code time}
     */
    public LocalDate previousDay(final ZonedDateTime time) {
        return previousDay(time, 1);
    }

    /**
     * Gets the date the specified number of days prior to the input date.
     *
     * @param time time; if null, return null
     * @param days number of days;
     * @return the date {@code days} before {@code date}
     */
    public LocalDate previousDay(final Instant time, final int days) {
        if (time == null) {
            return null;
        }

        return previousDay(DateTimeUtils.toZonedDateTime(time, timeZone()), days);
    }

    /**
     * Gets the previous date.
     *
     * @param time time; if null, return null
     * @return the day before {@code time}
     */
    public LocalDate previousDay(final Instant time) {
        return previousDay(time, 1);
    }

    /**
     * Gets the date the specified number of days prior to the input date.
     *
     * @param date date; if null, return null
     * @param days number of days;
     * @return the date {@code days} before {@code date}
     */
    public LocalDate previousDay(final String date, final int days) {
        if (date == null) {
            return null;
        }

        return previousDay(DateStringUtils.parseLocalDate(date), days);
    }

    /**
     * Gets the previous date.
     *
     * @param date date; if null, return null
     * @return the date before {@code date}
     */
    public LocalDate previousDay(final String date) {
        return previousDay(date, 1);
    }

    /**
     * Gets the date the specified number of days prior to the current day.
     *
     * @param days number of days;
     * @return the date {@code days} before the current day
     */
    public LocalDate previousDay(final int days) {
        return previousDay(currentDay(), days);
    }

    /**
     * Gets yesterday's date.
     *
     * @return the date before the current day
     */
    public LocalDate previousDay() {
        return previousDay(1);
    }

    /**
     * Gets the date {@code days} after the input {@code time}.
     *
     * @param date date; if null, return null
     * @param days number of days;
     * @return the day after {@code date}
     */
    public LocalDate nextDay(final LocalDate date, final int days) {
        if (date == null) {
            return null;
        }

        return date.plusDays(days);
    }

    /**
     * Gets the next date.
     *
     * @param time time; if null, return null
     * @return the day after {@code time}
     */
    public LocalDate nextDay(final LocalDate time) {
        return nextDay(time, 1);
    }

    /**
     * Gets the date {@code days} after the input {@code time}.
     *
     * @param time time; if null, return null
     * @param days number of days;
     * @return the day after {@code time}
     */
    public LocalDate nextDay(final ZonedDateTime time, final int days) {
        if (time == null) {
            return null;
        }

        return nextDay(time.withZoneSameInstant(timeZone()).toLocalDate(), days);
    }

    /**
     * Gets the next date.
     *
     * @param time time; if null, return null
     * @return the day after {@code time}
     */
    public LocalDate nextDay(final ZonedDateTime time) {
        return nextDay(time, 1);
    }

    /**
     * Gets the date {@code days} after the input {@code time}.
     *
     * @param time time; if null, return null
     * @param days number of days;
     * @return the day after {@code time}
     */
    public LocalDate nextDay(final Instant time, final int days) {
        if (time == null) {
            return null;
        }

        return nextDay(DateTimeUtils.toZonedDateTime(time, timeZone()).toLocalDate(), days);
    }

    /**
     * Gets the next date.
     *
     * @param time time; if null, return null
     * @return the day after {@code time}
     */
    public LocalDate nextDay(final Instant time) {
        return nextDay(time, 1);
    }

    /**
     * Gets the date {@code days} after the input {@code date}.
     *
     * @param date time; if null, return null
     * @param days number of days;
     * @return the day after {@code time}
     */
    public LocalDate nextDay(final String date, final int days) {
        if (date == null) {
            return null;
        }

        return nextDay(DateStringUtils.parseLocalDate(date), days);
    }

    /**
     * Gets the next date.
     *
     * @param date date; if null, return null
     * @return the date after {@code time}
     */
    public LocalDate nextDay(final String date) {
        return nextDay(date, 1);
    }

    /**
     * Gets the date {@code days} after the current day.
     *
     * @param days number of days;
     * @return the day after the current day
     */
    public LocalDate nextDay(final int days) {
        return nextDay(currentDay(), days);
    }

    /**
     * Gets tomorrow's date.
     *
     * @return the date after the current day
     */
    public LocalDate nextDay() {
        return nextDay(currentDay());
    }

    /**
     * Gets the days in a given range.
     *
     * @param start start of a time range; if null, return empty array
     * @param end   end of a time range; if null, return empty array
     * @return the inclusive days between {@code start} and {@code end}
     */
    public LocalDate[] daysInRange(final LocalDate start, final LocalDate end) {
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        LocalDate day = start;
        List<LocalDate> dateList = new ArrayList<>();

        while (!day.isAfter(end)) {
            dateList.add(day);
            day = day.plusDays(1);
        }

        return dateList.toArray(new LocalDate[0]);
    }

    /**
     * Gets the days in a given range.
     *
     * @param start start of a time range; if null, return empty array
     * @param end   end of a time range; if null, return empty array
     * @return the inclusive days between {@code start} and {@code end}
     */
    public LocalDate[] daysInRange(final ZonedDateTime start, final ZonedDateTime end) {
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return daysInRange(start.withZoneSameInstant(timeZone()).toLocalDate(), end.withZoneSameInstant(timeZone()).toLocalDate());
    }

    /**
     * Gets the days in a given range.
     *
     * @param start start of a time range; if null, return empty array
     * @param end   end of a time range; if null, return empty array
     * @return the inclusive days between {@code start} and {@code end}
     */
    public LocalDate[] daysInRange(final Instant start, final Instant end) {
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return daysInRange(DateTimeUtils.toZonedDateTime(start, timeZone()).toLocalDate(), DateTimeUtils.toZonedDateTime(end, timeZone()).toLocalDate());
    }

    /**
     * Gets the days in a given range.
     *
     * @param start start of a time range; if null, return empty array
     * @param end   end of a time range; if null, return empty array
     * @return the inclusive days between {@code start} and {@code end}
     */
    public LocalDate[] daysInRange(final String start, final String end) {
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return daysInRange(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end));
    }

    /**
     * Gets the number of days in a given range.
     *
     * @param start        start of a time range
     * @param end          end of a time range
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return the number of days between {@code start} and {@code end}, or {@code NULL_INT} if any input is null.
     */
    public int numberOfDays(final LocalDate start, final LocalDate end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        int days = (int) ChronoUnit.DAYS.between(start, end);
        if (days < 0) {
            days = days - (endInclusive ? 1 : 0);
        } else {
            days = days + (endInclusive ? 1 : 0);
        }
        return days;
    }

    /**
     * Gets the number of days in a given range, end date exclusive.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @return the number of days between {@code start} and {@code end}, or {@code NULL_INT} if any input is null.
     */
    public int numberOfDays(final LocalDate start, final LocalDate end) {
        return numberOfDays(start, end, false);
    }

    /**
     * Gets the number of days in a given range.
     *
     * @param start        start of a time range
     * @param end          end of a time range
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return the number of days between {@code start} and {@code end}, or {@code NULL_INT} if any input is null.
     */
    public int numberOfDays(final ZonedDateTime start, final ZonedDateTime end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberOfDays(start.withZoneSameInstant(timeZone()).toLocalDate(), end.withZoneSameInstant(timeZone()).toLocalDate(), endInclusive);
    }

    /**
     * Gets the number of days in a given range, end date exclusive.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @return the number of days between {@code start} and {@code end}, or {@code NULL_INT} if any input is null.
     */
    public int numberOfDays(final ZonedDateTime start, final ZonedDateTime end) {
        return numberOfDays(start, end, false);
    }

    /**
     * Gets the number of days in a given range.
     *
     * @param start        start of a time range
     * @param end          end of a time range
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return the number of days between {@code start} and {@code end}, or {@code NULL_INT} if any input is null.
     */
    public int numberOfDays(final Instant start, final Instant end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberOfDays(DateTimeUtils.toZonedDateTime(start, timeZone()).toLocalDate(), DateTimeUtils.toZonedDateTime(end, timeZone()).toLocalDate(), endInclusive);
    }

    /**
     * Gets the number of days in a given range, end date exclusive.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @return the number of days between {@code start} and {@code end}, or {@code NULL_INT} if any input is null.
     */
    public int numberOfDays(final Instant start, final Instant end) {
        return numberOfDays(start, end, false);
    }

    /**
     * Gets the number of days in a given range.
     *
     * @param start        start of a time range
     * @param end          end of a time range
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return the number of days between {@code start} and {@code end}, or {@code NULL_INT} if any input is null.
     */
    public int numberOfDays(final String start, final String end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberOfDays(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end), endInclusive);
    }

    /**
     * Gets the number of days in a given range, end date exclusive.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @return the number of days between {@code start} and {@code end}, or {@code NULL_INT} if any input is null.
     */
    public int numberOfDays(final String start, final String end) {
        return numberOfDays(start, end, false);
    }

    //TODO: remove
//    /**
//     * Returns the amount of time in nanoseconds between {@code start} and {@code end}.
//     *
//     * @param start start time; if null, return NULL_LONG
//     * @param end end time; if null, return NULL_LONG
//     * @return the amount of time in nanoseconds between the {@code start} and {@code end}
//     */
//    long diffNanos(final Instant start, final Instant end);
//
//    /**
//     * Returns the amount of time in days between {@code start} and {@code end}.
//     *
//     * @param start start time; if null, return NULL_LONG
//     * @param end end time; if null, return NULL_LONG
//     * @return the amount of time in days between the {@code start} and {@code end}
//     */
//    double diffDay(final Instant start, final Instant end);
//
//    /**
//     * Returns the number of 365 day years between {@code start} and {@code end}.
//     *
//     * @param start start; if null, return null
//     * @param end end; if null, return null
//     * @return the amount of time in years between the {@code start} and {@code end}
//     */
//    double diffYear365(final Instant start, final Instant end);
//
//    /**
//     * Returns the number of average (365.2425 day) years between {@code start} and {@code end}.
//     *
//     * @param start start; if null, return null
//     * @param end end; if null, return null
//     * @return the amount of time in years between the {@code start} and {@code end}
//     */
//    double diffYearAvg(final Instant start, final Instant end);

    //TODO: leave these dayOfWeek methods or remove them and just use DateTimeUtils?!

    /**
     * Gets the day of the week for a time.
     *
     * @param date date; if null, return null
     * @return the day of the week of {@code date}
     */
    public DayOfWeek dayOfWeek(final LocalDate date) {
        if (date == null) {
            return null;
        }

        return DayOfWeek.of(DateTimeUtils.dayOfWeek(date, timeZone()));
    }
    
    /**
     * Gets the day of the week for a time.
     *
     * @param time time; if null, return null
     * @return the day of the week of {@code time}
     */
    public DayOfWeek dayOfWeek(final ZonedDateTime time) {
        if (time == null) {
            return null;
        }

        return dayOfWeek(time.withZoneSameInstant(timeZone()));
    }
    
    /**
     * Gets the day of the week for a time.
     *
     * @param time time; if null, return null
     * @return the day of the week of {@code time}
     */
    public DayOfWeek dayOfWeek(final Instant time) {
        if (time == null) {
            return null;
        }

        return dayOfWeek(DateTimeUtils.toLocalDate(time, timeZone()));
    }

    /**
     * Gets the day of the week for a time.
     *
     * @param date date; if null, return null
     * @return the day of the week of {@code date}
     */
    public DayOfWeek dayOfWeek(final String date) {
        if (date == null) {
            return null;
        }

        return dayOfWeek(DateTimeUtils.parseLocalDate(date));
    }

    /**
     * Gets the day of the week for the current day.
     *
     * @return the day of the week of the current day
     */
    public DayOfWeek dayOfWeek() {
        return dayOfWeek(currentDay());
    }

}
