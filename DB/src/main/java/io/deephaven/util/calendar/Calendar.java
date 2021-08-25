/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.calendar;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.DBTimeZone;

import java.time.DayOfWeek;

/**
 * A calendar.
 *
 *
 * To comply with the ISO-8601 standard for Dates, Strings should be of the form "yyyy-MM-dd",
 *
 *
 * Methods on DBDateTime may not be precisely defined enough to return a DBDateTime, e.g nextDay().
 * In these cases, the method will return a String as discussed above.
 *
 *
 * To maintain consistency, each calendar has two fields: a name, and a time zone. A calendar with
 * the same schedule but a different time zone is considered a different calendar.
 *
 *
 * Frequently, the default implementation for methods on DBDateTimes is to call the corresponding
 * method on a String with {@code DBDateTime.toDateString}. This can be slower than methods written
 * explicitly for DBDateTimes. If performance is an issue, consider overriding these methods with
 * other behavior.
 */
public interface Calendar {

    /**
     * Gets the name of the calendar.
     *
     * @return the name of the calendar
     */
    String name();

    /**
     * Gets the current date.
     *
     * @return the current day
     */
    default String currentDay() {
        return DBTimeUtils.currentDate(timeZone());
    }

    /**
     * Gets yesterday's date.
     *
     * @return the date before the current day
     */
    default String previousDay() {
        return previousDay(currentDay());
    }

    /**
     * Gets the date the specified number of days prior to the current day.
     *
     * @param days number of days;
     * @return the date {@code days} before the current day
     */
    default String previousDay(final int days) {
        return previousDay(currentDay(), days);
    }

    /**
     * Gets the previous date.
     *
     * @param time time; if null, return null
     * @return the day before {@code time}
     */
    String previousDay(final DBDateTime time);

    /**
     * Gets the date the specified number of days prior to the input date.
     *
     * @param time time; if null, return null
     * @param days number of days;
     * @return the date {@code days} before {@code date}
     */
    String previousDay(final DBDateTime time, final int days);

    /**
     * Gets the previous date.
     *
     * @param date date; if null, return null
     * @return the date before {@code date}
     */
    String previousDay(final String date);

    /**
     * Gets the date the specified number of days prior to the input date.
     *
     * @param date date; if null, return null
     * @param days number of days;
     * @return the date {@code days} before {@code date}
     */
    String previousDay(final String date, final int days);

    /**
     * Gets tomorrow's date.
     *
     * @return the date after the current day
     */
    default String nextDay() {
        return nextDay(currentDay());
    }

    /**
     * Gets the date {@code days} after the current day.
     *
     * @param days number of days;
     * @return the day after the current day
     */
    default String nextDay(final int days) {
        return nextDay(currentDay(), days);
    }

    /**
     * Gets the next date.
     *
     * @param time time; if null, return null
     * @return the day after {@code time}
     */
    String nextDay(final DBDateTime time);

    /**
     * Gets the date {@code days} after the input {@code time}.
     *
     * @param time time; if null, return null
     * @param days number of days;
     * @return the day after {@code time}
     */
    String nextDay(final DBDateTime time, final int days);

    /**
     * Gets the next date.
     *
     * @param date date; if null, return null
     * @return the date after {@code time}
     */
    String nextDay(final String date);

    /**
     * Gets the date {@code days} after the input {@code date}.
     *
     * @param date time; if null, return null
     * @param days number of days;
     * @return the day after {@code time}
     */
    String nextDay(final String date, final int days);

    /**
     * Gets the days in a given range.
     *
     * @param start start of a time range; if null, return empty array
     * @param end end of a time range; if null, return empty array
     * @return the inclusive days between {@code start} and {@code end}
     */
    String[] daysInRange(DBDateTime start, DBDateTime end);

    /**
     * Gets the days in a given range.
     *
     * @param start start of a time range; if null, return empty array
     * @param end end of a time range; if null, return empty array
     * @return the inclusive days between {@code start} and {@code end}
     */
    String[] daysInRange(final String start, final String end);

    /**
     * Gets the number of days in a given range, end date exclusive.
     *
     * @param start start of a time range; if null, return {@code NULL_INT}
     * @param end end of a time range; if null, return {@code NULL_INT}
     * @return the number days between {@code start} and {@code end}, inclusive and exclusive
     *         respectively.
     */
    int numberOfDays(final DBDateTime start, final DBDateTime end);

    /**
     * Gets the number of days in a given range.
     *
     * @param start start of a time range; if null, return {@code NULL_INT}
     * @param end end of a time range; if null, return {@code NULL_INT}
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return the number of days between {@code start} and {@code end}, inclusive and
     *         {@code endInclusive} respectively.
     */
    int numberOfDays(final DBDateTime start, final DBDateTime end, final boolean endInclusive);

    /**
     * Gets the number of days in a given range, end date exclusive.
     *
     * @param start start of a time range; if null, return {@code NULL_INT}
     * @param end end of a time range; if null, return {@code NULL_INT}
     * @return the number of days between {@code start} and {@code end}, inclusive and exclusive
     *         respectively.
     */
    int numberOfDays(final String start, final String end);

    /**
     * Gets the number of days in a given range.
     *
     * @param start start of a time range; if null, return {@code NULL_INT}
     * @param end end of a time range; if null, return {@code NULL_INT}
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return the number of days between {@code start} and {@code end}, inclusive and
     *         {@code endInclusive} respectively.
     */
    int numberOfDays(final String start, final String end, final boolean endInclusive);

    /**
     * Returns the amount of time in nanoseconds between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @return the amount of time in nanoseconds between the {@code start} and {@code end}
     */
    long diffNanos(final DBDateTime start, final DBDateTime end);

    /**
     * Returns the amount of time in days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @return the amount of time in days between the {@code start} and {@code end}
     */
    double diffDay(final DBDateTime start, final DBDateTime end);

    /**
     * Returns the number of years between {@code start} and {@code end}.
     *
     * @param start start; if null, return null
     * @param end end; if null, return null
     * @return the amount of time in years between the {@code start} and {@code end}
     */
    double diffYear(DBDateTime start, DBDateTime end);

    /**
     * Gets the day of the week for the current day.
     *
     * @return the day of the week of the current day
     */
    default DayOfWeek dayOfWeek() {
        return dayOfWeek(currentDay());
    }

    /**
     * Gets the day of the week for a time.
     *
     * @param time time; if null, return null
     * @return the day of the week of {@code time}
     */
    DayOfWeek dayOfWeek(final DBDateTime time);

    /**
     * Gets the day of the week for a time.
     *
     * @param date date; if null, return null
     * @return the day of the week of {@code date}
     */
    DayOfWeek dayOfWeek(final String date);

    /**
     * Gets the timezone of the calendar.
     *
     * @return the time zone of the calendar
     */
    DBTimeZone timeZone();
}
