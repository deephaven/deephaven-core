/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.calendar;

import io.deephaven.db.tables.utils.DBDateTime;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * A business calendar. Calendar is extended with the concept of business and non-business time.
 *
 * To comply with the ISO-8601 standard for dates, Strings should be of the form "yyyy-MM-dd",
 */
public interface BusinessCalendar extends Calendar {

    /**
     * Gets the business periods for the default days.
     *
     * @return a list of strings with a comma separating open and close times
     */
    List<String> getDefaultBusinessPeriods();

    /**
     * Gets business schedules for dates that are different from the defaults. This returns all dates that are defined
     * as a holiday for the calendar.
     *
     * @return a map of dates and to their business periods
     */
    Map<LocalDate, BusinessSchedule> getHolidays();

    /**
     * Gets today's business schedule.
     *
     * @return today's business schedule
     */
    default BusinessSchedule currentBusinessSchedule() {
        return getBusinessSchedule(currentDay());
    }

    /**
     * Is the current day a business day?
     *
     * @return true if the current day is a business day; false otherwise.
     */
    default boolean isBusinessDay() {
        return isBusinessDay(currentDay());
    }

    /**
     * Is the day of the week a business day? A business day is a day that has a business schedule with one or more
     * business periods defined.
     *
     * @param day a day of the week
     * @return true if the day is a business day; false otherwise.
     */
    boolean isBusinessDay(DayOfWeek day);

    /**
     * Does time occur on a business day?
     *
     * @param time time
     * @return true if the date is a business day; false otherwise.
     */
    boolean isBusinessDay(final DBDateTime time);

    /**
     * Is the date a business day?
     *
     * @param date date
     * @return true if the date is a business day; false otherwise.
     */
    boolean isBusinessDay(final String date);

    /**
     * Is the date a business day?
     *
     * @param date date
     * @return true if the date is a business day; false otherwise.
     */
    boolean isBusinessDay(final LocalDate date);

    /**
     * Determines if the specified time is a business time. If the time falls between business periods, false will be
     * returned.
     *
     * @param time time
     * @return true if the specified time is a business time; otherwise, false.
     */
    boolean isBusinessTime(final DBDateTime time);

    /**
     * Gets the previous business day.
     *
     * @return previous business day
     */
    default String previousBusinessDay() {
        return previousBusinessDay(currentDay());
    }

    /**
     * Gets the business date {@code days} business days before the current day. If {@code days} is zero and today is
     * not a business day, null is returned.
     *
     * @param days number of days
     * @return the business date {@code days} business days before the current day
     */
    default String previousBusinessDay(int days) {
        return previousBusinessDay(currentDay(), days);
    }

    /**
     * Gets the previous business day.
     *
     * @param time time; if null, return null
     * @return the most recent business day before {@code time}
     */
    String previousBusinessDay(final DBDateTime time);

    /**
     * Gets the business date {@code days} business days before input {@code time}. If {@code days} is zero and the day
     * is not a business day, null is returned.
     *
     * @param time time; if null, return null
     * @param days number of days
     * @return the business date {@code days} business days before input {@code time}
     */
    String previousBusinessDay(final DBDateTime time, int days);

    /**
     * Gets the previous business day.
     *
     * @param date date; if null, return null
     * @return the most recent business day before {@code date}
     */
    String previousBusinessDay(String date);

    /**
     * Gets the business date {@code days} business days before input {@code date}. If {@code days} is zero and the day
     * is not a business day, null is returned.
     *
     * @param date date; if null, return null
     * @param days number of days
     * @return the business date {@code days} business days before input {@code date}
     */
    String previousBusinessDay(String date, int days);


    /**
     * Gets the previous business schedule.
     *
     * @return previous business schedule
     */
    default BusinessSchedule previousBusinessSchedule() {
        return previousBusinessSchedule(currentDay());
    }

    /**
     * Gets the business schedule {@code days} days before the current day.
     *
     * Assumes implementation of getBusinessSchedule(null) returns null.
     *
     * @param days number of days
     * @return the business schedule {@code days} days before the current day
     */
    default BusinessSchedule previousBusinessSchedule(int days) {
        return previousBusinessSchedule(currentDay(), days);
    }

    /**
     * Gets the previous business schedule before input {@code time}.
     *
     * Assumes implementation of getBusinessSchedule(null) returns null.
     *
     * @param time time; if null, return null
     * @return the most recent business schedule before {@code time}
     */
    BusinessSchedule previousBusinessSchedule(final DBDateTime time);

    /**
     * Gets the business schedule {@code days} days before input {@code time}.
     *
     * Assumes implementation of getBusinessSchedule(null) returns null.
     *
     * @param time time; if null, return null
     * @param days number of days
     */
    BusinessSchedule previousBusinessSchedule(final DBDateTime time, int days);

    /**
     * Gets the business schedule before input {@code date}.
     *
     * Assumes implementation of getBusinessSchedule(null) returns null.
     *
     * @param date date; if null, return null
     * @return the most recent business schedule before {@code date}
     */
    BusinessSchedule previousBusinessSchedule(String date);

    /**
     * Gets the business schedule {@code days} days before input {@code date}.
     *
     * Assumes implementation of getBusinessSchedule(null) returns null.
     *
     * @param date date; if null, return null
     * @param days number of days
     * @return the business schedule {@code days} days before input {@code date}
     */
    BusinessSchedule previousBusinessSchedule(String date, int days);

    /**
     * Gets the previous non-business day.
     *
     * @return the most recent non-business day before the current day
     */
    default String previousNonBusinessDay() {
        return previousNonBusinessDay(currentDay());
    }

    /**
     * Gets the non-business date {@code days} non-business days before the current day. If {@code days} is zero and the
     * day is a business day, null is returned.
     *
     * @param days number of days
     * @return the non-business date {@code days} non-business days before the current day
     */
    default String previousNonBusinessDay(int days) {
        return previousNonBusinessDay(currentDay(), days);
    }

    /**
     * Gets the previous non-business day.
     *
     * @param time time; if null, return null
     * @return the most recent non-business day before {@code time}
     */
    String previousNonBusinessDay(final DBDateTime time);

    /**
     * Gets the non-business date {@code days} non-business days before input {@code time}. If {@code days} is zero and
     * the day is a business day, null is returned.
     *
     * @param time time; if null, return null
     * @param days number of days
     * @return the non-business date {@code days} non-business days before input {@code time}
     */
    String previousNonBusinessDay(final DBDateTime time, int days);

    /**
     * Gets the previous non-business day.
     *
     * @param date date; if null, return null
     * @return the most recent non-business day before {@code date}
     */
    String previousNonBusinessDay(String date);

    /**
     * Gets the non-business date {@code days} non-business days before input {@code date}. If {@code days} is zero and
     * the day is a business day, null is returned.
     *
     * @param date date; if null, return null
     * @param days number of days
     * @return the non-business date {@code days} non-business days before input {@code date}
     */
    String previousNonBusinessDay(String date, int days);

    /**
     * Gets the next business day.
     *
     * @return next business day
     */
    default String nextBusinessDay() {
        return nextBusinessDay(currentDay());
    }

    /**
     * Gets the business date {@code days} business days after the current day. If {@code days} is zero and today is not
     * a business day, null is returned.
     *
     * @param days number of days
     * @return the business date {@code days} business days after the current day
     */
    default String nextBusinessDay(int days) {
        return nextBusinessDay(currentDay(), days);
    }

    /**
     * Gets the next business day.
     *
     * @param time time; if null, return null
     * @return the next business day after {@code time}
     */
    String nextBusinessDay(final DBDateTime time);

    /**
     * Gets the business date {@code days} business days after input {@code time}. If {@code days} is zero and the day
     * is not a business day, null is returned.
     *
     * @param time time; if null, return null
     * @param days number of days
     * @return the next business day after {@code time}
     */
    String nextBusinessDay(final DBDateTime time, int days);

    /**
     * Gets the next business day.
     *
     * @param date date; if null, return null
     * @return the next business day after {@code date}
     */
    String nextBusinessDay(String date);

    /**
     * Gets the business date {@code days} business days after input {@code date}. If {@code days} is zero and the day
     * is not a business day, null is returned.
     *
     * @param date date; if null, return null
     * @param days number of days
     * @return the business date {@code days} business days after input {@code date}
     */
    String nextBusinessDay(String date, int days);

    /**
     * Gets the next business schedule.
     *
     * @return next business schedule
     */
    default BusinessSchedule nextBusinessSchedule() {
        return nextBusinessSchedule(currentDay());
    }

    /**
     * Gets the business schedule {@code days} days after the current day.
     *
     * If the current day is null, assumes the implementation of getBusinessSchedule(null) returns null.
     *
     * @param days number of days
     * @return the next closest business schedule after the current day
     */
    default BusinessSchedule nextBusinessSchedule(int days) {
        return nextBusinessSchedule(currentDay(), days);
    }

    /**
     * Gets the next business schedule.
     *
     * @param time time; if null, return null
     * @return the next closest business schedule after {@code time}
     */
    BusinessSchedule nextBusinessSchedule(final DBDateTime time);

    /**
     * Gets the business schedule {@code days} days after input {@code time}.
     *
     * If {@code date} is null, assumes the implementation of getBusinessSchedule(null) returns null.
     *
     * @param time time; if null, return null
     * @param days number of days
     * @return the business schedule {@code days} after {@code time}
     */
    BusinessSchedule nextBusinessSchedule(final DBDateTime time, int days);

    /**
     * Gets the next business schedule after input {@code date}.
     *
     * Assumes implementation of getBusinessSchedule(null) returns null.
     *
     * @param date date; if null, return null
     * @return the next closest business schedule after {@code date}
     */
    BusinessSchedule nextBusinessSchedule(String date);

    /**
     * Gets the business schedule {@code days} days after input {@code date}.
     *
     * If {@code date} is null, assumes the implementation of getBusinessSchedule(null) returns null.
     *
     * @param date date; if null, return null
     * @param days number of days
     * @return the business schedule {@code days} after {@code date}
     */
    BusinessSchedule nextBusinessSchedule(String date, int days);

    /**
     * Gets the next non-business day.
     *
     * @return the next non-business day after the current day
     */
    default String nextNonBusinessDay() {
        return nextNonBusinessDay(currentDay());
    }

    /**
     * Gets the non-business date {@code days} non-business days after the current day. If {@code days} is zero and the
     * day is a business day, null is returned.
     *
     * @param days number of days
     * @return the non-business date {@code days} non-business days after the current day
     */
    default String nextNonBusinessDay(int days) {
        return nextNonBusinessDay(currentDay(), days);
    }

    /**
     * Gets the next non-business day.
     *
     * @param time time; if null, return null
     * @return the next non-business day after {@code time}
     */
    String nextNonBusinessDay(final DBDateTime time);

    /**
     * Gets the non-business date {@code days} non-business days after input {@code time}. If {@code days} is zero and
     * the day is a business day, null is returned.
     *
     * @param time time; if null, return null
     * @param days number of days
     * @return the non-business date {@code days} non-business days after input {@code time}
     */
    String nextNonBusinessDay(final DBDateTime time, int days);

    /**
     * Gets the next non-business day.
     *
     * @param date date; if null, return null
     * @return the next non-business day after {@code date}
     */
    String nextNonBusinessDay(String date);

    /**
     * Gets the non-business date {@code days} non-business days after input {@code date}. If {@code days} is zero and
     * the day is a business day, null is returned.
     *
     * @param date date; if null, return null
     * @param days number of days
     * @return the most recent business day before {@code time}
     */
    String nextNonBusinessDay(String date, int days);

    /**
     * Returns the business days between {@code start} and {@code end}, inclusive.
     *
     * Because no time information (e.g., hours, minutes, seconds) is returned, the corresponding days for {@code start}
     * and {@code end} will be included if they are business days.
     *
     * @param start start time; if null, return empty array
     * @param end end time; if null, return empty array
     * @return inclusive business days between {@code start} and {@code end}
     */
    String[] businessDaysInRange(final DBDateTime start, final DBDateTime end);

    /**
     * Returns the business days between {@code start} and {@code end}, inclusive.
     *
     * Because no time information (e.g., hours, minutes, seconds) is returned, the corresponding days for {@code start}
     * and {@code end} will be included if they are business days.
     *
     * @param start start time; if null, return empty array
     * @param end end time; if null, return empty array
     * @return inclusive business days between {@code start} and {@code end}
     */
    String[] businessDaysInRange(String start, String end);

    /**
     * Returns the non-business days between {@code start} and {@code end}, inclusive.
     *
     * Because no time information (e.g., hours, minutes, seconds) is returned, the corresponding days for {@code start}
     * and {@code end} will be included if they are non-business days.
     *
     * @param start start time; if null, return empty array
     * @param end end time; if null, return empty array
     * @return inclusive non-business days between {@code start} and {@code end}
     */
    String[] nonBusinessDaysInRange(final DBDateTime start, final DBDateTime end);

    /**
     * Returns the non-business days between {@code start} and {@code end}, inclusive.
     *
     * Because no time information (e.g., hours, minutes, seconds) is returned, the corresponding days for {@code start}
     * and {@code end} will be included if they are non-business days.
     *
     * @param start start time; if null, return empty array
     * @param end end time; if null, return empty array
     * @return inclusive non-business days between {@code start} and {@code end}
     */
    String[] nonBusinessDaysInRange(String start, String end);

    /**
     * Returns the length of a standard business day in nanoseconds.
     *
     * @return length of a standard business day in nanoseconds.
     */
    long standardBusinessDayLengthNanos();

    /**
     * Returns the amount of business time in nanoseconds between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @return the amount of business time in nanoseconds between the {@code start} and {@code end}
     */
    long diffBusinessNanos(DBDateTime start, DBDateTime end);

    /**
     * Returns the amount of non-business time in nanoseconds between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @return the amount of non-business time in nanoseconds between the {@code start} and {@code end}
     */
    long diffNonBusinessNanos(final DBDateTime start, final DBDateTime end);

    /**
     * Returns the amount of business time in standard business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @return the amount of business time in standard business days between the {@code start} and {@code end}
     */
    double diffBusinessDay(final DBDateTime start, final DBDateTime end);

    /**
     * Returns the amount of non-business time in standard business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @return the amount of non-business time in standard business days between the {@code start} and {@code end}
     */
    double diffNonBusinessDay(final DBDateTime start, final DBDateTime end);

    /**
     * Returns the number of business years between {@code start} and {@code end}.
     *
     * @param start start; if null, return null
     * @param end end; if null, return null
     * @return the amount of business time in business years between the {@code start} and {@code end}
     */
    double diffBusinessYear(DBDateTime start, DBDateTime end);

    /**
     * Returns the number of business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_INT
     * @param end end time; if null, return NULL_INT
     * @return number of business days between the {@code start} and {@code end}, inclusive and exclusive respectively.
     */
    int numberOfBusinessDays(DBDateTime start, DBDateTime end);

    /**
     * Returns the number of business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return number of business days between the {@code start} and {@code end}, inclusive and {@code endInclusive}
     *         respectively.
     */
    int numberOfBusinessDays(DBDateTime start, DBDateTime end, final boolean endInclusive);

    /**
     * Returns the number of business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_INT
     * @param end end time; if null, return NULL_INT
     * @return number of business days between the {@code start} and {@code end}, inclusive and exclusive respectively.
     */
    int numberOfBusinessDays(String start, String end);

    /**
     * Returns the number of business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_INT
     * @param end end time; if null, return NULL_INT
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return number of business days between the {@code start} and {@code end}, inclusive and {@code endInclusive}
     *         respectively.
     */
    int numberOfBusinessDays(String start, String end, final boolean endInclusive);

    /**
     * Returns the number of non-business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_INT
     * @param end end time; if null, return NULL_INT
     * @return number of business days between the {@code start} and {@code end}, inclusive and exclusive respectively.
     */
    int numberOfNonBusinessDays(DBDateTime start, DBDateTime end);

    /**
     * Returns the number of non-business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return number of business days between the {@code start} and {@code end}, inclusive and {@code endInclusive}
     *         respectively.
     */
    int numberOfNonBusinessDays(DBDateTime start, DBDateTime end, final boolean endInclusive);

    /**
     * Returns the number of non-business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_INT
     * @param end end time; if null, return NULL_INT
     * @return number of non-business days between the {@code start} and {@code end}, inclusive.
     */
    int numberOfNonBusinessDays(final String start, final String end);

    /**
     * Returns the number of non-business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_INT
     * @param end end time; if null, return NULL_INT
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return number of non-business days between the {@code start} and {@code end}, inclusive and {@code endInclusive}
     *         respectively.
     */
    int numberOfNonBusinessDays(final String start, final String end, final boolean endInclusive);

    /**
     * Returns the ratio of the current day's business day length and the standard business day length. For example, a
     * holiday has zero business time and will therefore return 0.0. A normal business day will be of the standard
     * length and will therefore return 1.0. A half day holiday will return 0.5.
     *
     * @see BusinessCalendar#fractionOfBusinessDayRemaining(DBDateTime)
     * @return ratio of the business day length and the standard business day length for the current day
     */
    default double fractionOfStandardBusinessDay() {
        return fractionOfStandardBusinessDay(currentDay());
    }

    /**
     * For the given date, returns the ratio of the business day length and the standard business day length. For
     * example, a holiday has zero business time and will therefore return 0.0. A normal business day will be of the
     * standard length and will therefore return 1.0. A half day holiday will return 0.5.
     *
     * @see BusinessCalendar#fractionOfBusinessDayRemaining(DBDateTime)
     * @param time time; if null, return 0
     * @return ratio of the business day length and the standard business day length for the date
     */
    double fractionOfStandardBusinessDay(final DBDateTime time);

    /**
     * For the given date, returns the ratio of the business day length and the standard business day length. For
     * example, a holiday has zero business time and will therefore return 0.0. A normal business day will be of the
     * standard length and will therefore return 1.0. A half day holiday will return 0.5.
     *
     * @see BusinessCalendar#fractionOfBusinessDayRemaining(DBDateTime)
     * @param date date; if null, return 0
     * @return ratio of the business day length and the standard business day length for the date
     */
    double fractionOfStandardBusinessDay(final String date);

    /**
     * Returns the fraction of the business day remaining after the given time.
     *
     * @param time time
     * @return the fraction of the day left after {@code time}; NULL_DOUBLE if time is null
     */
    double fractionOfBusinessDayRemaining(final DBDateTime time);

    /**
     * Returns the fraction of the business day complete by the given time.
     *
     * @param time time
     * @return the fraction of the day complete by {@code time}; NULL_DOUBLE if time is null
     */
    double fractionOfBusinessDayComplete(final DBDateTime time);

    /**
     * Is the time on the last business day of the month with business time remaining?
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the month with business time remaining; false
     *         otherwise.
     */
    boolean isLastBusinessDayOfMonth(final DBDateTime time);

    /**
     * Is the current day the last business day of the month?
     *
     * @return true if {@code date} is on the last business day of the month; false otherwise.
     */
    default boolean isLastBusinessDayOfMonth() {
        return isLastBusinessDayOfMonth(currentDay());
    }

    /**
     * Is the date the last business day of the month?
     *
     * @param date date
     * @return true if {@code date} is on the last business day of the month; false otherwise.
     */
    boolean isLastBusinessDayOfMonth(final String date);

    /**
     * Is the current day the last business day of the week?
     *
     * @return true if {@code date} is on the last business day of the week; false otherwise.
     */
    default boolean isLastBusinessDayOfWeek() {
        return isLastBusinessDayOfWeek(currentDay());
    }

    /**
     * Is the time on the last business day of the week with business time remaining?
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the week with business time remaining; false
     *         otherwise.
     */
    boolean isLastBusinessDayOfWeek(final DBDateTime time);

    /**
     * Is the date the last business day of the week?
     *
     * @param date date
     * @return true if {@code date} is on the last business day of the week; false otherwise.
     */
    boolean isLastBusinessDayOfWeek(final String date);

    /**
     * Gets the indicated business day.
     *
     * @param time time
     * @return the corresponding BusinessSchedule of {@code time}; null if time is null
     */
    @Deprecated
    BusinessSchedule getBusinessDay(final DBDateTime time);

    /**
     * Gets the indicated business day.
     *
     * @param date date
     * @return the corresponding BusinessSchedule of {@code date}
     */
    @Deprecated
    BusinessSchedule getBusinessDay(String date);

    /**
     * Gets the indicated business day.
     *
     * @param date date
     * @return the corresponding BusinessSchedule of {@code date}
     */
    @Deprecated
    BusinessSchedule getBusinessDay(final LocalDate date);

    /**
     * Gets the indicated business day's schedule. {@code getBusinessSchedule(null)} returns {@code null}.
     *
     * @param time time
     * @return the corresponding BusinessSchedule of {@code time}; null if time is null
     */
    BusinessSchedule getBusinessSchedule(final DBDateTime time);

    /**
     * Gets the indicated business day's schedule. {@code getBusinessSchedule(null)} returns {@code null}.
     *
     * @param date date
     * @return the corresponding BusinessSchedule of {@code date}
     */
    BusinessSchedule getBusinessSchedule(String date);

    /**
     * Gets the indicated business day's schedule. {@code getBusinessSchedule(null)} returns {@code null}.
     *
     * @param date date
     * @return the corresponding BusinessSchedule of {@code date}
     */
    BusinessSchedule getBusinessSchedule(final LocalDate date);
}
