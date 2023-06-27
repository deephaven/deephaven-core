/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import io.deephaven.base.verify.Require;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

//TODO: review all docs

/**
 * A business calendar. Calendar is extended with the concept of business and non-business time.
 *
 * To comply with the ISO-8601 standard for dates, Strings should be of the form "yyyy-MM-dd",
 */
//TODO: fail on out of range
//TODO: interface, abstract class, or class?
//TODO should the methods be DB null tolerant
public interface BusinessCalendar extends Calendar {

    // region Business Schedule

    //TODO: rename
    /**
     * Gets business schedules for dates that are different from the defaults. This returns all dates that are defined
     * as a holiday for the calendar.
     *
     * @return a map of dates and to their business periods
     */
    default Map<LocalDate, BusinessSchedule> getHolidays() {
        return Collections.unmodifiableMap(holidays);
    }

    //TODO: rename schedule or businessSchedule
    /**
     * Gets the indicated business day's schedule. {@code getBusinessSchedule(null)} returns {@code null}.
     *
     * @param date date
     * @return the corresponding BusinessSchedule of {@code date}
     */
    default BusinessSchedule getBusinessSchedule(final LocalDate date) {
        return dates.computeIfAbsent(date, this::newBusinessDay);
    }

    //TODO: rename schedule
    /**
     * Gets the indicated business day's schedule. {@code getBusinessSchedule(null)} returns {@code null}.
     *
     * @param time time
     * @return the corresponding BusinessSchedule of {@code time}; null if time is null
     */
    default BusinessSchedule getBusinessSchedule(final ZonedDateTime time) {
        if (time == null) {
            return null;
        }

        return getBusinessSchedule(time.withZoneSameInstant(timeZone()));
    }

    //TODO: rename schedule
    /**
     * Gets the indicated business day's schedule. {@code getBusinessSchedule(null)} returns {@code null}.
     *
     * @param time time
     * @return the corresponding BusinessSchedule of {@code time}; null if time is null
     */
    default BusinessSchedule getBusinessSchedule(final Instant time) {
        if (time == null) {
            return null;
        }

        return getBusinessSchedule(time.atZone(timeZone()));
    }

    //TODO: rename schedule
    /**
     * Gets the indicated business day's schedule. {@code getBusinessSchedule(null)} returns {@code null}.
     *
     * @param date date
     * @return the corresponding BusinessSchedule of {@code date}
     */
    default BusinessSchedule getBusinessSchedule(String date) {
        if (date == null) {
            return null;
        }

        return getBusinessSchedule(DateTimeUtils.parseLocalDate(date));
    }

    //TODO: rename current schedule?
    /**
     * Gets today's business schedule.
     *
     * @return today's business schedule
     */
    default BusinessSchedule currentBusinessSchedule() {
        return getBusinessSchedule(currentDay());
    }

    // endregion

    // region Business Day

    /**
     * Does time occur on a business day?
     *
     * @param date date
     * @return true if the date is a business day; false otherwise.
     */
    default boolean isBusinessDay(final LocalDate date) {
        return date != null && getBusinessSchedule(date).isBusinessDay();
    }

    /**
     * Is the date a business day?
     *
     * @param date date
     * @return true if the date is a business day; false otherwise.
     */
    default boolean isBusinessDay(final String date) {
        if (date == null) {
            return false;
        }

        return isBusinessDay(DateTimeUtils.parseLocalDate(date));
    }

    //TODO: review func
    /**
     * Does time occur on a business day?
     *
     * @param time time
     * @return true if the date is a business day; false otherwise.
     */
    default boolean isBusinessDay(final ZonedDateTime time){
        return fractionOfStandardBusinessDay(time) > 0.0;
    }

    //TODO: review func
    /**
     * Does time occur on a business day?
     *
     * @param time time
     * @return true if the date is a business day; false otherwise.
     */
    default boolean isBusinessDay(final Instant time){
        return fractionOfStandardBusinessDay(time) > 0.0;
    }

    //TODO: rename?
    /**
     * Is the day of the week a business day? A business day is a day that has a business schedule with one or more
     * business periods defined.
     *
     * @param day a day of the week
     * @return true if the day is a business day; false otherwise.
     */
    default boolean isBusinessDay(DayOfWeek day){
        return !weekendDays.contains(day);
    }

    //TODO: base on the current day or time?
    /**
     * Is the current day a business day?
     *
     * @return true if the current day is a business day; false otherwise.
     */
    default boolean isBusinessDay() {
        return isBusinessDay(currentDay());
    }

    /**
     * Is the time on the last business day of the month with business time remaining?
     *
     * @param date date
     * @return true if {@code date} is on the last business day of the month with business time remaining; false
     *         otherwise.
     */
    boolean isLastBusinessDayOfMonth(final LocalDate date) {
        if (!isBusinessDay(date)) {
            return false;
        }

        final LocalDate nextBusAfterDate = nextBusinessDay(date);

        if(nextBusAfterDate == null){
            ** raise an error;
            return false;
        }

        return date.getMonth() != nextBusAfterDate.getMonth();
    }

    /**
     * Is the time on the last business day of the month with business time remaining?
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the month with business time remaining; false
     *         otherwise.
     */
    default boolean isLastBusinessDayOfMonth(final ZonedDateTime time) {
        if(time == null){
            ** raise an error;
            return false;
        }

        return isLastBusinessDayOfMonth(DateTimeUtils.toLocalDate(time.withZoneSameInstant(timeZone())));
    }

    /**
     * Is the time on the last business day of the month with business time remaining?
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the month with business time remaining; false
     *         otherwise.
     */
    default boolean isLastBusinessDayOfMonth(final Instant time) {
        if(time == null){
            ** raise an error;
            return false;
        }

        return isLastBusinessDayOfMonth(DateTimeUtils.toLocalDate(time, timeZone()));
    }

    /**
     * Is the date the last business day of the month?
     *
     * @param date date
     * @return true if {@code date} is on the last business day of the month; false otherwise.
     */
    boolean isLastBusinessDayOfMonth(final String date) {
        if(date == null){
            ** raise an error;
            return false;
        }

        return isLastBusinessDayOfMonth(DateTimeUtils.parseLocalDate(date));
    }

    /**
     * Is the current day the last business day of the month?
     *
     * @return true if {@code date} is on the last business day of the month; false otherwise.
     */
    default boolean isLastBusinessDayOfMonth() {
        return isLastBusinessDayOfMonth(currentDay());
    }

    /**
     * Is the date the last business day of the week?
     *
     * @param date date
     * @return true if {@code date} is on the last business day of the week; false otherwise.
     */
    default boolean isLastBusinessDayOfWeek(final LocalDate date){
        if(date == null){
            *** error ***
            return false;
        }

        if (!isBusinessDay(date)) {
            return false;
        }

        final LocalDate nextBusinessDay = nextBusinessDay(date);
        return dayOfWeek(date).compareTo(dayOfWeek(nextBusinessDay)) > 0 || numberOfDays(date, nextBusinessDay) > 6;
    }

    /**
     * Is the time on the last business day of the week with business time remaining?
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the week with business time remaining; false
     *         otherwise.
     */
    default boolean isLastBusinessDayOfWeek(final ZonedDateTime time) {
        if(time == null){
            *** error ***;
            return false;
        }

        return isLastBusinessDayOfWeek(DateTimeUtils.toLocalDate(time, timeZone()));
    }

    /**
     * Is the time on the last business day of the week with business time remaining?
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the week with business time remaining; false
     *         otherwise.
     */
    default boolean isLastBusinessDayOfWeek(final Instant time) {
        if(time == null){
            *** error ***;
            return false;
        }

        return isLastBusinessDayOfWeek(DateTimeUtils.toLocalDate(time, timeZone()));
    }

    /**
     * Is the date the last business day of the week?
     *
     * @param date date
     * @return true if {@code date} is on the last business day of the week; false otherwise.
     */
    default boolean isLastBusinessDayOfWeek(final String date){
        if(date == null){
            *** error ***;
            return false;
        }

        return isLastBusinessDayOfWeek(DateTimeUtils.parseLocalDate(date));
    }

    /**
     * Is the current day the last business day of the week?
     *
     * @return true if {@code date} is on the last business day of the week; false otherwise.
     */
    default boolean isLastBusinessDayOfWeek() {
        return isLastBusinessDayOfWeek(currentDay());
    }

    // endregion

    // region Business Time

    /**
     * Determines if the specified time is a business time. If the time falls between business periods, false will be
     * returned.
     *
     * @param time time
     * @return true if the specified time is a business time; otherwise, false.
     */
    default boolean isBusinessTime(final ZonedDateTime time) {
        return time != null && getBusinessSchedule(time).isBusinessTime(time);
    }

    /**
     * Determines if the specified time is a business time. If the time falls between business periods, false will be
     * returned.
     *
     * @param time time
     * @return true if the specified time is a business time; otherwise, false.
     */
    default boolean isBusinessTime(final Instant time) {
        return time != null && getBusinessSchedule(time).isBusinessTime(time);
    }

    /**
     * Returns the length of a standard business day in nanoseconds.
     *
     * @return length of a standard business day in nanoseconds.
     */
    default long standardBusinessDayLengthNanos() {
        return lengthOfDefaultDayNanos;
    }

    /**
     * For the given date, returns the ratio of the business day length and the standard business day length. For
     * example, a holiday has zero business time and will therefore return 0.0. A normal business day will be of the
     * standard length and will therefore return 1.0. A half day holiday will return 0.5.
     *
     * @see BusinessCalendar#fractionOfBusinessDayRemaining(Instant)
     * @param date date; if null, return 0
     * @return ratio of the business day length and the standard business day length for the date
     */
    default double fractionOfStandardBusinessDay(final LocalDate date){
        final BusinessSchedule schedule = getBusinessSchedule(date);
        return schedule == null ? 0.0 : (double) schedule.getLOBD() / (double) standardBusinessDayLengthNanos();
    }

    /**
     * For the given date, returns the ratio of the business day length and the standard business day length. For
     * example, a holiday has zero business time and will therefore return 0.0. A normal business day will be of the
     * standard length and will therefore return 1.0. A half day holiday will return 0.5.
     *
     * @see BusinessCalendar#fractionOfBusinessDayRemaining(Instant)
     * @param time time; if null, return 0
     * @return ratio of the business day length and the standard business day length for the date
     */
    default double fractionOfStandardBusinessDay(final Instant time){
        return time == null ? 0.0 : fractionOfStandardBusinessDay(DateTimeUtils.toLocalDate(time, timeZone()));
    }

    /**
     * For the given date, returns the ratio of the business day length and the standard business day length. For
     * example, a holiday has zero business time and will therefore return 0.0. A normal business day will be of the
     * standard length and will therefore return 1.0. A half day holiday will return 0.5.
     *
     * @see BusinessCalendar#fractionOfBusinessDayRemaining(Instant)
     * @param time time; if null, return 0
     * @return ratio of the business day length and the standard business day length for the date
     */
    default double fractionOfStandardBusinessDay(final ZonedDateTime time){
        return time == null ? 0.0 : fractionOfStandardBusinessDay(DateTimeUtils.toLocalDate(time.toInstant(), timeZone()));
    }

    /**
     * Returns the ratio of the current day's business day length and the standard business day length. For example, a
     * holiday has zero business time and will therefore return 0.0. A normal business day will be of the standard
     * length and will therefore return 1.0. A half day holiday will return 0.5.
     *
     * @see BusinessCalendar#fractionOfBusinessDayRemaining(Instant)
     * @return ratio of the business day length and the standard business day length for the current day
     */
    default double fractionOfStandardBusinessDay() {
        return fractionOfStandardBusinessDay(currentDay());
    }

    /**
     * Returns the fraction of the business day remaining after the given time.
     *
     * @param time time
     * @return the fraction of the day left after {@code time}; NULL_DOUBLE if time is null
     */
    default double fractionOfBusinessDayRemaining(final Instant time){
        final BusinessSchedule businessDate = getBusinessSchedule(time);
        if (businessDate == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        if (businessDate.getLOBD() == 0) {
            return 0;
        }

        final long businessDaySoFar = businessDate.businessTimeElapsed(time);
        return (double) (businessDate.getLOBD() - businessDaySoFar) / (double) businessDate.getLOBD();
    }

    /**
     * Returns the fraction of the business day remaining after the given time.
     *
     * @param time time
     * @return the fraction of the day left after {@code time}; NULL_DOUBLE if time is null
     */
    default double fractionOfBusinessDayRemaining(final ZonedDateTime time){
        if(time == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return fractionOfBusinessDayRemaining(time.toInstant());
    }

    /**
     * Returns the fraction of the business day complete by the given time.
     *
     * @param time time
     * @return the fraction of the day complete by {@code time}; NULL_DOUBLE if time is null
     */
    default double fractionOfBusinessDayComplete(final Instant time) {
        if (time == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return 1.0 - fractionOfBusinessDayRemaining(time);
    }

    /**
     * Returns the fraction of the business day complete by the given time.
     *
     * @param time time
     * @return the fraction of the day complete by {@code time}; NULL_DOUBLE if time is null
     */
    default double fractionOfBusinessDayComplete(final ZonedDateTime time) {
        if (time == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return fractionOfBusinessDayComplete(time.toInstant());
    }

    // endregion

    // region Ranges

    //TODO: consistently handle inclusive / exclusive in these ranges

    //TODO: add InRange to name
    /**
     * Returns the number of business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_INT
     * @param end end time; if null, return NULL_INT
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return number of business days between the {@code start} and {@code end}, inclusive and {@code endInclusive}
     *         respectively.
     */
    default int numberOfBusinessDays(final LocalDate start, final LocalDate end, final boolean endInclusive)  {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        int days = 0;
        if (start.isBefore(end)) {
            if (isBusinessDay(start)) {
                days++;
            }
            start = nextBusinessDay(start);
        } else if (start.isAfter(end)) {
            //TODO: is this working right?
            return -numberOfBusinessDays(end, start, endInclusive);
        }

        LocalDate day = start;

        while (day.isBefore(end)) {
            days++;
            day = nextBusinessDay(day);
        }

        return days + (endInclusive && isBusinessDay(end) ? 1 : 0);
    }

    //TODO: add InRange to name
    //TODO: add endInclusive on all methods
    /**
     * Returns the number of business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return number of business days between the {@code start} and {@code end}, inclusive and {@code endInclusive}
     *         respectively.
     */
    default int numberOfBusinessDays(Instant start, Instant end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberOfBusinessDays(DateTimeUtils.toLocalDate(start, timeZone()), DateTimeUtils.toLocalDate(end, timeZone()), endInclusive);
    }

    //TODO: add InRange to name
    /**
     * Returns the number of business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return number of business days between the {@code start} and {@code end}, inclusive and {@code endInclusive}
     *         respectively.
     */
    default int numberOfBusinessDays(ZonedDateTime start, ZonedDateTime end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberOfBusinessDays(DateTimeUtils.toLocalDate(start.toInstant(), timeZone()), DateTimeUtils.toLocalDate(end.toInstant(), timeZone()), endInclusive);
    }

    //TODO: add InRange to name
    /**
     * Returns the number of business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_INT
     * @param end end time; if null, return NULL_INT
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return number of business days between the {@code start} and {@code end}, inclusive and {@code endInclusive}
     *         respectively.
     */
    default int numberOfBusinessDays(String start, String end, final boolean endInclusive)  {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberOfBusinessDays(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end), endInclusive);
    }

    //TODO: add InRange to name
    /**
     * Returns the number of non-business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_INT
     * @param end end time; if null, return NULL_INT
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return number of non-business days between the {@code start} and {@code end}, inclusive and {@code endInclusive}
     *         respectively.
     */
    default int numberOfNonBusinessDays(final LocalDate start, final LocalDate end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberOfDays(start, end, endInclusive) - numberOfBusinessDays(start, end, endInclusive);
    }

    //TODO: add InRange to name
    /**
     * Returns the number of non-business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return number of business days between the {@code start} and {@code end}, inclusive and {@code endInclusive}
     *         respectively.
     */
    default int numberOfNonBusinessDays(Instant start, Instant end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberOfNonBusinessDays(DateTimeUtils.toLocalDate(start, timeZone()), DateTimeUtils.toLocalDate(end, timeZone()), endInclusive);
    }

    //TODO: add InRange to name
    /**
     * Returns the number of non-business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_INT
     * @param end end time; if null, return NULL_INT
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return number of non-business days between the {@code start} and {@code end}, inclusive and {@code endInclusive}
     *         respectively.
     */
    default int numberOfNonBusinessDays(final String start, final String end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberOfNonBusinessDays(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end), endInclusive);
    }

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
    default LocalDate[] businessDaysInRange(final LocalDate start, final LocalDate end) {
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        LocalDate day = start;
        final List<LocalDate> dateList = new ArrayList<>();

        while (!day.isAfter(end)) {
            if (isBusinessDay(day)) {
                dateList.add(day);
            }
            day = day.plusDays(1);
        }

        return dateList.toArray(new LocalDate[0]);
    }

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
    default LocalDate[] businessDaysInRange(final Instant start, final Instant end) {
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return businessDaysInRange(DateTimeUtils.toLocalDate(start, timeZone()), DateTimeUtils.toLocalDate(end, timeZone()));
    }

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
    default LocalDate[] businessDaysInRange(final ZonedDateTime start, final ZonedDateTime end) {
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return businessDaysInRange(DateTimeUtils.toLocalDate(start.toInstant(), timeZone()), DateTimeUtils.toLocalDate(end.toInstant(), timeZone()));
    }

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
    default LocalDate[] businessDaysInRange(String start, String end){
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return businessDaysInRange(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end));
    }

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
    default LocalDate[] nonBusinessDaysInRange(final LocalDate start, final LocalDate end){
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        LocalDate day = start;
        final List<LocalDate> dateList = new ArrayList<>();

        while (!day.isAfter(end)) {
            if (!isBusinessDay(day)) {
                dateList.add(day);
            }
            day = day.plusDays(1);
        }

        return dateList.toArray(new LocalDate[0]);
    }

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
    default LocalDate[] nonBusinessDaysInRange(final Instant start, final Instant end){
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return nonBusinessDaysInRange(DateTimeUtils.toLocalDate(start, timeZone()), DateTimeUtils.toLocalDate(end, timeZone()));
    }

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
    default LocalDate[] nonBusinessDaysInRange(final ZonedDateTime start, final ZonedDateTime end){
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return nonBusinessDaysInRange(DateTimeUtils.toLocalDate(start.toInstant(), timeZone()), DateTimeUtils.toLocalDate(end.toInstant(), timeZone()));
    }

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
    default LocalDate[] nonBusinessDaysInRange(String start, String end){
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return nonBusinessDaysInRange(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end));
    }

    /**
     * Returns the amount of business time in nanoseconds between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @return the amount of business time in nanoseconds between the {@code start} and {@code end}
     */
    default long diffBusinessNanos(final Instant start, final Instant end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_LONG;
        }

        if (DateTimeUtils.isAfter(start, end)) {
            return -diffBusinessNanos(end, start);
        }

        long dayDiffNanos = 0;
        Instant day = start;

        while (!DateTimeUtils.isAfter(day, end)) {
            if (isBusinessDay(day)) {
                BusinessSchedule businessDate = getBusinessSchedule(day);

                if (businessDate != null) {
                    for (BusinessPeriod businessPeriod : businessDate.getBusinessPeriods()) {
                        Instant endOfPeriod = businessPeriod.getEndTime();
                        Instant startOfPeriod = businessPeriod.getStartTime();

                        // noinspection StatementWithEmptyBody
                        if (DateTimeUtils.isAfter(day, endOfPeriod) || DateTimeUtils.isBefore(end, startOfPeriod)) {
                            // continue
                        } else if (!DateTimeUtils.isAfter(day, startOfPeriod)) {
                            if (DateTimeUtils.isBefore(end, endOfPeriod)) {
                                dayDiffNanos += DateTimeUtils.minus(end, startOfPeriod);
                            } else {
                                dayDiffNanos += businessPeriod.getLength();
                            }
                        } else {
                            if (DateTimeUtils.isAfter(end, endOfPeriod)) {
                                dayDiffNanos += DateTimeUtils.minus(endOfPeriod, day);
                            } else {
                                dayDiffNanos += DateTimeUtils.minus(end, day);
                            }
                        }
                    }
                }
            }
            day = getBusinessSchedule(nextBusinessDay(day)).getSOBD();
        }
        return dayDiffNanos;
    }

    /**
     * Returns the amount of business time in nanoseconds between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @return the amount of business time in nanoseconds between the {@code start} and {@code end}
     */
    default long diffBusinessNanos(final ZonedDateTime start, final ZonedDateTime end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_LONG;
        }

        return diffBusinessNanos(start.toInstant(), end.toInstant());
    }

    /**
     * Returns the amount of non-business time in nanoseconds between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @return the amount of non-business time in nanoseconds between the {@code start} and {@code end}
     */
    default long diffNonBusinessNanos(final Instant start, final Instant end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_LONG;
        }

        if (DateTimeUtils.isAfter(start, end)) {
            return -diffNonBusinessNanos(end, start);
        }

        return DateTimeUtils.minus(end, start) - diffBusinessNanos(start, end);
    }

    /**
     * Returns the amount of non-business time in nanoseconds between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @return the amount of non-business time in nanoseconds between the {@code start} and {@code end}
     */
    default long diffNonBusinessNanos(final ZonedDateTime start, final ZonedDateTime end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_LONG;
        }

        return diffNonBusinessNanos(start.toInstant(), end.toInstant());
    }

    /**
     * Returns the amount of business time in standard business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @return the amount of business time in standard business days between the {@code start} and {@code end}
     */
    default double diffBusinessDay(final Instant start, final Instant end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return (double) diffBusinessNanos(start, end) / (double) standardBusinessDayLengthNanos();
    }

    /**
     * Returns the amount of business time in standard business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @return the amount of business time in standard business days between the {@code start} and {@code end}
     */
    default double diffBusinessDay(final ZonedDateTime start, final ZonedDateTime end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return diffBusinessDay(start.toInstant(), end.toInstant());
    }

    /**
     * Returns the amount of non-business time in standard business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @return the amount of non-business time in standard business days between the {@code start} and {@code end}
     */
    default double diffNonBusinessDay(final Instant start, final Instant end){
        if (start == null || end == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNonBusinessNanos(start, end) / (double) standardBusinessDayLengthNanos();
    }

    /**
     * Returns the amount of non-business time in standard business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @return the amount of non-business time in standard business days between the {@code start} and {@code end}
     */
    default double diffNonBusinessDay(final ZonedDateTime start, final ZonedDateTime end){
        if (start == null || end == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return diffNonBusinessDay(start.toInstant(), end.toInstant());
    }

    /**
     * Returns the number of business years between {@code start} and {@code end}.
     *
     * @param start start; if null, return null
     * @param end end; if null, return null
     * @return the amount of business time in business years between the {@code start} and {@code end}
     */
   default double diffBusinessYear(final Instant start, final Instant end){
        if (start == null || end == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        double businessYearDiff = 0.0;
        Instant time = start;
        while (!DateTimeUtils.isAfter(time, end)) {
            // get length of the business year
            final int startYear = DateTimeUtils.year(start, timeZone());
            final long businessYearLength = cachedYearLengths.computeIfAbsent(startYear, this::getBusinessYearLength);

            final Instant endOfYear = getFirstBusinessDateTimeOfYear(startYear + 1);
            final long yearDiff;
            if (DateTimeUtils.isAfter(endOfYear, end)) {
                yearDiff = diffBusinessNanos(time, end);
            } else {
                yearDiff = diffBusinessNanos(time, endOfYear);
            }

            businessYearDiff += (double) yearDiff / (double) businessYearLength;
            time = endOfYear;
        }

        return businessYearDiff;
    }

    /**
     * Returns the number of business years between {@code start} and {@code end}.
     *
     * @param start start; if null, return null
     * @param end end; if null, return null
     * @return the amount of business time in business years between the {@code start} and {@code end}
     */
    default double diffBusinessYear(final ZonedDateTime start, final ZonedDateTime end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return diffBusinessYear(start.toInstant(), end.toInstant());
    }

    // endregion

    // region Arithmetic

    //TODO: what is the simpliest API?
    //TODO: simplify names?  e.g. nonbusinessday -> NBD?
    //TODO: should the add/subtract methods on Instants or ZDT return times of LocalDates?

    /**
     * Adds a specified number of business days to an input date.  Adding negative days is equivalent to subtracting days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} business days after {@code date}; null if {@code date} is null or if {@code date} is not a business day and {@code days} is zero.
     */
    default LocalDate addBusinessDays(final LocalDate date, int days) {
        if (date == null) {
            return null;
        } else if(days == 0){
            return isBusinessDay() ? date : null;
        }

        final int step = days > 0 ? 1 : -1;
        LocalDate d = date;
        int count = 0;

        while (count != days) {
            d = d.plusDays(step);
            count += isBusinessDay(d) ? step : 0;
        }

        return d;
    }

    /**
     * Adds a specified number of business days to an input date.  Adding negative days is equivalent to subtracting days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} business days after {@code date}; null if {@code date} is null or if {@code date} is not a business day and {@code days} is zero.
     */
    default LocalDate addBusinessDays(final String date, int days) {
        if(date == null){
            return null;
        }

        //TODO: should the date parsing be quiet? Document exceptions?
        return addBusinessDays(DateTimeUtils.parseLocalDate(date), days);
    }

    /**
         * Adds a specified number of business days to an input time.  Adding negative days is equivalent to subtracting days.
         *
         * @param time time
         * @param days number of days to add.
         * @return {@code days} business days after {@code time}; null if {@code date} is null or if {@code time} is not a business day and {@code days} is zero.
         */
    default LocalDate addBusinessDays(final Instant time, int days) {
        if(time == null){
            return null;
        }

        return addBusinessDays(DateTimeUtils.toLocalDate(time, timeZone()), days);
    }

    /**
     * Adds a specified number of business days to an input time.  Adding negative days is equivalent to subtracting days.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} business days after {@code time}; null if {@code date} is null or if {@code time} is not a business day and {@code days} is zero.
     */
    default LocalDate addBusinessDays(final ZonedDateTime time, int days) {
        if(time == null){
            return null;
        }

        return addBusinessDays(time.toInstant(), days);
    }

    /**
     * Subtracts a specified number of business days from an input date.  Subtracting negative days is equivalent to adding days.
     *
     * @param date date
     * @param days number of days to subtract.
     * @return {@code days} business days before {@code time}; null if {@code date} is null or if {@code time} is not a business day and {@code days} is zero.
     */
    default LocalDate subtractBusinessDays(final LocalDate date, int days) {
        return addBusinessDays(date, -days);
    }

    /**
     * Subtracts a specified number of business days from an input date.  Subtracting negative days is equivalent to adding days.
     *
     * @param date date
     * @param days number of days to subtract.
     * @return {@code days} business days before {@code time}; null if {@code date} is null or if {@code time} is not a business day and {@code days} is zero.
     */
    default LocalDate subtractBusinessDays(final String date, int days) {
        return addBusinessDays(date, -days);
    }

    /**
     * Subtracts a specified number of business days from an input time.  Subtracting negative days is equivalent to adding days.
     *
     * @param time time
     * @param days number of days to subtract.
     * @return {@code days} business days before {@code time}; null if {@code date} is null or if {@code time} is not a business day and {@code days} is zero.
     */
    default LocalDate subtractBusinessDays(final Instant time, int days) {
        return addBusinessDays(time, -days);
    }

    /**
     * Subtracts a specified number of business days from an input time.  Subtracting negative days is equivalent to adding days.
     *
     * @param time time
     * @param days number of days to subtract.
     * @return {@code days} business days before {@code time}; null if {@code date} is null or if {@code time} is not a business day and {@code days} is zero.
     */
    default LocalDate subtractBusinessDays(final ZonedDateTime time, int days) {
        return addBusinessDays(time, -days);
    }

    /**
     * Adds a specified number of non-business days to an input date.  Adding negative days is equivalent to subtracting days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} non-business days after {@code date}; null if {@code date} is null or if {@code date} is a business day and {@code days} is zero.
     */
    default LocalDate addNonBusinessDays(final LocalDate date, int days) {
        if (date == null) {
            return null;
        } else if(days == 0){
            return isBusinessDay() ? null : date;
        }

        final int step = days > 0 ? 1 : -1;
        LocalDate d = date;
        int count = 0;

        while (count != days) {
            d = d.plusDays(step);
            count += isBusinessDay(d) ? 0 : step;
        }

        return d;
    }

    /**
     * Adds a specified number of non-business days to an input date.  Adding negative days is equivalent to subtracting days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} non-business days after {@code date}; null if {@code date} is null or if {@code date} is a business day and {@code days} is zero.
     */
    default LocalDate addNonBusinessDays(final LocalDate date, int days) {
        if(date == null){
            return null;
        }

        return addNonBusinessDays(DateTimeUtils.parseLocalDate(date), days);
    }

        /**
         * Adds a specified number of non-business days to an input time.  Adding negative days is equivalent to subtracting days.
         *
         * @param time time
         * @param days number of days to add.
         * @return {@code days} non-business days after {@code time}; null if {@code date} is null or if {@code date} is a business day and {@code days} is zero.
         */
    default LocalDate addNonBusinessDays(final Instant time, int days) {
        if(time == null){
            return null;
        }

        return addNonBusinessDays(DateTimeUtils.toLocalDate(time, timeZone()), days);
    }

    /**
     * Adds a specified number of non-business days to an input time.  Adding negative days is equivalent to subtracting days.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} non-business days after {@code time}; null if {@code date} is null or if {@code date} is a business day and {@code days} is zero.
     */
    default LocalDate addNonBusinessDays(final ZonedDateTime time, int days) {
        if(time == null){
            return null;
        }

        return addNonBusinessDays(time.toInstant(), days);
    }

    /**
     * Subtracts a specified number of non-business days to an input date.  Subtracting negative days is equivalent to adding days.
     *
     * @param date date
     * @param days number of days to subtract.
     * @return {@code days} non-business days after {@code date}; null if {@code date} is null or if {@code date} is a business day and {@code days} is zero.
     */
    default LocalDate subtractNonBusinessDays(final LocalDate date, int days) {
        return addNonBusinessDays(date, -days);
    }

    /**
     * Subtracts a specified number of non-business days to an input date.  Subtracting negative days is equivalent to adding days.
     *
     * @param date date
     * @param days number of days to subtract.
     * @return {@code days} non-business days after {@code date}; null if {@code date} is null or if {@code date} is a business day and {@code days} is zero.
     */
    default LocalDate subtractNonBusinessDays(final String date, int days) {
        return addNonBusinessDays(date, -days);
    }

    /**
     * Subtracts a specified number of non-business days to an input time.  Subtracting negative days is equivalent to adding days.
     *
     * @param time time
     * @param days number of days to subtract.
     * @return {@code days} non-business days after {@code time}; null if {@code date} is null or if {@code time} is a business day and {@code days} is zero.
     */
    default LocalDate subtractNonBusinessDays(final Instant time, int days) {
        return addNonBusinessDays(time, -days);
    }

    /**
     * Subtracts a specified number of non-business days to an input time.  Subtracting negative days is equivalent to adding days.
     *
     * @param time time
     * @param days number of days to subtract.
     * @return {@code days} non-business days after {@code time}; null if {@code date} is null or if {@code time} is a business day and {@code days} is zero.
     */
    default LocalDate subtractNonBusinessDays(final ZonedDateTime time, int days) {
        return addNonBusinessDays(time, -days);
    }

    /**
     * Adds a specified number of business days to the current date.  Adding negative days is equivalent to subtracting days.
     *
     * @param days number of days to add.
     * @return {@code days} business days after the current date; null if the current date is not a business day and {@code days} is zero.
     */
    default LocalDate nextBusinessDay(int days) {
        return addBusinessDays(currentDay(), days);
    }

    /**
     * Subtracts a specified number of business days from the current date.  Subtracting negative days is equivalent to adding days.
     *
     * @param days number of days to subtract.
     * @return {@code days} business days before the current date; null if the current date is not a business day and {@code days} is zero.
     */
    default LocalDate previousBusinessDay(int days) {
        return subtractBusinessDays(currentDay(), days);
    }

    /**
     * Adds a specified number of non-business days to the current date.  Adding negative days is equivalent to subtracting days.
     *
     * @param days number of days to add.
     * @return {@code days} non-business days after the current date; null if the current date is a business day and {@code days} is zero.
     */
    default LocalDate nextNonBusinessDay(int days) {
        return addNonBusinessDays(currentDay(), days);
    }

    /**
     * Subtracts a specified number of non-business days to the current date.  Subtracting negative days is equivalent to adding days.
     *
     * @param days number of days to subtract.
     * @return {@code days} non-business days before the current date; null if the current date is a business day and {@code days} is zero.
     */
    default LocalDate previousNonBusinessDay(int days) {
        return subtractNonBusinessDays(currentDay(), days);
    }

    // endregion

    //TODO: relocate
    //TODO: remove from API?
    //TODO: rename
    /**
     * Gets the business periods for the default days.
     *
     * @return a list of strings with a comma separating open and close times
     */
    default List<String> getDefaultBusinessPeriods(){
        return Collections.unmodifiableList(defaultBusinessPeriodStrings);
    }

}
