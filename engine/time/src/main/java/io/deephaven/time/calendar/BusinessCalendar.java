/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import io.deephaven.base.verify.Require;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.time.DateTimeUtils;

import java.time.*;
import java.util.*;

/**
 * A business calendar, with the concept of business and non-business time.
 * <p>
 * Date strings must be in a format that can be parsed by {@code DateTimeUtils#parseDate}. Methods that accept strings
 * can be slower than methods written explicitly for {@code Instant}, {@code ZonedDateTime}, or {@code LocalDate}.
 */
public class BusinessCalendar extends Calendar {

    private final LocalDate firstValidDate;
    private final LocalDate lastValidDate;
    private final CalendarDay<LocalTime> standardBusinessDay;
    private final Set<DayOfWeek> weekendDays;
    private final Map<LocalDate, CalendarDay<Instant>> holidays;

    // region Exceptions

    /**
     * A runtime exception that is thrown when a date is invalid.
     */
    public static class InvalidDateException extends RuntimeException {
        /**
         * Creates a new exception.
         *
         * @param message exception message.
         */
        private InvalidDateException(final String message) {
            super(message);
        }

        /**
         * Creates a new exception.
         *
         * @param message exception message.
         * @param cause cause of the exception.
         */
        public InvalidDateException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    // endregion

    // region Cache

    private final Map<LocalDate, CalendarDay<Instant>> cachedSchedules = new HashMap<>();
    private final Map<Integer, YearData> cachedYearData = new HashMap<>();

    private void populateSchedules() {
        LocalDate date = firstValidDate;

        while (!date.isAfter(lastValidDate)) {

            final CalendarDay<Instant> s = holidays.get(date);

            if (s != null) {
                cachedSchedules.put(date, s);
            } else if (weekendDays.contains(date.getDayOfWeek())) {
                cachedSchedules.put(date, CalendarDay.toInstant(CalendarDay.HOLIDAY, date, timeZone()));
            } else {
                cachedSchedules.put(date, CalendarDay.toInstant(standardBusinessDay, date, timeZone()));
            }

            date = date.plusDays(1);
        }
    }

    private static class YearData {
        private final Instant start;
        private final Instant end;
        private final long businessTimeNanos;

        public YearData(final Instant start, final Instant end, final long businessTimeNanos) {
            this.start = start;
            this.end = end;
            this.businessTimeNanos = businessTimeNanos;
        }
    }

    private void populateCachedYearData() {
        // Only cache complete years, since incomplete years can not be fully computed.

        final int yearStart =
                firstValidDate.getDayOfYear() == 1 ? firstValidDate.getYear() : firstValidDate.getYear() + 1;
        final int yearEnd = ((lastValidDate.isLeapYear() && lastValidDate.getDayOfYear() == 366)
                || lastValidDate.getDayOfYear() == 365) ? lastValidDate.getYear() : lastValidDate.getYear() - 1;

        for (int year = yearStart; year <= yearEnd; year++) {
            final LocalDate startDate = LocalDate.ofYearDay(year, 1);
            final LocalDate endDate = LocalDate.ofYearDay(year + 1, 1);
            final ZonedDateTime start = startDate.atTime(0, 0).atZone(timeZone());
            final ZonedDateTime end = endDate.atTime(0, 0).atZone(timeZone());

            LocalDate date = startDate;
            long businessTimeNanos = 0;

            while (date.isBefore(endDate)) {
                final CalendarDay<Instant> bs = this.calendarDay(date);
                businessTimeNanos += bs.businessNanos();
                date = date.plusDays(1);
            }

            final YearData yd = new YearData(start.toInstant(), end.toInstant(), businessTimeNanos);
            cachedYearData.put(year, yd);
        }
    }

    private YearData getYearData(final int year) {
        final YearData yd = cachedYearData.get(year);

        if (yd == null) {
            throw new InvalidDateException("Business calendar does not contain a complete year for: year=" + year);
        }

        return yd;
    }

    // endregion

    // region Constructors

    /**
     * Creates a new business calendar.
     *
     * @param name calendar name.
     * @param description calendar description.
     * @param timeZone calendar time zone.
     * @param firstValidDate first valid date for the business calendar.
     * @param lastValidDate last valid date for the business calendar.
     * @param standardBusinessDay business day schedule for a standard business day
     * @param weekendDays weekend days
     * @param holidays holidays.  Business day schedules for all holidays. A holiday is a date that has a schedule
     *                 that is different from the schedule for a standard business day or weekend.
     */
    public BusinessCalendar(final String name, final String description, final ZoneId timeZone,
            final LocalDate firstValidDate, final LocalDate lastValidDate,
            final CalendarDay<LocalTime> standardBusinessDay, final Set<DayOfWeek> weekendDays,
            final Map<LocalDate, CalendarDay<Instant>> holidays) {
        super(name, description, timeZone);
        this.firstValidDate = firstValidDate;
        this.lastValidDate = lastValidDate;
        this.standardBusinessDay = standardBusinessDay;
        this.weekendDays = Set.copyOf(weekendDays);
        this.holidays = Map.copyOf(holidays);
        populateSchedules();
        populateCachedYearData();
    }

    // endregion

    // region Getters

    /**
     * Returns the first valid date for the business calendar.
     *
     * @return first valid date for the business calendar.
     */
    public LocalDate firstValidDate() {
        return firstValidDate;
    }

    /**
     * Returns the last valid date for the business calendar.
     *
     * @return last valid date for the business calendar.
     */
    public LocalDate lastValidDate() {
        return lastValidDate;
    }


    // endregion

    // region Business Schedule

    /**
     * Returns the days that make up a weekend.
     *
     * @return days that make up a weekend.
     */
    public Set<DayOfWeek> weekendDays() {
        return this.weekendDays;
    }

    /**
     * Business day schedule for a standard business day.
     *
     * @return business day schedule for a standard business day.
     */
    public CalendarDay<LocalTime> standardBusinessDay() {
        return standardBusinessDay;
    }

    /**
     * Length of a standard business day in nanoseconds.
     *
     * @return length of a standard business day in nanoseconds
     */
    public long standardBusinessNanos() {
        return standardBusinessDay.businessNanos();
    }

    /**
     * Business day schedules for all holidays. A holiday is a date that has a schedule that is different from the
     * schedule for a standard business day or weekend.
     *
     * @return a map of holiday dates and their calendar days
     */
    public Map<LocalDate, CalendarDay<Instant>> holidays() {
        return Collections.unmodifiableMap(holidays);
    }

    /**
     * Returns the {@link CalendarDay} for a date.
     *
     * @param date date
     * @return the corresponding {@link CalendarDay} of {@code date}
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public CalendarDay<Instant> calendarDay(final LocalDate date) {
        Require.neqNull(date, "date");

        if (date.isBefore(firstValidDate)) {
            throw new InvalidDateException("Date is before the first valid business calendar date:  date=" + date
                    + " firstValidDate=" + firstValidDate);
        } else if (date.isAfter(lastValidDate)) {
            throw new InvalidDateException("Date is after the last valid business calendar date:  date=" + date
                    + " lastValidDate=" + lastValidDate);
        }

        return cachedSchedules.get(date);
    }

    /**
     * Returns the {@link CalendarDay} for a date.
     *
     * @param time time
     * @return the corresponding {@link CalendarDay} of {@code date}
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public CalendarDay<Instant> calendarDay(final ZonedDateTime time) {
        Require.neqNull(time, "time");
        return calendarDay(time.withZoneSameInstant(timeZone()).toLocalDate());
    }

    /**
     * Returns the {@link CalendarDay} for a date.
     *
     * @param time time
     * @return the corresponding {@link CalendarDay} of {@code date}
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public CalendarDay<Instant> calendarDay(final Instant time) {
        Require.neqNull(time, "time");
        return calendarDay(time.atZone(timeZone()));
    }

    /**
     * Returns the {@link CalendarDay} for a date.
     *
     * @param date date
     * @return the corresponding {@link CalendarDay} of {@code date}
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public CalendarDay<Instant> calendarDay(final String date) {
        Require.neqNull(date, "date");
        return this.calendarDay(DateTimeUtils.parseLocalDate(date));
    }

    /**
     * Returns the {@link CalendarDay} for a date.
     *
     * @return today's business day schedule
     * @throws InvalidDateException if the date is not in the valid range
     */
    public CalendarDay<Instant> calendarDay() {
        return this.calendarDay(calendarDate());
    }

    // endregion

    // region Business Day

    /**
     * Is the date a business day?
     *
     * @param date date
     * @return true if the date is a business day; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isBusinessDay(final LocalDate date) {
        return this.calendarDay(date).isBusinessDay();
    }

    /**
     * Is the date a business day?
     *
     * @param date date
     * @return true if the date is a business day; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public boolean isBusinessDay(final String date) {
        return calendarDay(date).isBusinessDay();
    }

    /**
     * Is the time on a business day?
     *
     * As long as the time occurs on a business day, it is considered a business day.  The time does not have to be
     * within the business day schedule.  To determine if a time is within the business day schedule, use
     * {@link #isBusinessTime(ZonedDateTime)}.
     *
     * @param time time
     * @return true if the date is a business day; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isBusinessDay(final ZonedDateTime time) {
        return this.calendarDay(time).isBusinessDay();
    }

    /**
     * Is the time on a business day?
     *
     * As long as the time occurs on a business day, it is considered a business day.  The time does not have to be
     * within the business day schedule.  To determine if a time is within the business day schedule, use
     * {@link #isBusinessTime(Instant)}.
     *
     * @param time time
     * @return true if the date is a business day; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isBusinessDay(final Instant time) {
        return this.calendarDay(time).isBusinessDay();
    }

    /**
     * Is the day of the week a normal business day?
     *
     * @param day a day of the week
     * @return true if the day is a business day; false otherwise
     * @throws RequirementFailure if the input is null
     */
    public boolean isBusinessDay(final DayOfWeek day) {
        Require.neqNull(day, "day");
        return !weekendDays.contains(day);
    }

    /**
     * Is the current day a business day?  As long as the current time occurs on a business day, it is considered a
     * business day.  The time does not have to be within the business day schedule.
     *
     * @return true if the current day is a business day; false otherwise
     */
    public boolean isBusinessDay() {
        return isBusinessDay(calendarDate());
    }

    /**
     * Is the date the last business day of the month?
     *
     * @param date date
     * @return true if {@code date} is the last business day of the month; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    boolean isLastBusinessDayOfMonth(final LocalDate date) {
        Require.neqNull(date, "date");

        if (!isBusinessDay(date)) {
            return false;
        }

        final LocalDate nextBusAfterDate = plusBusinessDays(date, 1);
        assert nextBusAfterDate != null;
        return date.getMonth() != nextBusAfterDate.getMonth();
    }

    /**
     * Is the time on the last business day of the month?
     *
     * As long as the time occurs on a business day, it is considered a business day.  The time does not have to be
     * within the business day schedule.
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the month; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isLastBusinessDayOfMonth(final ZonedDateTime time) {
        Require.neqNull(time, "time");
        return isLastBusinessDayOfMonth(DateTimeUtils.toLocalDate(time.withZoneSameInstant(timeZone())));
    }

    /**
     * Is the time on the last business day of the month?
     *
     * As long as the time occurs on a business day, it is considered a business day.  The time does not have to be
     * within the business day schedule.
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the month; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isLastBusinessDayOfMonth(final Instant time) {
        Require.neqNull(time, "time");
        return isLastBusinessDayOfMonth(DateTimeUtils.toLocalDate(time, timeZone()));
    }

    /**
     * Is the date the last business day of the month?
     *
     * @param date date
     * @return true if {@code time} is on the last business day of the month; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public boolean isLastBusinessDayOfMonth(final String date) {
        Require.neqNull(date, "date");
        return isLastBusinessDayOfMonth(DateTimeUtils.parseLocalDate(date));
    }

    /**
     * Is the current date the last business day of the month?
     *
     * @return true if the current date is the last business day of the month; false otherwise.
     */
    public boolean isLastBusinessDayOfMonth() {
        return isLastBusinessDayOfMonth(calendarDate());
    }

    /**
     * Is the date the last business day of the week?
     *
     * @param date date
     * @return true if {@code date} is on the last business day of the week; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isLastBusinessDayOfWeek(final LocalDate date) {
        Require.neqNull(date, "date");

        if (!isBusinessDay(date)) {
            return false;
        }

        final LocalDate nextBusinessDay = plusBusinessDays(date, 1);
        return date.getDayOfWeek().compareTo(nextBusinessDay.getDayOfWeek()) > 0
                || numberCalendarDates(date, nextBusinessDay) > 6;
    }

    /**
     * Is the time on the last business day of the week?
     *
     * As long as the time occurs on a business day, it is considered a business day.  The time does not have to be
     * within the business day schedule.
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the week; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isLastBusinessDayOfWeek(final ZonedDateTime time) {
        Require.neqNull(time, "time");
        return isLastBusinessDayOfWeek(time.withZoneSameInstant(timeZone()).toLocalDate());
    }

    /**
     * Is the time on the last business day of the week?
     *
     * As long as the time occurs on a business day, it is considered a business day.  The time does not have to be
     * within the business day schedule.
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the week; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isLastBusinessDayOfWeek(final Instant time) {
        Require.neqNull(time, "time");
        return isLastBusinessDayOfWeek(DateTimeUtils.toLocalDate(time, timeZone()));
    }

    /**
     * Is the date is last business day of the week?
     *
     * @param date date
     * @return true if {@code date} is the last business day of the week; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public boolean isLastBusinessDayOfWeek(final String date) {
        Require.neqNull(date, "date");
        return isLastBusinessDayOfWeek(DateTimeUtils.parseLocalDate(date));
    }

    /**
     * Is the current date the last business day of the week?
     *
     * @return true if the current date is the last business day of the week; false otherwise.
     */
    public boolean isLastBusinessDayOfWeek() {
        return isLastBusinessDayOfWeek(calendarDate());
    }

    /**
     * Is the date the last business day of the year?
     *
     * @param date date
     * @return true if {@code date} is the last business day of the year; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    boolean isLastBusinessDayOfYear(final LocalDate date) {
        Require.neqNull(date, "date");

        if (!isBusinessDay(date)) {
            return false;
        }

        final LocalDate nextBusAfterDate = plusBusinessDays(date, 1);
        assert nextBusAfterDate != null;
        return date.getYear() != nextBusAfterDate.getYear();
    }

    /**
     * Is the time on the last business day of the year?
     *
     * As long as the time occurs on a business day, it is considered a business day.  The time does not have to be
     * within the business day schedule.
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the year; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isLastBusinessDayOfYear(final ZonedDateTime time) {
        Require.neqNull(time, "time");
        return isLastBusinessDayOfYear(DateTimeUtils.toLocalDate(time.withZoneSameInstant(timeZone())));
    }

    /**
     * Is the time on the last business day of the year?
     *
     * As long as the time occurs on a business day, it is considered a business day.  The time does not have to be
     * within the business day schedule.
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the year; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isLastBusinessDayOfYear(final Instant time) {
        Require.neqNull(time, "time");
        return isLastBusinessDayOfYear(DateTimeUtils.toLocalDate(time, timeZone()));
    }

    /**
     * Is the date the last business day of the year?
     *
     * @param date date
     * @return true if {@code time} is on the last business day of the year; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    boolean isLastBusinessDayOfYear(final String date) {
        Require.neqNull(date, "date");
        return isLastBusinessDayOfYear(DateTimeUtils.parseLocalDate(date));
    }

    /**
     * Is the current date the last business day of the year?
     *
     * As long as the current time occurs on a business day, it is considered a business day.  The time does not have
     * to be within the business day schedule.
     *
     * @return true if the current date is the last business day of the year; false otherwise.
     */
    public boolean isLastBusinessDayOfYear() {
        return isLastBusinessDayOfYear(calendarDate());
    }

    // endregion

    // region Business Time

    /**
     * Determines if the specified time is a business time. Business times fall within business time ranges of the day's
     * business schedule.
     *
     * @param time time
     * @return true if the specified time is a business time; otherwise, false
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isBusinessTime(final ZonedDateTime time) {
        Require.neqNull(time, "time");
        return this.calendarDay(time).isBusinessTime(time.toInstant());
    }

    /**
     * Determines if the specified time is a business time. Business times fall within business time ranges of the day's
     * business schedule.
     *
     * @param time time
     * @return true if the specified time is a business time; otherwise, false
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isBusinessTime(final Instant time) {
        Require.neqNull(time, "time");
        return this.calendarDay(time).isBusinessTime(time);
    }

    /**
     * Determines if the specified time is a business time. Business times fall within business time ranges of the day's
     * business schedule.
     *
     * @return true if the specified time is a business time; otherwise, false
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isBusinessTime() {
        return isBusinessTime(DateTimeUtils.now());
    }

    /**
     * Returns the ratio of the business day length and the standard business day length. For example, a holiday has
     * zero business time and will therefore return 0.0. A normal business day will be of the standard length and will
     * therefore return 1.0. A NYSE half day holiday will return 0.538 (3.5 hours open, over a standard 6.5 hour day).
     *
     * @param date date
     * @return ratio of the business day length and the standard business day length for the date
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionStandardBusinessDay(final LocalDate date) {
        Require.neqNull(date, "date");
        final CalendarDay<Instant> schedule = this.calendarDay(date);
        return (double) schedule.businessNanos() / (double) standardBusinessNanos();
    }

    /**
     * Returns the ratio of the business day length and the standard business day length. For example, a holiday has
     * zero business time and will therefore return 0.0. A normal business day will be of the standard length and will
     * therefore return 1.0. A half day holiday will return 0.5.
     *
     * @param date date
     * @return ratio of the business day length and the standard business day length for the date
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public double fractionStandardBusinessDay(final String date) {
        Require.neqNull(date, "date");
        return fractionStandardBusinessDay(DateTimeUtils.parseLocalDate(date));
    }

    /**
     * Returns the ratio of the business day length and the standard business day length. For example, a holiday has
     * zero business time and will therefore return 0.0. A normal business day will be of the standard length and will
     * therefore return 1.0. A half day holiday will return 0.5.
     *
     * @param time time
     * @return ratio of the business day length and the standard business day length for the date
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionStandardBusinessDay(final Instant time) {
        Require.neqNull(time, "time");
        return fractionStandardBusinessDay(DateTimeUtils.toLocalDate(time, timeZone()));
    }

    /**
     * Returns the ratio of the business day length and the standard business day length. For example, a holiday has
     * zero business time and will therefore return 0.0. A normal business day will be of the standard length and will
     * therefore return 1.0. A half day holiday will return 0.5.
     *
     * @param time time
     * @return ratio of the business day length and the standard business day length for the date
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionStandardBusinessDay(final ZonedDateTime time) {
        Require.neqNull(time, "time");
        return fractionStandardBusinessDay(DateTimeUtils.toLocalDate(time.toInstant(), timeZone()));
    }

    /**
     * Returns the ratio of the business day length and the standard business day length. For example, a holiday has
     * zero business time and will therefore return 0.0. A normal business day will be of the standard length and will
     * therefore return 1.0. A half day holiday will return 0.5.
     *
     * @return ratio of the business day length and the standard business day length for the date
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionStandardBusinessDay() {
        return fractionStandardBusinessDay(calendarDate());
    }

    /**
     * Fraction of the business day complete.
     *
     * @param time time
     * @return the fraction of the business day complete, or 1.0 if the day is not a business day
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionBusinessDayComplete(final Instant time) {
        Require.neqNull(time, "time");

        final CalendarDay<Instant> schedule = this.calendarDay(time);

        if (!schedule.isBusinessDay()) {
            return 1.0;
        }

        final long businessDaySoFar = schedule.businessNanosElapsed(time);
        return (double) businessDaySoFar / (double) schedule.businessNanos();
    }

    /**
     * Fraction of the business day complete.
     *
     * @param time time
     * @return the fraction of the business day complete, or 1.0 if the day is not a business day
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionBusinessDayComplete(final ZonedDateTime time) {
        Require.neqNull(time, "time");
        return fractionBusinessDayComplete(time.toInstant());
    }

    /**
     * Fraction of the current business day complete.
     *
     * @return the fraction of the business day complete, or 1.0 if the day is not a business day
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionBusinessDayComplete() {
        return fractionBusinessDayComplete(DateTimeUtils.now());
    }

    /**
     * Fraction of the business day remaining.
     *
     * @param time time
     * @return the fraction of the business day complete, or 0.0 if the day is not a business day
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionBusinessDayRemaining(final Instant time) {
        Require.neqNull(time, "time");
        return 1.0 - fractionBusinessDayComplete(time);
    }

    /**
     * Fraction of the business day remaining.
     *
     * @param time time
     * @return the fraction of the business day complete, or 0.0 if the day is not a business day
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionBusinessDayRemaining(final ZonedDateTime time) {
        Require.neqNull(time, "time");
        return 1.0 - fractionBusinessDayComplete(time);
    }

    /**
     * Fraction of the business day remaining.
     *
     * @return the fraction of the business day complete, or 0.0 if the day is not a business day
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionBusinessDayRemaining() {
        return fractionBusinessDayRemaining(DateTimeUtils.now());
    }

    // endregion

    // region Ranges

    /**
     * Returns the number of business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of business dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberBusinessDates(final LocalDate start, final LocalDate end, final boolean startInclusive,
            final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");

        int days = 0;

        for (LocalDate day = start; !day.isAfter(end); day = day.plusDays(1)) {
            final boolean skip = (!startInclusive && day.equals(start)) || (!endInclusive && day.equals(end));

            if (!skip && isBusinessDay(day)) {
                days++;
            }
        }

        return days;
    }

    /**
     * Returns the number of business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of business dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public int numberBusinessDates(final String start, final String end, final boolean startInclusive,
            final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return numberBusinessDates(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end),
                startInclusive, endInclusive);
    }

    /**
     * Returns the number of business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of business dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberBusinessDates(final ZonedDateTime start, final ZonedDateTime end, final boolean startInclusive,
            final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return numberBusinessDates(start.withZoneSameInstant(timeZone()).toLocalDate(),
                end.withZoneSameInstant(timeZone()).toLocalDate(), startInclusive, endInclusive);
    }

    /**
     * Returns the number of business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of business dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberBusinessDates(final Instant start, final Instant end, final boolean startInclusive,
            final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return numberBusinessDates(DateTimeUtils.toLocalDate(start, timeZone()),
                DateTimeUtils.toLocalDate(end, timeZone()), startInclusive, endInclusive);
    }

    /**
     * Returns the number of business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return number of business dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberBusinessDates(final LocalDate start, final LocalDate end) {
        return numberBusinessDates(start, end, true, true);
    }

    /**
     * Returns the number of business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return number of business dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public int numberBusinessDates(final String start, final String end) {
        return numberBusinessDates(start, end, true, true);
    }

    /**
     * Returns the number of business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return number of business dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberBusinessDates(final ZonedDateTime start, final ZonedDateTime end) {
        return numberBusinessDates(start, end, true, true);
    }

    /**
     * Returns the number of business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return number of business dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberBusinessDates(final Instant start, final Instant end) {
        return numberBusinessDates(start, end, true, true);
    }

    /**
     * Returns the number of non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of non-business dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberNonBusinessDates(final LocalDate start, final LocalDate end, final boolean startInclusive,
            final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");

        return numberCalendarDates(start, end, startInclusive, endInclusive)
                - numberBusinessDates(start, end, startInclusive, endInclusive);
    }

    /**
     * Returns the number of non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of non-business dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public int numberNonBusinessDates(final String start, final String end, final boolean startInclusive,
            final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return numberNonBusinessDates(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end),
                startInclusive, endInclusive);
    }

    /**
     * Returns the number of non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of non-business dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberNonBusinessDates(final ZonedDateTime start, final ZonedDateTime end, final boolean startInclusive,
            final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return numberNonBusinessDates(start.withZoneSameInstant(timeZone()).toLocalDate(),
                end.withZoneSameInstant(timeZone()).toLocalDate(), startInclusive, endInclusive);
    }

    /**
     * Returns the number of non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of non-business dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberNonBusinessDates(final Instant start, final Instant end, final boolean startInclusive,
            final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return numberNonBusinessDates(DateTimeUtils.toLocalDate(start, timeZone()),
                DateTimeUtils.toLocalDate(end, timeZone()), startInclusive, endInclusive);
    }

    /**
     * Returns the number of non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return number of non-business dates between {@code start} and {@code end}; including {@code start} and
     *         {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberNonBusinessDates(final LocalDate start, final LocalDate end) {
        return numberNonBusinessDates(start, end, true, true);
    }

    /**
     * Returns the number of non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return number of non-business dates between {@code start} and {@code end}; including {@code start} and
     *         {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public int numberNonBusinessDates(final String start, final String end) {
        return numberNonBusinessDates(start, end, true, true);
    }

    /**
     * Returns the number of non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return number of non-business dates between {@code start} and {@code end}; including {@code start} and
     *         {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberNonBusinessDates(final ZonedDateTime start, final ZonedDateTime end) {
        return numberNonBusinessDates(start, end, true, true);
    }

    /**
     * Returns the number of non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return number of non-business dates between {@code start} and {@code end}; including {@code start} and
     *         {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberNonBusinessDates(final Instant start, final Instant end) {
        return numberNonBusinessDates(start, end, true, true);
    }

    /**
     * Returns the business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return business dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] businessDates(final LocalDate start, final LocalDate end, final boolean startInclusive,
            final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");

        List<LocalDate> dateList = new ArrayList<>();

        for (LocalDate day = start; !day.isAfter(end); day = day.plusDays(1)) {
            final boolean skip = (!startInclusive && day.equals(start)) || (!endInclusive && day.equals(end));

            if (!skip && isBusinessDay(day)) {
                dateList.add(day);
            }
        }

        return dateList.toArray(new LocalDate[0]);
    }

    /**
     * Returns the business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return business dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate[] businessDates(final String start, final String end, final boolean startInclusive,
            final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return businessDates(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end), startInclusive,
                endInclusive);
    }

    /**
     * Returns the business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return business dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] businessDates(final ZonedDateTime start, final ZonedDateTime end, final boolean startInclusive,
            final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return businessDates(start.withZoneSameInstant(timeZone()).toLocalDate(),
                end.withZoneSameInstant(timeZone()).toLocalDate(), startInclusive, endInclusive);
    }

    /**
     * Returns the business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return business dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] businessDates(final Instant start, final Instant end, final boolean startInclusive,
            final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return businessDates(DateTimeUtils.toLocalDate(start, timeZone()), DateTimeUtils.toLocalDate(end, timeZone()),
                startInclusive, endInclusive);
    }

    /**
     * Returns the business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return business dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] businessDates(final LocalDate start, final LocalDate end) {
        return businessDates(start, end, true, true);
    }

    /**
     * Returns the business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return business dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate[] businessDates(final String start, final String end) {
        return businessDates(start, end, true, true);
    }

    /**
     * Returns the business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return business dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] businessDates(final ZonedDateTime start, final ZonedDateTime end) {
        return businessDates(start, end, true, true);
    }

    /**
     * Returns the business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return business dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] businessDates(final Instant start, final Instant end) {
        return businessDates(start, end, true, true);
    }

    /**
     * Returns the non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return non-business dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] nonBusinessDates(final LocalDate start, final LocalDate end, final boolean startInclusive,
            final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");

        List<LocalDate> dateList = new ArrayList<>();

        for (LocalDate day = start; !day.isAfter(end); day = day.plusDays(1)) {
            final boolean skip = (!startInclusive && day.equals(start)) || (!endInclusive && day.equals(end));

            if (!skip && !isBusinessDay(day)) {
                dateList.add(day);
            }
        }

        return dateList.toArray(new LocalDate[0]);
    }

    /**
     * Returns the non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return non-business dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate[] nonBusinessDates(final String start, final String end, final boolean startInclusive,
            final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return nonBusinessDates(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end), startInclusive,
                endInclusive);
    }

    /**
     * Returns the non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return non-business dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] nonBusinessDates(final ZonedDateTime start, final ZonedDateTime end,
            final boolean startInclusive, final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return nonBusinessDates(start.withZoneSameInstant(timeZone()).toLocalDate(),
                end.withZoneSameInstant(timeZone()).toLocalDate(), startInclusive, endInclusive);
    }

    /**
     * Returns the non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return non-business dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] nonBusinessDates(final Instant start, final Instant end, final boolean startInclusive,
            final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return nonBusinessDates(DateTimeUtils.toLocalDate(start, timeZone()),
                DateTimeUtils.toLocalDate(end, timeZone()), startInclusive, endInclusive);
    }

    /**
     * Returns the non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return non-business dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] nonBusinessDates(final LocalDate start, final LocalDate end) {
        return nonBusinessDates(start, end, true, true);
    }

    /**
     * Returns the non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return non-business dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate[] nonBusinessDates(final String start, final String end) {
        return nonBusinessDates(start, end, true, true);
    }

    /**
     * Returns the non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return non-business dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] nonBusinessDates(final ZonedDateTime start, final ZonedDateTime end) {
        return nonBusinessDates(start, end, true, true);
    }

    /**
     * Returns the non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return non-business dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] nonBusinessDates(final Instant start, final Instant end) {
        return nonBusinessDates(start, end, true, true);
    }

    // endregion

    // region Differences

    /**
     * Returns the amount of business time in nanoseconds between two times.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return the amount of business time in nanoseconds between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public long diffBusinessNanos(final Instant start, final Instant end) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");

        if (DateTimeUtils.isAfter(start, end)) {
            return -diffBusinessNanos(end, start);
        }

        final LocalDate startDate = DateTimeUtils.toLocalDate(start, timeZone());
        final LocalDate endDate = DateTimeUtils.toLocalDate(end, timeZone());

        assert startDate != null;
        assert endDate != null;

        if (startDate.equals(endDate)) {
            final CalendarDay<Instant> schedule = this.calendarDay(startDate);
            return schedule.businessNanosElapsed(end) - schedule.businessNanosElapsed(start);
        }

        long rst = this.calendarDay(startDate).businessNanosRemaining(start)
                + this.calendarDay(endDate).businessNanosElapsed(end);

        for (LocalDate d = startDate.plusDays(1); d.isBefore(endDate); d = d.plusDays(1)) {
            rst += this.calendarDay(d).businessNanos();
        }

        return rst;
    }

    /**
     * Returns the amount of business time in nanoseconds between two times.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return the amount of business time in nanoseconds between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public long diffBusinessNanos(final ZonedDateTime start, final ZonedDateTime end) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return diffBusinessNanos(start.toInstant(), end.toInstant());
    }

    /**
     * Returns the amount of non-business time in nanoseconds between two times.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return the amount of nonbusiness time in nanoseconds between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public long diffNonBusinessNanos(final Instant start, final Instant end) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return DateTimeUtils.diffNanos(start, end) - diffBusinessNanos(start, end);
    }

    /**
     * Returns the amount of non-business time in nanoseconds between two times.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return the amount of non-business time in nanoseconds between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public long diffNonBusinessNanos(final ZonedDateTime start, final ZonedDateTime end) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return diffNonBusinessNanos(start.toInstant(), end.toInstant());
    }

    /**
     * Returns the amount of business time in standard business days between two times.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return the amount of business time in standard business days between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public double diffBusinessDays(final Instant start, final Instant end) {
        return (double) diffBusinessNanos(start, end) / (double) standardBusinessNanos();
    }

    /**
     * Returns the amount of business time in standard business days between two times.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return the amount of business time in standard business days between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public double diffBusinessDays(final ZonedDateTime start, final ZonedDateTime end) {
        return (double) diffBusinessNanos(start, end) / (double) standardBusinessNanos();
    }

    /**
     * Returns the number of business years between {@code start} and {@code end}.
     *
     * @param start start; if null, return null
     * @param end end; if null, return null
     * @return the amount of business time in business years between the {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public double diffBusinessYears(final Instant start, final Instant end) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");

        final int yearStart = DateTimeUtils.year(start, timeZone());
        final int yearEnd = DateTimeUtils.year(end, timeZone());

        if (yearStart == yearEnd) {
            return (double) diffBusinessNanos(start, end) / (double) getYearData(yearStart).businessTimeNanos;
        }

        final YearData yearDataStart = getYearData(yearStart);
        final YearData yearDataEnd = getYearData(yearEnd);

        return (double) diffBusinessNanos(start, yearDataStart.end) / (double) yearDataStart.businessTimeNanos +
                (double) diffBusinessNanos(yearDataEnd.start, end) / (double) yearDataEnd.businessTimeNanos +
                yearEnd - yearStart - 1;
    }

    /**
     * Returns the number of business years between {@code start} and {@code end}.
     *
     * @param start start; if null, return null
     * @param end end; if null, return null
     * @return the amount of business time in business years between the {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public double diffBusinessYears(final ZonedDateTime start, final ZonedDateTime end) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return diffBusinessYears(start.toInstant(), end.toInstant());
    }

    // endregion

    // region Arithmetic

    /**
     * Adds a specified number of business days to an input date. Adding negative days is equivalent to subtracting
     * days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} business days after {@code date}; null if {@code date} is not a business day and
     *         {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate plusBusinessDays(final LocalDate date, final int days) {
        Require.neqNull(date, "date");

        if (days == 0) {
            return isBusinessDay(date) ? date : null;
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
     * Adds a specified number of business days to an input date. Adding negative days is equivalent to subtracting
     * days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} business days after {@code date}; null if {@code date} is not a business day and
     *         {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate plusBusinessDays(final String date, final int days) {
        Require.neqNull(date, "date");
        return plusBusinessDays(DateTimeUtils.parseLocalDate(date), days);
    }

    /**
     * Adds a specified number of business days to an input time. Adding negative days is equivalent to subtracting
     * days.
     *
     * Day additions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} business days after {@code time}; null if {@code time} is not a business day and
     *         {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public Instant plusBusinessDays(final Instant time, final int days) {
        Require.neqNull(time, "time");
        final ZonedDateTime zdt = plusBusinessDays(DateTimeUtils.toZonedDateTime(time, timeZone()), days);
        return zdt == null ? null : zdt.toInstant();
    }

    /**
     * Adds a specified number of business days to an input time. Adding negative days is equivalent to subtracting
     * days.
     *
     * Day additions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     *
     * The resultant time will have the same time zone as the calendar. This could be different than the time zone of
     * the input {@link ZonedDateTime}.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} business days after {@code time}; null if {@code time} is not a business day and
     *         {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public ZonedDateTime plusBusinessDays(final ZonedDateTime time, final int days) {
        Require.neqNull(time, "time");
        final ZonedDateTime zdt = time.withZoneSameInstant(timeZone());
        final LocalDate pbd = plusBusinessDays(zdt.toLocalDate(), days);
        return pbd == null ? null
                : pbd
                        .atTime(zdt.toLocalTime())
                        .atZone(timeZone());
    }

    /**
     * Subtracts a specified number of business days from an input date. Subtracting negative days is equivalent to
     * adding days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} business days before {@code date}; null if {@code date} is not a business day and
     *         {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate minusBusinessDays(final LocalDate date, final int days) {
        return plusBusinessDays(date, -days);
    }

    /**
     * Subtracts a specified number of business days from an input date. Subtracting negative days is equivalent to
     * adding days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} business days before {@code date}; null if {@code date} is not a business day and
     *         {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate minusBusinessDays(final String date, final int days) {
        return plusBusinessDays(date, -days);
    }

    /**
     * Subtracts a specified number of business days from an input time. Subtracting negative days is equivalent to
     * adding days.
     *
     * Day subtractions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} business days before {@code time}; null if {@code time} is not a business day and
     *         {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public Instant minusBusinessDays(final Instant time, final int days) {
        return plusBusinessDays(time, -days);
    }

    /**
     * Subtracts a specified number of business days from an input time. Subtracting negative days is equivalent to
     * adding days.
     *
     * Day subtraction are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     *
     * The resultant time will have the same time zone as the calendar. This could be different than the time zone of
     * the input {@link ZonedDateTime}.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} business days before {@code time}; null if {@code time} is not a business day and
     *         {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public ZonedDateTime minusBusinessDays(final ZonedDateTime time, final int days) {
        return plusBusinessDays(time, -days);
    }

    /**
     * Adds a specified number of non-business days to an input date. Adding negative days is equivalent to subtracting
     * days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} non-business days after {@code date}; null if {@code date} is not a business day and
     *         {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate plusNonBusinessDays(final LocalDate date, final int days) {
        Require.neqNull(date, "date");

        if (days == 0) {
            return isBusinessDay(date) ? null : date;
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
     * Adds a specified number of non-business days to an input date. Adding negative days is equivalent to subtracting
     * days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} non-business days after {@code date}; null if {@code date} is not a business day and
     *         {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate plusNonBusinessDays(final String date, final int days) {
        Require.neqNull(date, "date");
        return this.plusNonBusinessDays(DateTimeUtils.parseLocalDate(date), days);
    }

    /**
     * Adds a specified number of non-business days to an input time. Adding negative days is equivalent to subtracting
     * days.
     *
     * Day additions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     *
     * The resultant time will have the same time zone as the calendar. This could be different than the time zone of
     * the input {@link ZonedDateTime}.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} non-business days after {@code time}; null if {@code time} is not a business day and
     *         {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public Instant plusNonBusinessDays(final Instant time, final int days) {
        Require.neqNull(time, "time");
        final ZonedDateTime zdt = plusNonBusinessDays(DateTimeUtils.toZonedDateTime(time, timeZone()), days);
        return zdt == null ? null : zdt.toInstant();

    }

    /**
     * Adds a specified number of non-business days to an input time. Adding negative days is equivalent to subtracting
     * days.
     *
     * Day additions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     *
     * The resultant time will have the same time zone as the calendar. This could be different than the time zone of
     * the input {@link ZonedDateTime}.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} non-business days after {@code time}; null if {@code time} is not a business day and
     *         {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public ZonedDateTime plusNonBusinessDays(final ZonedDateTime time, final int days) {
        Require.neqNull(time, "time");
        final ZonedDateTime zdt = time.withZoneSameInstant(timeZone());
        final LocalDate pbd = plusNonBusinessDays(zdt.toLocalDate(), days);
        return pbd == null ? null
                : pbd
                        .atTime(zdt.toLocalTime())
                        .atZone(timeZone());
    }

    /**
     * Subtracts a specified number of non-business days to an input date. Subtracting negative days is equivalent to
     * adding days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} non-business days before {@code date}; null if {@code date} is a business day and
     *         {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate minusNonBusinessDays(final LocalDate date, final int days) {
        return this.plusNonBusinessDays(date, -days);
    }

    /**
     * Subtracts a specified number of non-business days to an input date. Subtracting negative days is equivalent to
     * adding days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} non-business days before {@code date}; null if {@code date} is a business day and
     *         {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate minusNonBusinessDays(final String date, final int days) {
        return plusNonBusinessDays(date, -days);
    }

    /**
     * Subtracts a specified number of non-business days to an input time. Subtracting negative days is equivalent to
     * adding days.
     *
     * Day subtractions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} non-business days before {@code time}; null if {@code time} is a business day and
     *         {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public Instant minusNonBusinessDays(final Instant time, final int days) {
        return plusNonBusinessDays(time, -days);
    }

    /**
     * Subtracts a specified number of non-business days to an input time. Subtracting negative days is equivalent to
     * adding days.
     *
     * Day subtractions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     *
     * The resultant time will have the same time zone as the calendar. This could be different than the time zone of
     * the input {@link ZonedDateTime}.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} non-business days before {@code time}; null if {@code time} is a business day and
     *         {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public ZonedDateTime minusNonBusinessDays(final ZonedDateTime time, final int days) {
        return plusNonBusinessDays(time, -days);
    }

    /**
     * Adds a specified number of business days to the current date. Adding negative days is equivalent to subtracting
     * days.
     *
     * @param days number of days to add.
     * @return {@code days} business days after the current date; null if the current date is not a business day and
     *         {@code days} is zero
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate futureBusinessDate(final int days) {
        return plusBusinessDays(calendarDate(), days);
    }

    /**
     * Subtracts a specified number of business days from the current date. Subtracting negative days is equivalent to
     * adding days.
     *
     * @param days number of days to subtract.
     * @return {@code days} business days before the current date; null if the current date is not a business day and
     *         {@code days} is zero
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate pastBusinessDate(final int days) {
        return minusBusinessDays(calendarDate(), days);
    }

    /**
     * Adds a specified number of non-business days to the current date. Adding negative days is equivalent to
     * subtracting days.
     *
     * @param days number of days to add.
     * @return {@code days} non-business days after the current date; null if the current date is a business day and
     *         {@code days} is zero
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate futureNonBusinessDate(final int days) {
        return this.plusNonBusinessDays(calendarDate(), days);
    }

    /**
     * Subtracts a specified number of non-business days to the current date. Subtracting negative days is equivalent to
     * adding days.
     *
     * @param days number of days to subtract.
     * @return {@code days} non-business days before the current date; null if the current date is a business day and
     *         {@code days} is zero
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate pastNonBusinessDate(final int days) {
        return minusNonBusinessDays(calendarDate(), days);
    }

    // endregion

}
