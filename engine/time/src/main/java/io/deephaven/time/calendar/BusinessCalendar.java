//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.base.verify.Require;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;

import java.time.*;
import java.util.*;

import static io.deephaven.util.QueryConstants.*;

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

    private static class SummaryData extends ReadOptimizedConcurrentCache.IntKeyedValue {
        private final Instant startInstant;
        private final LocalDate startDate;
        private final Instant endInstant;
        private final LocalDate endDate; // exclusive
        private final long businessTimeNanos;
        private final int businessDays;
        private final int nonBusinessDays;
        private final ArrayList<LocalDate> businessDates;
        private final ArrayList<LocalDate> nonBusinessDates;

        public SummaryData(
                final int key,
                final Instant startInstant,
                final LocalDate startDate,
                final Instant endInstant,
                final LocalDate endDate,
                final long businessTimeNanos,
                final int businessDays,
                final int nonBusinessDays,
                final ArrayList<LocalDate> businessDates,
                final ArrayList<LocalDate> nonBusinessDates) {
            super(key);
            this.startInstant = startInstant;
            this.startDate = startDate;
            this.endInstant = endInstant;
            this.endDate = endDate;
            this.businessTimeNanos = businessTimeNanos;
            this.businessDays = businessDays;
            this.nonBusinessDays = nonBusinessDays;
            this.businessDates = businessDates;
            this.nonBusinessDates = nonBusinessDates;
        }
    }

    private final ReadOptimizedConcurrentCache<ReadOptimizedConcurrentCache.Pair<CalendarDay<Instant>>> schedulesCache =
            new ReadOptimizedConcurrentCache<>(10000, this::computeCalendarDay);
    private final YearMonthSummaryCache<SummaryData> summaryCache =
            new YearMonthSummaryCache<>(this::computeMonthSummary, this::computeYearSummary);
    private final int yearCacheStart;
    private final int yearCacheEnd;

    @Override
    synchronized void clearCache() {
        super.clearCache();
        schedulesCache.clear();
        summaryCache.clear();
    }

    private SummaryData summarize(final int key, final LocalDate startDate, final LocalDate endDate) {
        final ZonedDateTime start = startDate.atTime(0, 0).atZone(timeZone());
        final ZonedDateTime end = endDate.atTime(0, 0).atZone(timeZone());

        LocalDate date = startDate;
        long businessTimeNanos = 0;
        int businessDays = 0;
        int nonBusinessDays = 0;
        final ArrayList<LocalDate> businessDates = new ArrayList<>();
        final ArrayList<LocalDate> nonBusinessDates = new ArrayList<>();

        while (date.isBefore(endDate)) {
            final CalendarDay<Instant> bs = calendarDay(date);

            if (bs.isBusinessDay()) {
                ++businessDays;
                businessDates.add(date);
            } else {
                ++nonBusinessDays;
                nonBusinessDates.add(date);
            }

            businessTimeNanos += bs.businessNanos();
            date = date.plusDays(1);
        }

        return new SummaryData(
                key,
                start.toInstant(),
                start.toLocalDate(),
                end.toInstant(),
                end.toLocalDate(),
                businessTimeNanos,
                businessDays,
                nonBusinessDays,
                businessDates,
                nonBusinessDates);
    }

    private SummaryData computeMonthSummary(final int key) {
        final int year = YearMonthSummaryCache.yearFromYearMonthKey(key);
        final int month = YearMonthSummaryCache.monthFromYearMonthKey(key);
        final LocalDate startDate = LocalDate.of(year, month, 1);
        final LocalDate endDate = startDate.plusMonths(1); // exclusive
        return summarize(key, startDate, endDate);
    }

    private SummaryData computeYearSummary(final int year) {
        Instant startInstant = null;
        LocalDate startDate = null;
        Instant endInstant = null;
        LocalDate endDate = null;
        long businessTimeNanos = 0;
        int businessDays = 0;
        int nonBusinessDays = 0;
        ArrayList<LocalDate> businessDates = new ArrayList<>();
        ArrayList<LocalDate> nonBusinessDates = new ArrayList<>();

        for (int month = 1; month <= 12; month++) {
            SummaryData ms = summaryCache.getMonthSummary(year, month);
            if (month == 1) {
                startInstant = ms.startInstant;
                startDate = ms.startDate;
            }

            if (month == 12) {
                endInstant = ms.endInstant;
                endDate = ms.endDate;
            }

            businessTimeNanos += ms.businessTimeNanos;
            businessDays += ms.businessDays;
            nonBusinessDays += ms.nonBusinessDays;
            businessDates.addAll(ms.businessDates);
            nonBusinessDates.addAll(ms.nonBusinessDates);
        }

        return new SummaryData(
                year,
                startInstant,
                startDate,
                endInstant,
                endDate,
                businessTimeNanos,
                businessDays,
                nonBusinessDays,
                businessDates,
                nonBusinessDates);
    }

    private SummaryData getYearSummary(final int year) {

        if (year < yearCacheStart || year > yearCacheEnd) {
            throw new InvalidDateException("Business calendar does not contain a complete year for: year=" + year);
        }

        return summaryCache.getYearSummary(year);
    }

    /**
     * Creates a key for the schedules cache from a date.
     *
     * @param date date
     * @return key
     */
    static int schedulesCacheKeyFromDate(final LocalDate date) {
        return date.getYear() * 10000 + date.getMonthValue() * 100 + date.getDayOfMonth();
    }

    /**
     * Creates a date from a schedules cache key.
     *
     * @param key key
     * @return date
     */
    static LocalDate schedulesCacheDateFromKey(final int key) {
        return LocalDate.of(key / 10000, (key % 10000) / 100, key % 100);
    }

    private ReadOptimizedConcurrentCache.Pair<CalendarDay<Instant>> computeCalendarDay(final int key) {
        final LocalDate date = schedulesCacheDateFromKey(key);
        final CalendarDay<Instant> h = holidays.get(date);
        final CalendarDay<Instant> v;

        if (h != null) {
            v = h;
        } else if (weekendDays.contains(date.getDayOfWeek())) {
            v = CalendarDay.toInstant(CalendarDay.HOLIDAY, date, timeZone());
        } else {
            v = CalendarDay.toInstant(standardBusinessDay, date, timeZone());
        }

        return new ReadOptimizedConcurrentCache.Pair<>(key, v);
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
     * @param holidays holidays. Business day schedules for all holidays. A holiday is a date that has a schedule that
     *        is different from the schedule for a standard business day or weekend.
     * @throws RequirementFailure if any argument is null.
     */
    public BusinessCalendar(final String name, final String description, final ZoneId timeZone,
            final LocalDate firstValidDate, final LocalDate lastValidDate,
            final CalendarDay<LocalTime> standardBusinessDay, final Set<DayOfWeek> weekendDays,
            final Map<LocalDate, CalendarDay<Instant>> holidays) {
        super(name, description, timeZone);
        this.firstValidDate = Require.neqNull(firstValidDate, "firstValidDate");
        this.lastValidDate = Require.neqNull(lastValidDate, "lastValidDate");
        this.standardBusinessDay = Require.neqNull(standardBusinessDay, "standardBusinessDay");
        this.weekendDays = Set.copyOf(Require.neqNull(weekendDays, "weekendDays"));
        this.holidays = Map.copyOf(Require.neqNull(holidays, "holidays"));

        // Only cache complete years, since incomplete years can not be fully computed.
        yearCacheStart =
                firstValidDate.getDayOfYear() == 1 ? firstValidDate.getYear() : firstValidDate.getYear() + 1;
        yearCacheEnd = ((lastValidDate.isLeapYear() && lastValidDate.getDayOfYear() == 366)
                || lastValidDate.getDayOfYear() == 365) ? lastValidDate.getYear() : lastValidDate.getYear() - 1;
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
     * Length of a standard business day.
     *
     * @return length of a standard business day
     */
    public Duration standardBusinessDuration() {
        return standardBusinessDay.businessDuration();
    }

    /**
     * Business day schedules for all holidays. A holiday is a date that has a schedule that is different from the
     * schedule for a standard business day or weekend.
     *
     * @return a map of holiday dates and their calendar days
     */
    public Map<LocalDate, CalendarDay<Instant>> holidays() {
        return holidays;
    }

    /**
     * Returns the {@link CalendarDay} for a date.
     *
     * @param date date
     * @return the corresponding {@link CalendarDay} of {@code date}. {@code null} if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public CalendarDay<Instant> calendarDay(final LocalDate date) {
        if (date == null) {
            return null;
        }

        if (date.isBefore(firstValidDate)) {
            throw new InvalidDateException("Date is before the first valid business calendar date:  date=" + date
                    + " firstValidDate=" + firstValidDate);
        } else if (date.isAfter(lastValidDate)) {
            throw new InvalidDateException("Date is after the last valid business calendar date:  date=" + date
                    + " lastValidDate=" + lastValidDate);
        }

        final int key = schedulesCacheKeyFromDate(date);
        return schedulesCache.computeIfAbsent(key).getValue();
    }

    /**
     * Returns the {@link CalendarDay} for a date.
     *
     * @param time time
     * @return the corresponding {@link CalendarDay} of {@code date}. {@code null} if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public CalendarDay<Instant> calendarDay(final ZonedDateTime time) {
        if (time == null) {
            return null;
        }

        return calendarDay(time.withZoneSameInstant(timeZone()).toLocalDate());
    }

    /**
     * Returns the {@link CalendarDay} for a date.
     *
     * @param time time
     * @return the corresponding {@link CalendarDay} of {@code date}. {@code null} if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public CalendarDay<Instant> calendarDay(final Instant time) {
        if (time == null) {
            return null;
        }

        return calendarDay(time.atZone(timeZone()));
    }

    /**
     * Returns the {@link CalendarDay} for a date.
     *
     * @param date date
     * @return the corresponding {@link CalendarDay} of {@code date}. {@code null} if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public CalendarDay<Instant> calendarDay(final String date) {
        if (date == null) {
            return null;
        }

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
     * @return true if the date is a business day; false otherwise. False if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isBusinessDay(final LocalDate date) {
        if (date == null) {
            return false;
        }

        return this.calendarDay(date).isBusinessDay();
    }

    /**
     * Is the date a business day?
     *
     * @param date date
     * @return true if the date is a business day; false otherwise. False if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public boolean isBusinessDay(final String date) {
        if (date == null) {
            return false;
        }

        return calendarDay(date).isBusinessDay();
    }

    /**
     * Is the time on a business day?
     * <p>
     * As long as the time occurs on a business day, it is considered a business day. The time does not have to be
     * within the business day schedule. To determine if a time is within the business day schedule, use
     * {@link #isBusinessTime(ZonedDateTime)}.
     *
     * @param time time
     * @return true if the date is a business day; false otherwise. False if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isBusinessDay(final ZonedDateTime time) {
        if (time == null) {
            return false;
        }

        return this.calendarDay(time).isBusinessDay();
    }

    /**
     * Is the time on a business day?
     * <p>
     * As long as the time occurs on a business day, it is considered a business day. The time does not have to be
     * within the business day schedule. To determine if a time is within the business day schedule, use
     * {@link #isBusinessTime(Instant)}.
     *
     * @param time time
     * @return true if the date is a business day; false otherwise. False if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isBusinessDay(final Instant time) {
        if (time == null) {
            return false;
        }

        return this.calendarDay(time).isBusinessDay();
    }

    /**
     * Is the day of the week a normal business day?
     *
     * @param day a day of the week
     * @return true if the day is a business day; false otherwise. False if the input is {@code null}.
     */
    public boolean isBusinessDay(final DayOfWeek day) {
        if (day == null) {
            return false;
        }

        return !weekendDays.contains(day);
    }

    /**
     * Is the current day a business day? As long as the current time occurs on a business day, it is considered a
     * business day. The time does not have to be within the business day schedule.
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
     * @return true if {@code date} is the last business day of the month; false otherwise. False if the input is
     *         {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    boolean isLastBusinessDayOfMonth(final LocalDate date) {
        if (date == null || !isBusinessDay(date)) {
            return false;
        }

        final LocalDate nextBusAfterDate = plusBusinessDays(date, 1);
        assert nextBusAfterDate != null;
        return date.getMonth() != nextBusAfterDate.getMonth();
    }

    /**
     * Is the time on the last business day of the month?
     * <p>
     * As long as the time occurs on a business day, it is considered a business day. The time does not have to be
     * within the business day schedule.
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the month; false otherwise. False if the input is
     *         {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isLastBusinessDayOfMonth(final ZonedDateTime time) {
        if (time == null) {
            return false;
        }

        Require.neqNull(time, "time");
        return isLastBusinessDayOfMonth(DateTimeUtils.toLocalDate(time.withZoneSameInstant(timeZone())));
    }

    /**
     * Is the time on the last business day of the month?
     * <p>
     * As long as the time occurs on a business day, it is considered a business day. The time does not have to be
     * within the business day schedule.
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the month; false otherwise. False if the input is
     *         {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isLastBusinessDayOfMonth(final Instant time) {
        if (time == null) {
            return false;
        }

        return isLastBusinessDayOfMonth(DateTimeUtils.toLocalDate(time, timeZone()));
    }

    /**
     * Is the date the last business day of the month?
     *
     * @param date date
     * @return true if {@code time} is on the last business day of the month; false otherwise. False if the input is
     *         {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public boolean isLastBusinessDayOfMonth(final String date) {
        if (date == null) {
            return false;
        }

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
     * @return true if {@code date} is on the last business day of the week; false otherwise. False if the input is
     *         {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isLastBusinessDayOfWeek(final LocalDate date) {
        if (date == null || !isBusinessDay(date)) {
            return false;
        }

        final LocalDate nextBusinessDay = plusBusinessDays(date, 1);
        return date.getDayOfWeek().compareTo(nextBusinessDay.getDayOfWeek()) > 0
                || numberCalendarDates(date, nextBusinessDay) > 6;
    }

    /**
     * Is the time on the last business day of the week?
     * <p>
     * As long as the time occurs on a business day, it is considered a business day. The time does not have to be
     * within the business day schedule.
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the week; false otherwise. False if the input is
     *         {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isLastBusinessDayOfWeek(final ZonedDateTime time) {
        if (time == null) {
            return false;
        }

        return isLastBusinessDayOfWeek(time.withZoneSameInstant(timeZone()).toLocalDate());
    }

    /**
     * Is the time on the last business day of the week?
     * <p>
     * As long as the time occurs on a business day, it is considered a business day. The time does not have to be
     * within the business day schedule.
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the week; false otherwise. False if the input is
     *         {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isLastBusinessDayOfWeek(final Instant time) {
        if (time == null) {
            return false;
        }

        return isLastBusinessDayOfWeek(DateTimeUtils.toLocalDate(time, timeZone()));
    }

    /**
     * Is the date is last business day of the week?
     *
     * @param date date
     * @return true if {@code date} is the last business day of the week; false otherwise. False if the input is
     *         {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public boolean isLastBusinessDayOfWeek(final String date) {
        if (date == null) {
            return false;
        }

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
     * @return true if {@code date} is the last business day of the year; false otherwise. False if the input is
     *         {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    boolean isLastBusinessDayOfYear(final LocalDate date) {
        if (date == null || !isBusinessDay(date)) {
            return false;
        }

        final LocalDate nextBusAfterDate = plusBusinessDays(date, 1);
        assert nextBusAfterDate != null;
        return date.getYear() != nextBusAfterDate.getYear();
    }

    /**
     * Is the time on the last business day of the year?
     * <p>
     * As long as the time occurs on a business day, it is considered a business day. The time does not have to be
     * within the business day schedule.
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the year; false otherwise. False if the input is
     *         {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isLastBusinessDayOfYear(final ZonedDateTime time) {
        if (time == null) {
            return false;
        }

        return isLastBusinessDayOfYear(DateTimeUtils.toLocalDate(time.withZoneSameInstant(timeZone())));
    }

    /**
     * Is the time on the last business day of the year?
     * <p>
     * As long as the time occurs on a business day, it is considered a business day. The time does not have to be
     * within the business day schedule.
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the year; false otherwise. False if the input is
     *         {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isLastBusinessDayOfYear(final Instant time) {
        if (time == null) {
            return false;
        }

        return isLastBusinessDayOfYear(DateTimeUtils.toLocalDate(time, timeZone()));
    }

    /**
     * Is the date the last business day of the year?
     *
     * @param date date
     * @return true if {@code time} is on the last business day of the year; false otherwise. False if the input is
     *         {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    boolean isLastBusinessDayOfYear(final String date) {
        if (date == null) {
            return false;
        }

        Require.neqNull(date, "date");
        return isLastBusinessDayOfYear(DateTimeUtils.parseLocalDate(date));
    }

    /**
     * Is the current date the last business day of the year?
     * <p>
     * As long as the current time occurs on a business day, it is considered a business day. The time does not have to
     * be within the business day schedule.
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
     * @return true if the specified time is a business time; otherwise, false. False if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isBusinessTime(final ZonedDateTime time) {
        if (time == null) {
            return false;
        }

        return this.calendarDay(time).isBusinessTime(time.toInstant());
    }

    /**
     * Determines if the specified time is a business time. Business times fall within business time ranges of the day's
     * business schedule.
     *
     * @param time time
     * @return true if the specified time is a business time; otherwise, false. False if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isBusinessTime(final Instant time) {
        if (time == null) {
            return false;
        }

        return this.calendarDay(time).isBusinessTime(time);
    }

    /**
     * Determines if the current time according to the Deephaven system clock is a business time. Business times fall
     * within business time ranges of the day's business schedule.
     *
     * @return true if the specified time is a business time; otherwise, false.
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
     * @return ratio of the business day length and the standard business day length for the date.
     *         {@link io.deephaven.util.QueryConstants#NULL_DOUBLE} if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionStandardBusinessDay(final LocalDate date) {
        if (date == null) {
            return NULL_DOUBLE;
        }

        final CalendarDay<Instant> schedule = this.calendarDay(date);
        return (double) schedule.businessNanos() / (double) standardBusinessNanos();
    }

    /**
     * Returns the ratio of the business day length and the standard business day length. For example, a holiday has
     * zero business time and will therefore return 0.0. A normal business day will be of the standard length and will
     * therefore return 1.0. A half day holiday will return 0.5.
     *
     * @param date date
     * @return ratio of the business day length and the standard business day length for the date.
     *         {@link io.deephaven.util.QueryConstants#NULL_DOUBLE} if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public double fractionStandardBusinessDay(final String date) {
        if (date == null) {
            return NULL_DOUBLE;
        }

        return fractionStandardBusinessDay(DateTimeUtils.parseLocalDate(date));
    }

    /**
     * Returns the ratio of the business day length and the standard business day length. For example, a holiday has
     * zero business time and will therefore return 0.0. A normal business day will be of the standard length and will
     * therefore return 1.0. A half day holiday will return 0.5.
     *
     * @param time time
     * @return ratio of the business day length and the standard business day length for the date.
     *         {@link io.deephaven.util.QueryConstants#NULL_DOUBLE} if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionStandardBusinessDay(final Instant time) {
        if (time == null) {
            return NULL_DOUBLE;
        }

        return fractionStandardBusinessDay(DateTimeUtils.toLocalDate(time, timeZone()));
    }

    /**
     * Returns the ratio of the business day length and the standard business day length. For example, a holiday has
     * zero business time and will therefore return 0.0. A normal business day will be of the standard length and will
     * therefore return 1.0. A half day holiday will return 0.5.
     *
     * @param time time
     * @return ratio of the business day length and the standard business day length for the date.
     *         {@link io.deephaven.util.QueryConstants#NULL_DOUBLE} if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionStandardBusinessDay(final ZonedDateTime time) {
        if (time == null) {
            return NULL_DOUBLE;
        }

        return fractionStandardBusinessDay(DateTimeUtils.toLocalDate(time.toInstant(), timeZone()));
    }

    /**
     * Returns the ratio of the business day length and the standard business day length. For example, a holiday has
     * zero business time and will therefore return 0.0. A normal business day will be of the standard length and will
     * therefore return 1.0. A half day holiday will return 0.5.
     *
     * @return ratio of the business day length and the standard business day length for the date
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionStandardBusinessDay() {
        return fractionStandardBusinessDay(calendarDate());
    }

    /**
     * Fraction of the business day complete.
     *
     * @param time time
     * @return the fraction of the business day complete, or 1.0 if the day is not a business day.
     *         {@link io.deephaven.util.QueryConstants#NULL_DOUBLE} if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionBusinessDayComplete(final Instant time) {
        if (time == null) {
            return NULL_DOUBLE;
        }

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
     * @return the fraction of the business day complete, or 1.0 if the day is not a business day.
     *         {@link io.deephaven.util.QueryConstants#NULL_DOUBLE} if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionBusinessDayComplete(final ZonedDateTime time) {
        if (time == null) {
            return NULL_DOUBLE;
        }

        return fractionBusinessDayComplete(time.toInstant());
    }

    /**
     * Fraction of the current business day complete.
     *
     * @return the fraction of the business day complete, or 1.0 if the day is not a business day
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionBusinessDayComplete() {
        return fractionBusinessDayComplete(DateTimeUtils.now());
    }

    /**
     * Fraction of the business day remaining.
     *
     * @param time time
     * @return the fraction of the business day complete, or 0.0 if the day is not a business
     *         day.{@link io.deephaven.util.QueryConstants#NULL_DOUBLE} if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionBusinessDayRemaining(final Instant time) {
        if (time == null) {
            return NULL_DOUBLE;
        }

        return 1.0 - fractionBusinessDayComplete(time);
    }

    /**
     * Fraction of the business day remaining.
     *
     * @param time time
     * @return the fraction of the business day complete, or 0.0 if the day is not a business day.
     *         {@link io.deephaven.util.QueryConstants#NULL_DOUBLE} if the input is {@code null}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionBusinessDayRemaining(final ZonedDateTime time) {
        if (time == null) {
            return NULL_DOUBLE;
        }

        return 1.0 - fractionBusinessDayComplete(time);
    }

    /**
     * Fraction of the business day remaining.
     *
     * @return the fraction of the business day complete, or 0.0 if the day is not a business day
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionBusinessDayRemaining() {
        return fractionBusinessDayRemaining(DateTimeUtils.now());
    }

    // endregion

    // region Ranges

    private int numberBusinessDatesInternal(final LocalDate start, final LocalDate end, final boolean startInclusive,
            final boolean endInclusive) {
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
     * @return number of business dates between {@code start} and {@code end}. {@link QueryConstants#NULL_INT} if any
     *         input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberBusinessDates(final LocalDate start, final LocalDate end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return NULL_INT;
        }

        if (start.isAfter(end)) {
            return 0;
        }

        SummaryData summaryFirst = null;
        SummaryData summary = null;
        int days = 0;

        for (Iterator<SummaryData> it = summaryCache.iterator(start, end, startInclusive, endInclusive); it
                .hasNext();) {
            summary = it.next();

            if (summaryFirst == null) {
                summaryFirst = summary;
            }

            days += summary.businessDays;
        }

        if (summaryFirst == null) {
            return numberBusinessDatesInternal(start, end, startInclusive, endInclusive);
        } else {
            days += numberBusinessDatesInternal(start, summaryFirst.startDate, startInclusive, false);
            days += numberBusinessDatesInternal(summary.endDate, end, true, endInclusive);
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
     * @return number of business dates between {@code start} and {@code end}. {@link QueryConstants#NULL_INT} if any
     *         input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public int numberBusinessDates(final String start, final String end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return NULL_INT;
        }

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
     * @return number of business dates between {@code start} and {@code end}. {@link QueryConstants#NULL_INT} if any
     *         input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberBusinessDates(final ZonedDateTime start, final ZonedDateTime end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return NULL_INT;
        }

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
     * @return number of business dates between {@code start} and {@code end}. {@link QueryConstants#NULL_INT} if any
     *         input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberBusinessDates(final Instant start, final Instant end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return NULL_INT;
        }

        return numberBusinessDates(DateTimeUtils.toLocalDate(start, timeZone()),
                DateTimeUtils.toLocalDate(end, timeZone()), startInclusive, endInclusive);
    }

    /**
     * Returns the number of business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return number of business dates between {@code start} and {@code end}; including {@code start} and {@code end}.
     *         {@link QueryConstants#NULL_INT} if any input is {@code null}.
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
     * @return number of business dates between {@code start} and {@code end}; including {@code start} and {@code end}.
     *         {@link QueryConstants#NULL_INT} if any input is {@code null}.
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
     * @return number of business dates between {@code start} and {@code end}; including {@code start} and {@code end}.
     *         {@link QueryConstants#NULL_INT} if any input is {@code null}.
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
     * @return number of business dates between {@code start} and {@code end}; including {@code start} and {@code end}.
     *         {@link QueryConstants#NULL_INT} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberBusinessDates(final Instant start, final Instant end) {
        return numberBusinessDates(start, end, true, true);
    }

    private int numberNonBusinessDatesInternal(final LocalDate start, final LocalDate end, final boolean startInclusive,
            final boolean endInclusive) {
        int days = 0;

        for (LocalDate day = start; !day.isAfter(end); day = day.plusDays(1)) {
            final boolean skip = (!startInclusive && day.equals(start)) || (!endInclusive && day.equals(end));

            if (!skip && !isBusinessDay(day)) {
                days++;
            }
        }

        return days;
    }

    /**
     * Returns the number of non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of non-business dates between {@code start} and {@code end}. {@link QueryConstants#NULL_INT} if
     *         any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberNonBusinessDates(final LocalDate start, final LocalDate end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return NULL_INT;
        }

        if (start.isAfter(end)) {
            return 0;
        }

        SummaryData summaryFirst = null;
        SummaryData summary = null;
        int days = 0;

        for (Iterator<SummaryData> it = summaryCache.iterator(start, end, startInclusive, endInclusive); it
                .hasNext();) {
            summary = it.next();

            if (summaryFirst == null) {
                summaryFirst = summary;
            }

            days += summary.nonBusinessDays;
        }

        if (summaryFirst == null) {
            return numberNonBusinessDatesInternal(start, end, startInclusive, endInclusive);
        } else {
            days += numberNonBusinessDatesInternal(start, summaryFirst.startDate, startInclusive, false);
            days += numberNonBusinessDatesInternal(summary.endDate, end, true, endInclusive);
        }

        return days;
    }

    /**
     * Returns the number of non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of non-business dates between {@code start} and {@code end}. {@link QueryConstants#NULL_INT} if
     *         any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public int numberNonBusinessDates(final String start, final String end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return NULL_INT;
        }

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
     * @return number of non-business dates between {@code start} and {@code end}. {@link QueryConstants#NULL_INT} if
     *         any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberNonBusinessDates(final ZonedDateTime start, final ZonedDateTime end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return NULL_INT;
        }

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
     * @return number of non-business dates between {@code start} and {@code end}. {@link QueryConstants#NULL_INT} if
     *         any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberNonBusinessDates(final Instant start, final Instant end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return NULL_INT;
        }

        return numberNonBusinessDates(DateTimeUtils.toLocalDate(start, timeZone()),
                DateTimeUtils.toLocalDate(end, timeZone()), startInclusive, endInclusive);
    }

    /**
     * Returns the number of non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return number of non-business dates between {@code start} and {@code end}; including {@code start} and
     *         {@code end}. {@link QueryConstants#NULL_INT} if any input is {@code null}.
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
     *         {@code end}. {@link QueryConstants#NULL_INT} if any input is {@code null}.
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
     *         {@code end}. {@link QueryConstants#NULL_INT} if any input is {@code null}.
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
     *         {@code end}. {@link QueryConstants#NULL_INT} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public int numberNonBusinessDates(final Instant start, final Instant end) {
        return numberNonBusinessDates(start, end, true, true);
    }

    private void businessDatesInternal(final ArrayList<LocalDate> result, final LocalDate start, final LocalDate end,
            final boolean startInclusive,
            final boolean endInclusive) {
        for (LocalDate day = start; !day.isAfter(end); day = day.plusDays(1)) {
            final boolean skip = (!startInclusive && day.equals(start)) || (!endInclusive && day.equals(end));

            if (!skip && isBusinessDay(day)) {
                result.add(day);
            }
        }
    }

    /**
     * Returns the business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return business dates between {@code start} and {@code end}. {@code null} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] businessDates(final LocalDate start, final LocalDate end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return null;
        }

        if (start.isAfter(end)) {
            return new LocalDate[0];
        }

        final ArrayList<LocalDate> dateList = new ArrayList<>();

        SummaryData summaryFirst = null;
        SummaryData summary = null;

        for (Iterator<SummaryData> it = summaryCache.iterator(start, end, startInclusive, endInclusive); it
                .hasNext();) {
            summary = it.next();

            if (summaryFirst == null) {
                summaryFirst = summary;
                businessDatesInternal(dateList, start, summaryFirst.startDate, startInclusive, false);
            }

            dateList.addAll(summary.businessDates);
        }

        if (summaryFirst == null) {
            businessDatesInternal(dateList, start, end, startInclusive, endInclusive);
        } else {
            businessDatesInternal(dateList, summary.endDate, end, true, endInclusive);
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
     * @return business dates between {@code start} and {@code end}. {@code null} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public String[] businessDates(final String start, final String end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return null;
        }

        final LocalDate[] dates =
                businessDates(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end), startInclusive,
                        endInclusive);
        return dates == null ? null : Arrays.stream(dates).map(DateTimeUtils::formatDate).toArray(String[]::new);
    }

    /**
     * Returns the business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return business dates between {@code start} and {@code end}. {@code null} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] businessDates(final ZonedDateTime start, final ZonedDateTime end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return null;
        }

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
     * @return business dates between {@code start} and {@code end}. {@code null} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] businessDates(final Instant start, final Instant end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return null;
        }

        return businessDates(DateTimeUtils.toLocalDate(start, timeZone()), DateTimeUtils.toLocalDate(end, timeZone()),
                startInclusive, endInclusive);
    }

    /**
     * Returns the business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return business dates between {@code start} and {@code end}; including {@code start} and {@code end}.
     *         {@code null} if any input is {@code null}.
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
     * @return business dates between {@code start} and {@code end}; including {@code start} and {@code end}.
     *         {@code null} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public String[] businessDates(final String start, final String end) {
        return businessDates(start, end, true, true);
    }

    /**
     * Returns the business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return business dates between {@code start} and {@code end}; including {@code start} and {@code end}.
     *         {@code null} if any input is {@code null}.
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
     * @return business dates between {@code start} and {@code end}; including {@code start} and {@code end}.
     *         {@code null} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] businessDates(final Instant start, final Instant end) {
        return businessDates(start, end, true, true);
    }

    private void nonBusinessDatesInternal(final ArrayList<LocalDate> result, final LocalDate start, final LocalDate end,
            final boolean startInclusive,
            final boolean endInclusive) {
        for (LocalDate day = start; !day.isAfter(end); day = day.plusDays(1)) {
            final boolean skip = (!startInclusive && day.equals(start)) || (!endInclusive && day.equals(end));

            if (!skip && !isBusinessDay(day)) {
                result.add(day);
            }
        }
    }

    /**
     * Returns the non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return non-business dates between {@code start} and {@code end}. {@code null} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] nonBusinessDates(final LocalDate start, final LocalDate end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return null;
        }

        if (start.isAfter(end)) {
            return new LocalDate[0];
        }

        final ArrayList<LocalDate> dateList = new ArrayList<>();

        SummaryData summaryFirst = null;
        SummaryData summary = null;

        for (Iterator<SummaryData> it = summaryCache.iterator(start, end, startInclusive, endInclusive); it
                .hasNext();) {
            summary = it.next();

            if (summaryFirst == null) {
                summaryFirst = summary;
                nonBusinessDatesInternal(dateList, start, summaryFirst.startDate, startInclusive, false);
            }

            dateList.addAll(summary.nonBusinessDates);
        }

        if (summaryFirst == null) {
            nonBusinessDatesInternal(dateList, start, end, startInclusive, endInclusive);
        } else {
            nonBusinessDatesInternal(dateList, summary.endDate, end, true, endInclusive);
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
     * @return non-business dates between {@code start} and {@code end}. {@code null} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public String[] nonBusinessDates(final String start, final String end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return null;
        }

        final LocalDate[] dates =
                nonBusinessDates(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end), startInclusive,
                        endInclusive);
        return dates == null ? null : Arrays.stream(dates).map(DateTimeUtils::formatDate).toArray(String[]::new);
    }

    /**
     * Returns the non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return non-business dates between {@code start} and {@code end}. {@code null} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] nonBusinessDates(final ZonedDateTime start, final ZonedDateTime end,
            final boolean startInclusive, final boolean endInclusive) {
        if (start == null || end == null) {
            return null;
        }

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
     * @return non-business dates between {@code start} and {@code end}. {@code null} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] nonBusinessDates(final Instant start, final Instant end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return null;
        }

        return nonBusinessDates(DateTimeUtils.toLocalDate(start, timeZone()),
                DateTimeUtils.toLocalDate(end, timeZone()), startInclusive, endInclusive);
    }

    /**
     * Returns the non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return non-business dates between {@code start} and {@code end}; including {@code start} and {@code end}.
     *         {@code null} if any input is {@code null}.
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
     * @return non-business dates between {@code start} and {@code end}; including {@code start} and {@code end}.
     *         {@code null} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public String[] nonBusinessDates(final String start, final String end) {
        return nonBusinessDates(start, end, true, true);
    }

    /**
     * Returns the non-business dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return non-business dates between {@code start} and {@code end}; including {@code start} and {@code end}.
     *         {@code null} if any input is {@code null}.
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
     * @return non-business dates between {@code start} and {@code end}; including {@code start} and {@code end}.
     *         {@code null} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public LocalDate[] nonBusinessDates(final Instant start, final Instant end) {
        return nonBusinessDates(start, end, true, true);
    }

    // endregion

    // region Differences

    private long diffBusinessNanosInternal(final Instant start, final LocalDate startDate, final Instant end,
            final LocalDate endDate) {

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
     * @return the amount of business time in nanoseconds between {@code start} and {@code end}.
     *         {@link QueryConstants#NULL_LONG} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public long diffBusinessNanos(final Instant start, final Instant end) {
        if (start == null || end == null) {
            return NULL_LONG;
        }

        if (DateTimeUtils.isAfter(start, end)) {
            return -diffBusinessNanos(end, start);
        }

        final LocalDate startDate = DateTimeUtils.toLocalDate(start, timeZone());
        final LocalDate endDate = DateTimeUtils.toLocalDate(end, timeZone());

        assert startDate != null;
        assert endDate != null;

        SummaryData summaryFirst = null;
        SummaryData summary = null;
        long nanos = 0;

        for (Iterator<SummaryData> it = summaryCache.iterator(startDate, endDate, false, false); it.hasNext();) {
            summary = it.next();

            if (summaryFirst == null) {
                summaryFirst = summary;
            }

            nanos += summary.businessTimeNanos;
        }

        if (summaryFirst == null) {
            return diffBusinessNanosInternal(start, startDate, end, endDate);
        } else {
            nanos += diffBusinessNanosInternal(start, startDate, summaryFirst.startInstant, summaryFirst.startDate);
            nanos += diffBusinessNanosInternal(summary.endInstant, summary.endDate, end, endDate);
        }

        return nanos;
    }

    /**
     * Returns the amount of business time in nanoseconds between two times.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return the amount of business time in nanoseconds between {@code start} and {@code end}.
     *         {@link QueryConstants#NULL_LONG} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public long diffBusinessNanos(final ZonedDateTime start, final ZonedDateTime end) {
        if (start == null || end == null) {
            return NULL_LONG;
        }

        return diffBusinessNanos(start.toInstant(), end.toInstant());
    }

    /**
     * Returns the amount of non-business time in nanoseconds between two times.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return the amount of nonbusiness time in nanoseconds between {@code start} and {@code end}.
     *         {@link QueryConstants#NULL_LONG} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public long diffNonBusinessNanos(final Instant start, final Instant end) {
        if (start == null || end == null) {
            return NULL_LONG;
        }

        return DateTimeUtils.diffNanos(start, end) - diffBusinessNanos(start, end);
    }

    /**
     * Returns the amount of non-business time in nanoseconds between two times.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return the amount of non-business time in nanoseconds between {@code start} and {@code end}.
     *         {@link QueryConstants#NULL_LONG} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public long diffNonBusinessNanos(final ZonedDateTime start, final ZonedDateTime end) {
        if (start == null || end == null) {
            return NULL_LONG;
        }

        return diffNonBusinessNanos(start.toInstant(), end.toInstant());
    }

    /**
     * Returns the amount of business time between two times.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return the amount of business time between {@code start} and {@code end}. {@code null} if any input is
     *         {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public Duration diffBusinessDuration(final Instant start, final Instant end) {
        if (start == null || end == null) {
            return null;
        }

        return Duration.ofNanos(diffBusinessNanos(start, end));
    }

    /**
     * Returns the amount of business time between two times.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return the amount of business time between {@code start} and {@code end}. {@code null} if any input is
     *         {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public Duration diffBusinessDuration(final ZonedDateTime start, final ZonedDateTime end) {
        if (start == null || end == null) {
            return null;
        }

        return Duration.ofNanos(diffBusinessNanos(start, end));
    }

    /**
     * Returns the amount of non-business time between two times.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return the amount of non-business time between {@code start} and {@code end}. {@code null} if any input is
     *         {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public Duration diffNonBusinessDuration(final Instant start, final Instant end) {
        if (start == null || end == null) {
            return null;
        }

        return Duration.ofNanos(diffNonBusinessNanos(start, end));
    }

    /**
     * Returns the amount of non-business time between two times.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return the amount of non-business time between {@code start} and {@code end}. {@code null} if any input is
     *         {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public Duration diffNonBusinessDuration(final ZonedDateTime start, final ZonedDateTime end) {
        if (start == null || end == null) {
            return null;
        }

        return Duration.ofNanos(diffNonBusinessNanos(start, end));
    }

    /**
     * Returns the amount of business time in standard business days between two times.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return the amount of business time in standard business days between {@code start} and {@code end}.
     *         {@link QueryConstants#NULL_DOUBLE} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public double diffBusinessDays(final Instant start, final Instant end) {
        if (start == null || end == null) {
            return NULL_DOUBLE;
        }

        return (double) diffBusinessNanos(start, end) / (double) standardBusinessNanos();
    }

    /**
     * Returns the amount of business time in standard business days between two times.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return the amount of business time in standard business days between {@code start} and {@code end}.
     *         {@link QueryConstants#NULL_DOUBLE} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public double diffBusinessDays(final ZonedDateTime start, final ZonedDateTime end) {
        if (start == null || end == null) {
            return NULL_DOUBLE;
        }

        return (double) diffBusinessNanos(start, end) / (double) standardBusinessNanos();
    }

    /**
     * Returns the number of business years between {@code start} and {@code end}.
     *
     * @param start start; if null, return null
     * @param end end; if null, return null
     * @return the amount of business time in business years between the {@code start} and {@code end}.
     *         {@link QueryConstants#NULL_DOUBLE} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public double diffBusinessYears(final Instant start, final Instant end) {
        if (start == null || end == null) {
            return NULL_DOUBLE;
        }

        final int yearStart = DateTimeUtils.year(start, timeZone());
        final int yearEnd = DateTimeUtils.year(end, timeZone());

        if (yearStart == yearEnd) {
            return (double) diffBusinessNanos(start, end) / (double) getYearSummary(yearStart).businessTimeNanos;
        }

        final SummaryData yearDataStart = getYearSummary(yearStart);
        final SummaryData yearDataEnd = getYearSummary(yearEnd);

        return (double) diffBusinessNanos(start, yearDataStart.endInstant) / (double) yearDataStart.businessTimeNanos +
                (double) diffBusinessNanos(yearDataEnd.startInstant, end) / (double) yearDataEnd.businessTimeNanos +
                yearEnd - yearStart - 1;
    }

    /**
     * Returns the number of business years between {@code start} and {@code end}.
     *
     * @param start start; if null, return null
     * @param end end; if null, return null
     * @return the amount of business time in business years between the {@code start} and {@code end}.
     *         {@link QueryConstants#NULL_DOUBLE} if any input is {@code null}.
     * @throws InvalidDateException if the dates are not in the valid range
     */
    public double diffBusinessYears(final ZonedDateTime start, final ZonedDateTime end) {
        if (start == null || end == null) {
            return NULL_DOUBLE;
        }

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
     * @return {@code days} business days after {@code date}. {@code }null} if {@code date} is not a business day and
     *         {@code days} is zero. {@code null} if inputs are {@code null} or {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate plusBusinessDays(final LocalDate date, final int days) {
        if (date == null || days == NULL_INT) {
            return null;
        }

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
     * @return {@code days} business days after {@code date}. {@code null} if {@code date} is not a business day and
     *         {@code days} is zero. {@code null} if inputs are {@code null} or {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public String plusBusinessDays(final String date, final int days) {
        if (date == null || days == NULL_INT) {
            return null;
        }

        final LocalDate d = plusBusinessDays(DateTimeUtils.parseLocalDate(date), days);
        return d == null ? null : d.toString();
    }

    /**
     * Adds a specified number of business days to an input time. Adding negative days is equivalent to subtracting
     * days.
     * <p>
     * Day additions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} business days after {@code time}. {@code null} if {@code time} is not a business day and
     *         {@code days} is zero. {@code null} if inputs are {@code null} or {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public Instant plusBusinessDays(final Instant time, final int days) {
        if (time == null || days == NULL_INT) {
            return null;
        }

        final ZonedDateTime zdt = plusBusinessDays(DateTimeUtils.toZonedDateTime(time, timeZone()), days);
        return zdt == null ? null : zdt.toInstant();
    }

    /**
     * Adds a specified number of business days to an input time. Adding negative days is equivalent to subtracting
     * days.
     * <p>
     * Day additions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     * <p>
     * The resultant time will have the same time zone as the calendar. This could be different than the time zone of
     * the input {@link ZonedDateTime}.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} business days after {@code time}. {@code null} if {@code time} is not a business day and
     *         {@code days} is zero. {@code null} if inputs are {@code null} or {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public ZonedDateTime plusBusinessDays(final ZonedDateTime time, final int days) {
        if (time == null || days == NULL_INT) {
            return null;
        }

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
     * @return {@code days} business days before {@code date}. {@code null} if {@code date} is not a business day and
     *         {@code days} is zero. {@code null} if inputs are {@code null} or {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate minusBusinessDays(final LocalDate date, final int days) {
        if (date == null || days == NULL_INT) {
            return null;
        }

        return plusBusinessDays(date, -days);
    }

    /**
     * Subtracts a specified number of business days from an input date. Subtracting negative days is equivalent to
     * adding days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} business days before {@code date}. {@code null} if {@code date} is not a business day and
     *         {@code days} is zero. {@code null} if inputs are {@code null} or {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public String minusBusinessDays(final String date, final int days) {
        if (date == null || days == NULL_INT) {
            return null;
        }

        return plusBusinessDays(date, -days);
    }

    /**
     * Subtracts a specified number of business days from an input time. Subtracting negative days is equivalent to
     * adding days.
     * <p>
     * Day subtractions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} business days before {@code time}. {@code null} if {@code time} is not a business day and
     *         {@code days} is zero. {@code null} if inputs are {@code null} or {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public Instant minusBusinessDays(final Instant time, final int days) {
        if (time == null || days == NULL_INT) {
            return null;
        }

        return plusBusinessDays(time, -days);
    }

    /**
     * Subtracts a specified number of business days from an input time. Subtracting negative days is equivalent to
     * adding days.
     * <p>
     * Day subtraction are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     * <p>
     * The resultant time will have the same time zone as the calendar. This could be different than the time zone of
     * the input {@link ZonedDateTime}.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} business days before {@code time}. {@code null} if {@code time} is not a business day and
     *         {@code days} is zero. {@code null} if inputs are {@code null} or {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public ZonedDateTime minusBusinessDays(final ZonedDateTime time, final int days) {
        if (time == null || days == NULL_INT) {
            return null;
        }

        return plusBusinessDays(time, -days);
    }

    /**
     * Adds a specified number of non-business days to an input date. Adding negative days is equivalent to subtracting
     * days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} non-business days after {@code date}. {@code null} if {@code date} is not a business day and
     *         {@code days} is zero. {@code null} if inputs are {@code null} or {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate plusNonBusinessDays(final LocalDate date, final int days) {
        if (date == null || days == NULL_INT) {
            return null;
        }

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
     * @return {@code days} non-business days after {@code date}. {@code null} if {@code date} is not a business day and
     *         {@code days} is zero. {@code null} if inputs are {@code null} or {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public String plusNonBusinessDays(final String date, final int days) {
        if (date == null || days == NULL_INT) {
            return null;
        }

        final LocalDate d = this.plusNonBusinessDays(DateTimeUtils.parseLocalDate(date), days);
        return d == null ? null : d.toString();
    }

    /**
     * Adds a specified number of non-business days to an input time. Adding negative days is equivalent to subtracting
     * days.
     * <p>
     * Day additions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     * <p>
     * The resultant time will have the same time zone as the calendar. This could be different than the time zone of
     * the input {@link ZonedDateTime}.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} non-business days after {@code time}. {@code null} if {@code time} is not a business day and
     *         {@code days} is zero. {@code null} if inputs are {@code null} or {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public Instant plusNonBusinessDays(final Instant time, final int days) {
        if (time == null || days == NULL_INT) {
            return null;
        }

        final ZonedDateTime zdt = plusNonBusinessDays(DateTimeUtils.toZonedDateTime(time, timeZone()), days);
        return zdt == null ? null : zdt.toInstant();
    }

    /**
     * Adds a specified number of non-business days to an input time. Adding negative days is equivalent to subtracting
     * days.
     * <p>
     * Day additions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     * <p>
     * The resultant time will have the same time zone as the calendar. This could be different than the time zone of
     * the input {@link ZonedDateTime}.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} non-business days after {@code time}. {@code null} if {@code time} is not a business day and
     *         {@code days} is zero. {@code null} if inputs are {@code null} or {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public ZonedDateTime plusNonBusinessDays(final ZonedDateTime time, final int days) {
        if (time == null || days == NULL_INT) {
            return null;
        }

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
     * @return {@code days} non-business days before {@code date}. {@code null} if {@code date} is a business day and
     *         {@code days} is zero. {@code null} if inputs are {@code null} or {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate minusNonBusinessDays(final LocalDate date, final int days) {
        if (date == null || days == NULL_INT) {
            return null;
        }

        return this.plusNonBusinessDays(date, -days);
    }

    /**
     * Subtracts a specified number of non-business days to an input date. Subtracting negative days is equivalent to
     * adding days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} non-business days before {@code date}. {@code null} if {@code date} is a business day and
     *         {@code days} is zero.
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public String minusNonBusinessDays(final String date, final int days) {
        if (date == null || days == NULL_INT) {
            return null;
        }

        return plusNonBusinessDays(date, -days);
    }

    /**
     * Subtracts a specified number of non-business days to an input time. Subtracting negative days is equivalent to
     * adding days.
     * <p>
     * Day subtractions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} non-business days before {@code time}. {@code null} if {@code time} is a business day and
     *         {@code days} is zero. {@code null} if inputs are {@code null} or {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public Instant minusNonBusinessDays(final Instant time, final int days) {
        if (time == null || days == NULL_INT) {
            return null;
        }

        return plusNonBusinessDays(time, -days);
    }

    /**
     * Subtracts a specified number of non-business days to an input time. Subtracting negative days is equivalent to
     * adding days.
     * <p>
     * Day subtractions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     * <p>
     * The resultant time will have the same time zone as the calendar. This could be different than the time zone of
     * the input {@link ZonedDateTime}.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} non-business days before {@code time}. {@code null} if {@code time} is a business day and
     *         {@code days} is zero. {@code null} if inputs are {@code null} or {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public ZonedDateTime minusNonBusinessDays(final ZonedDateTime time, final int days) {
        if (time == null || days == NULL_INT) {
            return null;
        }

        return plusNonBusinessDays(time, -days);
    }

    /**
     * Adds a specified number of business days to the current date. Adding negative days is equivalent to subtracting
     * days.
     *
     * @param days number of days to add.
     * @return {@code days} business days after the current date. {@code null} if the current date is not a business day
     *         and {@code days} is zero. {@code null} if input is {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate futureBusinessDate(final int days) {
        if (days == NULL_INT) {
            return null;
        }

        return plusBusinessDays(calendarDate(), days);
    }

    /**
     * Subtracts a specified number of business days from the current date. Subtracting negative days is equivalent to
     * adding days.
     *
     * @param days number of days to subtract.
     * @return {@code days} business days before the current date. {@code null} if the current date is not a business
     *         day and {@code days} is zero. {@code null} if input is {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate pastBusinessDate(final int days) {
        if (days == NULL_INT) {
            return null;
        }

        return minusBusinessDays(calendarDate(), days);
    }

    /**
     * Adds a specified number of non-business days to the current date. Adding negative days is equivalent to
     * subtracting days.
     *
     * @param days number of days to add.
     * @return {@code days} non-business days after the current date. {@code null} if the current date is a business day
     *         and {@code days} is zero. {@code null} if input is {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate futureNonBusinessDate(final int days) {
        if (days == NULL_INT) {
            return null;
        }

        return this.plusNonBusinessDays(calendarDate(), days);
    }

    /**
     * Subtracts a specified number of non-business days to the current date. Subtracting negative days is equivalent to
     * adding days.
     *
     * @param days number of days to subtract.
     * @return {@code days} non-business days before the current date. {@code null} if the current date is a business
     *         day and {@code days} is zero. {@code null} if input is {@link QueryConstants#NULL_INT}.
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate pastNonBusinessDate(final int days) {
        if (days == NULL_INT) {
            return null;
        }

        return minusNonBusinessDays(calendarDate(), days);
    }

    // endregion

}
