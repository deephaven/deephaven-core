/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import io.deephaven.base.verify.Require;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;

import java.time.*;
import java.util.*;

//TODO: update all headers
//TODO: review all docs
//TODO: add final to all methods

/**
 * A business calendar, with the concept of business and non-business time.
 *
 * Methods that take strings as arguments will generally be lower performance than non-string methods,
 * since strings must first be parsed into dates or times.
 */
//TODO: fail on out of range
//TODO should the methods be DB null tolerant
    //TODO: add null annotations
@SuppressWarnings("unused") //TODO: remove unused annotation
public class BusinessCalendar extends Calendar {

    private final LocalDate firstValidDate;
    private final LocalDate lastValidDate;
    private final BusinessSchedule<LocalTime> standardBusinessSchedule;
    private final Set<DayOfWeek> weekendDays;
    private final Map<LocalDate, BusinessSchedule<Instant>> holidays;

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
        private InvalidDateException(String message) {
            super(message);
        }

        /**
         * Creates a new exception.
         *
         * @param message exception message.
         * @param cause cause of the exception.
         */
        public InvalidDateException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // endregion

    // region Cache

    private final Map<LocalDate,BusinessSchedule<Instant>> cachedSchedules = new HashMap<>();
    private final Map<Integer, YearData> cachedYearData = new HashMap<>();

    private void populateSchedules() {
        LocalDate date = firstValidDate;

        while(date.compareTo(lastValidDate) <= 0) {

            final BusinessSchedule<Instant> s = holidays.get(date);

            if( s != null){
                cachedSchedules.put(date, s);
            } else if(weekendDays.contains(date.getDayOfWeek())) {
                cachedSchedules.put(date, BusinessSchedule.toInstant(BusinessSchedule.HOLIDAY, date, timeZone()));
            } else {
                cachedSchedules.put(date, BusinessSchedule.toInstant(standardBusinessSchedule, date, timeZone()));
            }

            date = date.plusDays(1);
        }
    }

    private static class YearData {
        private final ZonedDateTime start; //TODO: type?
        private final ZonedDateTime end;   //TODO: type?
        private final long businessTimeNanos;

        public YearData(final ZonedDateTime start, final ZonedDateTime end, final long businessTimeNanos) {
            this.start = start;
            this.end = end;
            this.businessTimeNanos = businessTimeNanos;
        }
    }

    private void populateCachedYearData() {
        // Only cache complete years, since incomplete years can not be fully computed.

        final int yearStart = firstValidDate.getDayOfYear() == 1 ? firstValidDate.getYear() : firstValidDate.getYear() + 1;
        final int yearEnd = ((lastValidDate.isLeapYear() && lastValidDate.getDayOfYear() == 366) || lastValidDate.getDayOfYear() == 365) ? lastValidDate.getYear() : lastValidDate.getYear() - 1;

        for (int year = yearStart; year <= yearEnd; year++) {
            final LocalDate startDate = LocalDate.ofYearDay(year, 0);
            final LocalDate endDate = LocalDate.ofYearDay(year + 1, 0);
            final ZonedDateTime start = startDate.atTime(0, 0).atZone(timeZone());
            final ZonedDateTime end = endDate.atTime(0, 0).atZone(timeZone());

            LocalDate date = startDate;
            long businessTimeNanos = 0;

            while (date.isBefore(endDate)) {
                final BusinessSchedule<Instant> bs = businessSchedule(date);
                businessTimeNanos += bs.businessNanos();
                date = date.plusDays(1);
            }

            final YearData yd = new YearData(start, end, businessTimeNanos);
            cachedYearData.put(year, yd);
        }
    }

    // endregion

    // region Constructors

    public BusinessCalendar(String name, String description, ZoneId timeZone, LocalDate firstValidDate, LocalDate lastValidDate, BusinessSchedule<LocalTime> standardBusinessSchedule, Set<DayOfWeek> weekendDays, Map<LocalDate, BusinessSchedule<Instant>> holidays) {
        super(name, description, timeZone);
        this.firstValidDate = firstValidDate;
        this.lastValidDate = lastValidDate;
        this.standardBusinessSchedule = standardBusinessSchedule;
        this.weekendDays = weekendDays;
        this.holidays = holidays;
        populateSchedules();
        populateCachedYearData();
    }

    // endregion

    // region Business Schedule

    //TODO: rename?
    /**
     * Business schedule for a standard business day.
     *
     * @return business schedule for a standard business day.
     */
    public BusinessSchedule<LocalTime> standardBusinessSchedule() {
        return standardBusinessSchedule;
    }

    //TODO: rename?
    /**
     * Length of a standard business day in nanoseconds.
     *
     * @return length of a standard business day in nanoseconds
     */
    public long standardBusinessDayLengthNanos() {
        return standardBusinessSchedule.businessNanos();
    }

    /**
     * Business schedules for all holidays.  A holiday is a date that has a schedule that is different from
     * the schedule for a standard business day or weekend.
     *
     * @return a map of holiday dates and their business periods
     */
    public Map<LocalDate, BusinessSchedule<Instant>> holidays() {
        return Collections.unmodifiableMap(holidays);
    }

    /**
     * Gets the indicated business day's schedule.
     *
     * @param date date
     * @return the corresponding BusinessSchedule of {@code date}
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public BusinessSchedule<Instant> businessSchedule(final LocalDate date) {
        Require.neqNull(date, "date");

        if(date.isBefore(firstValidDate)){
            throw new InvalidDateException("Date is before the first valid business calendar date:  date=" + date + " firstValidDate=" + firstValidDate);
        } else if(date.isAfter(lastValidDate)){
            throw new InvalidDateException("Date is after the last valid business calendar date:  date=" + date + " lastValidDate=" + lastValidDate);
        }

        return cachedSchedules.get(date);
    }

    /**
     * Gets the indicated business day's schedule.
     *
     * @param time time
     * @return the corresponding BusinessSchedule of {@code date}
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public BusinessSchedule<Instant> businessSchedule(final ZonedDateTime time) {
        Require.neqNull(time, "time");
        return businessSchedule(time.withZoneSameInstant(timeZone()).toLocalDate());
    }

    /**
     * Gets the indicated business day's schedule.
     *
     * @param time time
     * @return the corresponding BusinessSchedule of {@code date}
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public BusinessSchedule<Instant> businessSchedule(final Instant time) {
        Require.neqNull(time, "time");
        return businessSchedule(time.atZone(timeZone()));
    }

    /**
     * Gets the indicated business day's schedule.
     *
     * @param date date
     * @return the corresponding BusinessSchedule of {@code date}
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public BusinessSchedule<Instant> businessSchedule(String date) {
        Require.neqNull(date, "date");
        return businessSchedule(DateTimeUtils.parseLocalDate(date));
    }

    //TODO: rename current schedule? --> or zero arg?
    /**
     * Gets today's business schedule.
     *
     * @return today's business schedule
     */
    public BusinessSchedule<Instant> currentBusinessSchedule() {
        return businessSchedule(currentDate());
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
        return businessSchedule(date).isBusinessDay();
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
        return businessSchedule(date).isBusinessDay();
    }

    /**
     * Is the time on a business day?
     *
     * @param time time
     * @return true if the date is a business day; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isBusinessDay(final ZonedDateTime time){
        return businessSchedule(time).isBusinessDay();
    }

    /**
     * Is the time on a business day?
     *
     * @param time time
     * @return true if the date is a business day; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isBusinessDay(final Instant time){
        return businessSchedule(time).isBusinessDay();
    }

    //TODO: rename?
    /**
     * Is the day of the week a normal business day?
     *
     * @param day a day of the week
     * @return true if the day is a business day; false otherwise
     * @throws RequirementFailure if the input is null
     */
    public boolean isBusinessDay(DayOfWeek day){
        Require.neqNull(day, "day");
        return !weekendDays.contains(day);
    }

    /**
     * Is the current day a business day?
     *
     * @return true if the current day is a business day; false otherwise
     */
    public boolean isBusinessDay() {
        return isBusinessDay(currentDate());
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

        if(nextBusAfterDate == null){
            //TODO ** raise an error;
            return false;
        }

        return date.getMonth() != nextBusAfterDate.getMonth();
    }

    /**
     * Is the time on the last business day of the month?
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
    boolean isLastBusinessDayOfMonth(final String date) {
        Require.neqNull(date, "date");
        return isLastBusinessDayOfMonth(DateTimeUtils.parseLocalDate(date));
    }

    /**
     * Is the current date the last business day of the month?
     *
     * @return true if the current date is the last business day of the month; false otherwise.
     */
    public boolean isLastBusinessDayOfMonth() {
        return isLastBusinessDayOfMonth(currentDate());
    }

    /**
     * Is the date the last business day of the week?
     *
     * @param date date
     * @return true if {@code date} is on the last business day of the week; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isLastBusinessDayOfWeek(final LocalDate date){
        Require.neqNull(date, "date");

        if (!isBusinessDay(date)) {
            return false;
        }

        final LocalDate nextBusinessDay = plusBusinessDays(date, 1);
        return date.getDayOfWeek().compareTo(nextBusinessDay.getDayOfWeek()) > 0 || numberCalendarDates(date, nextBusinessDay) > 6;
    }

    /**
     * Is the time on the last business day of the week?
     *
     * @param time time
     * @return true if {@code time} is on the last business day of the week; false otherwise
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isLastBusinessDayOfWeek(final ZonedDateTime time) {
        Require.neqNull(time, "time");
        return isLastBusinessDayOfWeek(DateTimeUtils.toLocalDate(time, timeZone()));
    }

    /**
     * Is the time on the last business day of the week?
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
    public boolean isLastBusinessDayOfWeek(final String date){
        Require.neqNull(date, "date");
        return isLastBusinessDayOfWeek(DateTimeUtils.parseLocalDate(date));
    }

    /**
     * Is the current date the last business day of the week?
     *
     * @return true if the current date is the last business day of the week; false otherwise.
     */
    public boolean isLastBusinessDayOfWeek() {
        return isLastBusinessDayOfWeek(currentDate());
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

        if(nextBusAfterDate == null){
            //TODO ** raise an error;
            return false;
        }

        return date.getYear() != nextBusAfterDate.getYear();
    }

    /**
     * Is the time on the last business day of the year?
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
     * @return true if the current date is the last business day of the year; false otherwise.
     */
    public boolean isLastBusinessDayOfYear() {
        return isLastBusinessDayOfYear(currentDate());
    }

    // endregion

    // region Business Time

    /**
     * Determines if the specified time is a business time.
     * Business times fall within business periods of the day's business schedule.
     *
     * @param time time
     * @return true if the specified time is a business time; otherwise, false
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isBusinessTime(final ZonedDateTime time) {
        Require.neqNull(time, "time");
        return businessSchedule(time).isBusinessTime(time.toInstant());
    }

    /**
     * Determines if the specified time is a business time.
     * Business times fall within business periods of the day's business schedule.
     *
     * @param time time
     * @return true if the specified time is a business time; otherwise, false
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isBusinessTime(final Instant time) {
        Require.neqNull(time, "time");
        return businessSchedule(time).isBusinessTime(time);
    }

    /**
     * Determines if the current time is a business time.
     * Business times fall within business periods of the day's business schedule.
     *
     * @return true if the specified time is a business time; otherwise, false
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public boolean isBusinessTime() {
        return isBusinessTime(DateTimeUtils.now());
    }

    /**
     * Returns the ratio of the business day length and the standard business day length.
     * For example, a holiday has zero business time and will therefore return 0.0.
     * A normal business day will be of the standard length and will therefore return 1.0.
     * A half day holiday will return 0.5.
     *
     * @param date date
     * @return ratio of the business day length and the standard business day length for the date
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionOfStandardBusinessDay(final LocalDate date){
        Require.neqNull(date, "date");
        final BusinessSchedule<Instant> schedule = businessSchedule(date);
        return (double) schedule.businessNanos() / (double) standardBusinessDayLengthNanos();
    }

    /**
     * Returns the ratio of the business day length and the standard business day length.
     * For example, a holiday has zero business time and will therefore return 0.0.
     * A normal business day will be of the standard length and will therefore return 1.0.
     * A half day holiday will return 0.5.
     *
     * @param date date
     * @return ratio of the business day length and the standard business day length for the date
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public double fractionOfStandardBusinessDay(final String date){
        Require.neqNull(date, "date");
        return fractionOfStandardBusinessDay(DateTimeUtils.parseLocalDate(date));
    }

    /**
     * Returns the ratio of the business day length and the standard business day length.
     * For example, a holiday has zero business time and will therefore return 0.0.
     * A normal business day will be of the standard length and will therefore return 1.0.
     * A half day holiday will return 0.5.
     *
     * @param time time
     * @return ratio of the business day length and the standard business day length for the date
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionOfStandardBusinessDay(final Instant time){
        Require.neqNull(time, "time");
        return fractionOfStandardBusinessDay(DateTimeUtils.toLocalDate(time, timeZone()));
    }

    /**
     * Returns the ratio of the business day length and the standard business day length.
     * For example, a holiday has zero business time and will therefore return 0.0.
     * A normal business day will be of the standard length and will therefore return 1.0.
     * A half day holiday will return 0.5.
     *
     * @param time time
     * @return ratio of the business day length and the standard business day length for the date
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionOfStandardBusinessDay(final ZonedDateTime time){
        Require.neqNull(time, "time");
        return fractionOfStandardBusinessDay(DateTimeUtils.toLocalDate(time.toInstant(), timeZone()));
    }

    /**
     * Returns the ratio of the business day length and the standard business day length.
     * For example, a holiday has zero business time and will therefore return 0.0.
     * A normal business day will be of the standard length and will therefore return 1.0.
     * A half day holiday will return 0.5.
     *
     * @return ratio of the business day length and the standard business day length for the date
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionOfStandardBusinessDay() {
        return fractionOfStandardBusinessDay(currentDate());
    }

    //TODO: remove Of from function names!
    /**
     * Fraction of the business day complete.
     *
     * @param time time
     * @return the fraction of the business day complete, or 1.0 if the day is not a business day
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionOfBusinessDayComplete(final Instant time) {
        Require.neqNull(time, "time");

        final BusinessSchedule<Instant> schedule = businessSchedule(time);

        if(!schedule.isBusinessDay()) {
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
    public double fractionOfBusinessDayComplete(final ZonedDateTime time) {
        Require.neqNull(time, "time");
        return fractionOfBusinessDayComplete(time.toInstant());
    }

    /**
     * Fraction of the current business day complete.
     *
     * @return the fraction of the business day complete, or 1.0 if the day is not a business day
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionOfBusinessDayComplete() {
        return fractionOfBusinessDayComplete(DateTimeUtils.now());
    }

    /**
     * Fraction of the business day remaining.
     *
     * @param time time
     * @return the fraction of the business day complete, or 0.0 if the day is not a business day
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionOfBusinessDayRemaining(final Instant time){
        Require.neqNull(time, "time");
        return 1.0 - fractionOfBusinessDayComplete(time);
    }

    /**
     * Fraction of the business day remaining.
     *
     * @param time time
     * @return the fraction of the business day complete, or 0.0 if the day is not a business day
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionOfBusinessDayRemaining(final ZonedDateTime time){
        Require.neqNull(time, "time");
        return 1.0 - fractionOfBusinessDayComplete(time);
    }

    /**
     * Fraction of the business day remaining.
     *
     * @return the fraction of the business day complete, or 0.0 if the day is not a business day
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public double fractionOfBusinessDayRemaining(){
        return fractionOfBusinessDayRemaining(DateTimeUtils.now());
    }

    // endregion

    ***

    // region Ranges

    //TODO: consistently handle inclusive / exclusive in these ranges

    /**
     * Returns the number of business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_INT
     * @param end end time; if null, return NULL_INT
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return number of business days between the {@code start} and {@code end}, inclusive and {@code endInclusive}
     *         respectively.
     */
    public int numberBusinessDates(final LocalDate start, final LocalDate end, final boolean endInclusive)  {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        int days = 0;
        if (start.isBefore(end)) {
            if (isBusinessDay(start)) {
                days++;
            }
            start = plusBusinessDays(start, 1);
        } else if (start.isAfter(end)) {
            //TODO: is this working right?
            return -numberBusinessDates(end, start, endInclusive);
        }

        LocalDate day = start;

        while (day.isBefore(end)) {
            days++;
            day = plusBusinessDays(day,1);
        }

        return days + (endInclusive && isBusinessDay(end) ? 1 : 0);
    }

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
    public int numberBusinessDates(Instant start, Instant end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberBusinessDates(DateTimeUtils.toLocalDate(start, timeZone()), DateTimeUtils.toLocalDate(end, timeZone()), endInclusive);
    }

    /**
     * Returns the number of business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return number of business days between the {@code start} and {@code end}, inclusive and {@code endInclusive}
     *         respectively.
     */
    public int numberBusinessDates(ZonedDateTime start, ZonedDateTime end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberBusinessDates(DateTimeUtils.toLocalDate(start.toInstant(), timeZone()), DateTimeUtils.toLocalDate(end.toInstant(), timeZone()), endInclusive);
    }

    /**
     * Returns the number of business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_INT
     * @param end end time; if null, return NULL_INT
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return number of business days between the {@code start} and {@code end}, inclusive and {@code endInclusive}
     *         respectively.
     */
    public int numberBusinessDates(String start, String end, final boolean endInclusive)  {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberBusinessDates(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end), endInclusive);
    }

    /**
     * Returns the number of non-business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_INT
     * @param end end time; if null, return NULL_INT
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return number of non-business days between the {@code start} and {@code end}, inclusive and {@code endInclusive}
     *         respectively.
     */
    public int numberNonBusinessDates(final LocalDate start, final LocalDate end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberCalendarDates(start, end, endInclusive) - numberBusinessDates(start, end, endInclusive);
    }

    /**
     * Returns the number of non-business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return number of business days between the {@code start} and {@code end}, inclusive and {@code endInclusive}
     *         respectively.
     */
    public int numberNonBusinessDates(Instant start, Instant end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberNonBusinessDates(DateTimeUtils.toLocalDate(start, timeZone()), DateTimeUtils.toLocalDate(end, timeZone()), endInclusive);
    }

    /**
     * Returns the number of non-business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_INT
     * @param end end time; if null, return NULL_INT
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return number of non-business days between the {@code start} and {@code end}, inclusive and {@code endInclusive}
     *         respectively.
     */
    public int numberNonBusinessDates(final String start, final String end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberNonBusinessDates(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end), endInclusive);
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
    public LocalDate[] businessDates(final LocalDate start, final LocalDate end) {
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
    public LocalDate[] businessDates(final Instant start, final Instant end) {
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return businessDates(DateTimeUtils.toLocalDate(start, timeZone()), DateTimeUtils.toLocalDate(end, timeZone()));
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
    public LocalDate[] businessDates(final ZonedDateTime start, final ZonedDateTime end) {
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return businessDates(DateTimeUtils.toLocalDate(start.toInstant(), timeZone()), DateTimeUtils.toLocalDate(end.toInstant(), timeZone()));
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
    public LocalDate[] businessDates(String start, String end){
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return businessDates(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end));
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
    public LocalDate[] nonBusinessDates(final LocalDate start, final LocalDate end){
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
    public LocalDate[] nonBusinessDates(final Instant start, final Instant end){
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return nonBusinessDates(DateTimeUtils.toLocalDate(start, timeZone()), DateTimeUtils.toLocalDate(end, timeZone()));
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
    public LocalDate[] nonBusinessDates(final ZonedDateTime start, final ZonedDateTime end){
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return nonBusinessDates(DateTimeUtils.toLocalDate(start.toInstant(), timeZone()), DateTimeUtils.toLocalDate(end.toInstant(), timeZone()));
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
    public LocalDate[] nonBusinessDates(String start, String end){
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return nonBusinessDates(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end));
    }

    /**
     * Returns the amount of business time in nanoseconds between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @return the amount of business time in nanoseconds between the {@code start} and {@code end}
     */
    public long diffBusinessNanos(final Instant start, final Instant end) {
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
                BusinessSchedule<Instant> businessDate = businessSchedule(day);

                if (businessDate != null) {
                    for (BusinessPeriod<Instant> businessPeriod : businessDate.periods()) {
                        Instant endOfPeriod = businessPeriod.end();
                        Instant startOfPeriod = businessPeriod.start();

                        // noinspection StatementWithEmptyBody
                        if (DateTimeUtils.isAfter(day, endOfPeriod) || DateTimeUtils.isBefore(end, startOfPeriod)) {
                            // continue
                        } else if (!DateTimeUtils.isAfter(day, startOfPeriod)) {
                            if (DateTimeUtils.isBefore(end, endOfPeriod)) {
                                dayDiffNanos += DateTimeUtils.minus(end, startOfPeriod);
                            } else {
                                dayDiffNanos += businessPeriod.nanos();
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
            day = businessSchedule(plusBusinessDays(day,1)).businessStart();
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
    public long diffBusinessNanos(final ZonedDateTime start, final ZonedDateTime end) {
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
    public long diffNonBusinessNanos(final Instant start, final Instant end) {
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
    public long diffNonBusinessNanos(final ZonedDateTime start, final ZonedDateTime end) {
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
    public double diffBusinessDays(final Instant start, final Instant end) {
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
    public double diffBusinessDays(final ZonedDateTime start, final ZonedDateTime end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return diffBusinessDays(start.toInstant(), end.toInstant());
    }

    /**
     * Returns the amount of non-business time in standard business days between {@code start} and {@code end}.
     *
     * @param start start time; if null, return NULL_LONG
     * @param end end time; if null, return NULL_LONG
     * @return the amount of non-business time in standard business days between the {@code start} and {@code end}
     */
    public double diffNonBusinessDays(final Instant start, final Instant end){
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
    public double diffNonBusinessDays(final ZonedDateTime start, final ZonedDateTime end){
        if (start == null || end == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        double businessYearDiff = 0.0;
        ZonedDateTime time = start;

        while (!DateTimeUtils.isAfter(time, end)) {
            final int year = time.getYear();
            final YearData yd = cachedYearData.get(year);

            if(yd == null){
                throw new InvalidDateException("Business calendar does not contain a complete year for: year=" + year);
            }

            final long yearDiff;
            if (DateTimeUtils.isAfter(yd.end, end)) {
                yearDiff = diffBusinessNanos(time, end);
            } else {
                yearDiff = diffBusinessNanos(time, yd.end);
            }

            businessYearDiff += (double) yearDiff / (double) yd.businessTimeNanos;
            time = yd.end;
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
   public double diffBusinessYears(final Instant start, final Instant end){
        if (start == null || end == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return diffBusinessYears(DateTimeUtils.toZonedDateTime(start, timeZone()), DateTimeUtils.toZonedDateTime(end, timeZone()));
    }

    /**
     * Returns the number of business years between {@code start} and {@code end}.
     *
     * @param start start; if null, return null
     * @param end end; if null, return null
     * @return the amount of business time in business years between the {@code start} and {@code end}
     */
    public double diffBusinessYears(final ZonedDateTime start, final ZonedDateTime end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return diffBusinessYears(start.toInstant(), end.toInstant());
    }

    // endregion

    // region Arithmetic

    //TODO: should the add/subtract methods on Instants or ZDT return times of LocalDates?

    /**
     * Adds a specified number of business days to an input date.  Adding negative days is equivalent to subtracting days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} business days after {@code date}; null if {@code date} is not a business day and {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate plusBusinessDays(final LocalDate date, final int days) {
        Require.neqNull(date, "date");

        if(days == 0){
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
     * @return {@code days} business days after {@code date}; null if {@code date} is not a business day and {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate plusBusinessDays(final String date, final int days) {
        Require.neqNull(date, "date");
        return plusBusinessDays(DateTimeUtils.parseLocalDate(date), days);
    }

    /**
     * Adds a specified number of business days to an input date.  Adding negative days is equivalent to subtracting days.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} business days after {@code time}; null if {@code time} is not a business day and {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
         */
    public LocalDate plusBusinessDays(final Instant time, final int days) {
        Require.neqNull(time, "time");
        return plusBusinessDays(DateTimeUtils.toLocalDate(time, timeZone()), days);
    }

    /**
     * Adds a specified number of business days to an input date.  Adding negative days is equivalent to subtracting days.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} business days after {@code time}; null if {@code time} is not a business day and {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate plusBusinessDays(final ZonedDateTime time, final int days) {
        Require.neqNull(time, "time");
        return plusBusinessDays(time.toInstant(), days);
    }

    /**
     * Subtracts a specified number of business days from an input date.  Subtracting negative days is equivalent to adding days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} business days before {@code date}; null if {@code date} is not a business day and {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate minusBusinessDays(final LocalDate date, final int days) {
        return plusBusinessDays(date, -days);
    }

    /**
     * Subtracts a specified number of business days from an input date.  Subtracting negative days is equivalent to adding days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} business days before {@code date}; null if {@code date} is not a business day and {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate minusBusinessDays(final String date, final int days) {
        return plusBusinessDays(date, -days);
    }

    /**
     * Subtracts a specified number of business days from an input date.  Subtracting negative days is equivalent to adding days.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} business days before {@code time}; null if {@code time} is not a business day and {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate minusBusinessDays(final Instant time, final int days) {
        return plusBusinessDays(time, -days);
    }

    /**
     * Subtracts a specified number of business days from an input date.  Subtracting negative days is equivalent to adding days.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} business days before {@code time}; null if {@code time} is not a business day and {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate minusBusinessDays(final ZonedDateTime time, final int days) {
        return plusBusinessDays(time, -days);
    }

    /**
     * Adds a specified number of non-business days to an input date.  Adding negative days is equivalent to subtracting days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} non-business days after {@code date}; null if {@code date} is not a business day and {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate plusNonBusinessDays(final LocalDate date, final int days) {
        Require.neqNull(date, "date");

        if(days == 0){
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
     * @return {@code days} non-business days after {@code date}; null if {@code date} is not a business day and {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate plusNonBusinessDays(final String date, final int days) {
        Require.neqNull(date, "date");
        return this.plusNonBusinessDays(DateTimeUtils.parseLocalDate(date), days);
    }

        /**
         * Adds a specified number of non-business days to an input date.  Adding negative days is equivalent to subtracting days.
         *
         * @param time time
         * @param days number of days to add.
         * @return {@code days} non-business days after {@code time}; null if {@code time} is not a business day and {@code days} is zero.
         * @throws RequirementFailure if the input is null
         * @throws InvalidDateException if the date is not in the valid range
         */
    public LocalDate plusNonBusinessDays(final Instant time, final int days) {
        Require.neqNull(time, "time");
        return this.plusNonBusinessDays(DateTimeUtils.toLocalDate(time, timeZone()), days);
    }

    /**
     * Adds a specified number of non-business days to an input date.  Adding negative days is equivalent to subtracting days.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} non-business days after {@code time}; null if {@code time} is not a business day and {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate plusNonBusinessDays(final ZonedDateTime time, final int days) {
        Require.neqNull(time, "time");
        return plusNonBusinessDays(time.toInstant(), days);
    }

    /**
     * Subtracts a specified number of non-business days to an input date.  Subtracting negative days is equivalent to adding days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} non-business days before {@code date}; null if {@code date} is a business day and {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate minusNonBusinessDays(final LocalDate date, final int days) {
        return this.plusNonBusinessDays(date, -days);
    }

    /**
     * Subtracts a specified number of non-business days to an input date.  Subtracting negative days is equivalent to adding days.
     *
     * @param date date
     * @param days number of days to add.
     * @return {@code days} non-business days before {@code date}; null if {@code date} is a business day and {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate minusNonBusinessDays(final String date, final int days) {
        return plusNonBusinessDays(date, -days);
    }

    /**
     * Subtracts a specified number of non-business days to an input date.  Subtracting negative days is equivalent to adding days.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} non-business days before {@code time}; null if {@code time} is a business day and {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate minusNonBusinessDays(final Instant time, final int days) {
        return plusNonBusinessDays(time, -days);
    }

    /**
     * Subtracts a specified number of non-business days to an input date.  Subtracting negative days is equivalent to adding days.
     *
     * @param time time
     * @param days number of days to add.
     * @return {@code days} non-business days before {@code time}; null if {@code time} is a business day and {@code days} is zero.
     * @throws RequirementFailure if the input is null
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate minusNonBusinessDays(final ZonedDateTime time, final int days) {
        return plusNonBusinessDays(time, -days);
    }

    /**
     * Adds a specified number of business days to the current date.  Adding negative days is equivalent to subtracting days.
     *
     * @param days number of days to add.
     * @return {@code days} business days after the current date; null if the current date is not a business day and {@code days} is zero
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate futureBusinessDate(final int days) {
        return plusBusinessDays(currentDate(), days);
    }

    /**
     * Subtracts a specified number of business days from the current date.  Subtracting negative days is equivalent to adding days.
     *
     * @param days number of days to subtract.
     * @return {@code days} business days before the current date; null if the current date is not a business day and {@code days} is zero
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate pastBusinessDate(final int days) {
        return minusBusinessDays(currentDate(), days);
    }

    /**
     * Adds a specified number of non-business days to the current date.  Adding negative days is equivalent to subtracting days.
     *
     * @param days number of days to add.
     * @return {@code days} non-business days after the current date; null if the current date is a business day and {@code days} is zero
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate futureNonBusinessDate(final int days) {
        return this.plusNonBusinessDays(currentDate(), days);
    }

    /**
     * Subtracts a specified number of non-business days to the current date.  Subtracting negative days is equivalent to adding days.
     *
     * @param days number of days to subtract.
     * @return {@code days} non-business days before the current date; null if the current date is a business day and {@code days} is zero
     * @throws InvalidDateException if the date is not in the valid range
     */
    public LocalDate pastNonBusinessDate(final int days) {
        return minusNonBusinessDays(currentDate(), days);
    }

    // endregion

}
