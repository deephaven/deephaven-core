/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
//TODO: update copyrights
package io.deephaven.time.calendar;

import java.time.*;

//TODO: major cleanup

**

/**
 * Convenience methods for {@link BusinessCalendar} and {@link Calendar}.
 */
@SuppressWarnings({"WeakerAccess", "SameParameterValue"})
public class StaticCalendarMethods {


    // These methods are imported from DateTimeUtils, so are not included here
    // public static long diffNanos(final Instant start, final Instant end) {
    // public static long diffDay(final Instant start, final Instant end) {
    // public static long diffYear(final Instant start, final Instant end) {

    private StaticCalendarMethods() {
    }


//
//
//
//    *****
//
//    public static String name() {
//    }
//
//    public static String description() {
//    }
//
//    public static ZoneId timeZone() {
//    }
//
//    public static LocalDate plusDays(final LocalDate date, final int days) {
//    }
//
//    public static LocalDate plusDays(final String date, final int days) {
//    }
//
//    public static LocalDate plusDays(final Instant time, final int days) {
//    }
//
//    public static LocalDate plusDays(final ZonedDateTime time, final int days) {
//    }
//
//    public static LocalDate minusDays(final LocalDate date, final int days) {
//    }
//
//    public static LocalDate minusDays(final String date, final int days) {
//    }
//
//    public static LocalDate minusDays(final Instant time, final int days) {
//    }
//
//    public static LocalDate minusDays(final ZonedDateTime time, final int days) {
//    }
//
//    public static LocalDate currentDate() {
//    }
//
//    public static LocalDate futureDate(final int days) {
//    }
//
//    public static LocalDate pastDate(final int days) {
//    }
//
//    public static LocalDate[] calendarDates(final LocalDate start, final LocalDate end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static LocalDate[] calendarDates(final String start, final String end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static LocalDate[] calendarDates(final ZonedDateTime start, final ZonedDateTime end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static LocalDate[] calendarDates(final Instant start, final Instant end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static LocalDate[] calendarDates(final LocalDate start, final LocalDate end) {
//    }
//
//    public static LocalDate[] calendarDates(final String start, final String end) {
//    }
//
//    public static LocalDate[] calendarDates(final ZonedDateTime start, final ZonedDateTime end) {
//    }
//
//    public static LocalDate[] calendarDates(final Instant start, final Instant end) {
//    }
//
//    public static int numberCalendarDates(final LocalDate start, final LocalDate end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static int numberCalendarDates(final String start, final String end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static int numberCalendarDates(final ZonedDateTime start, final ZonedDateTime end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static int numberCalendarDates(final Instant start, final Instant end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static int numberCalendarDates(final LocalDate start, final LocalDate end) {
//    }
//
//    public static int numberCalendarDates(final String start, final String end) {
//    }
//
//    public static int numberCalendarDates(final ZonedDateTime start, final ZonedDateTime end) {
//    }
//
//    public static int numberCalendarDates(final Instant start, final Instant end) {
//    }
//
//    *****
//
//    public static static class InvalidDateException extends RuntimeException {
//    }
//
//    public static InvalidDateException(final String message, final Throwable cause) {
//    }
//
//    public static YearData(final Instant start, final Instant end, final long businessTimeNanos) {
//    }
//
//    public static BusinessCalendar(final String name, final String description, final ZoneId timeZone, final LocalDate firstValidDate, final LocalDate lastValidDate, final BusinessSchedule<LocalTime> standardBusinessSchedule, final Set<DayOfWeek> weekendDays, final Map<LocalDate, BusinessSchedule<Instant>> holidays) {
//    }
//
//    public static BusinessSchedule<LocalTime> standardBusinessSchedule() {
//    }
//
//    public static long standardBusinessNanos() {
//    }
//
//    public static Map<LocalDate, BusinessSchedule<Instant>> holidays() {
//    }
//
//    public static BusinessSchedule<Instant> businessSchedule(final LocalDate date) {
//    }
//
//    public static BusinessSchedule<Instant> businessSchedule(final ZonedDateTime time) {
//    }
//
//    public static BusinessSchedule<Instant> businessSchedule(final Instant time) {
//    }
//
//    public static BusinessSchedule<Instant> businessSchedule(final String date) {
//    }
//
//    public static BusinessSchedule<Instant> businessSchedule() {
//    }
//
//    public static boolean isBusinessDay(final LocalDate date) {
//    }
//
//    public static boolean isBusinessDay(final String date) {
//    }
//
//    public static boolean isBusinessDay(final ZonedDateTime time) {
//    }
//
//    public static boolean isBusinessDay(final Instant time) {
//    }
//
//    public static boolean isBusinessDay(final DayOfWeek day) {
//    }
//
//    public static boolean isBusinessDay() {
//    }
//
//    public static boolean isLastBusinessDayOfMonth(final ZonedDateTime time) {
//    }
//
//    public static boolean isLastBusinessDayOfMonth(final Instant time) {
//    }
//
//    public static boolean isLastBusinessDayOfMonth() {
//    }
//
//    public static boolean isLastBusinessDayOfWeek(final LocalDate date) {
//    }
//
//    public static boolean isLastBusinessDayOfWeek(final ZonedDateTime time) {
//    }
//
//    public static boolean isLastBusinessDayOfWeek(final Instant time) {
//    }
//
//    public static boolean isLastBusinessDayOfWeek(final String date) {
//    }
//
//    public static boolean isLastBusinessDayOfWeek() {
//    }
//
//    public static boolean isLastBusinessDayOfYear(final ZonedDateTime time) {
//    }
//
//    public static boolean isLastBusinessDayOfYear(final Instant time) {
//    }
//
//    public static boolean isLastBusinessDayOfYear() {
//    }
//
//    public static boolean isBusinessTime(final ZonedDateTime time) {
//    }
//
//    public static boolean isBusinessTime(final Instant time) {
//    }
//
//    public static boolean isBusinessTime() {
//    }
//
//    public static double fractionStandardBusinessDay(final LocalDate date) {
//    }
//
//    public static double fractionStandardBusinessDay(final String date) {
//    }
//
//    public static double fractionStandardBusinessDay(final Instant time) {
//    }
//
//    public static double fractionStandardBusinessDay(final ZonedDateTime time) {
//    }
//
//    public static double fractionStandardBusinessDay() {
//    }
//
//    public static double fractionBusinessDayComplete(final Instant time) {
//    }
//
//    public static double fractionBusinessDayComplete(final ZonedDateTime time) {
//    }
//
//    public static double fractionBusinessDayComplete() {
//    }
//
//    public static double fractionBusinessDayRemaining(final Instant time) {
//    }
//
//    public static double fractionBusinessDayRemaining(final ZonedDateTime time) {
//    }
//
//    public static double fractionBusinessDayRemaining() {
//    }
//
//    public static int numberBusinessDates(final LocalDate start, final LocalDate end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static int numberBusinessDates(final String start, final String end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static int numberBusinessDates(final ZonedDateTime start, final ZonedDateTime end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static int numberBusinessDates(final Instant start, final Instant end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static int numberBusinessDates(final LocalDate start, final LocalDate end) {
//    }
//
//    public static int numberBusinessDates(final String start, final String end) {
//    }
//
//    public static int numberBusinessDates(final ZonedDateTime start, final ZonedDateTime end) {
//    }
//
//    public static int numberBusinessDates(final Instant start, final Instant end) {
//    }
//
//    public static int numberNonBusinessDates(final LocalDate start, final LocalDate end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static int numberNonBusinessDates(final String start, final String end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static int numberNonBusinessDates(final ZonedDateTime start, final ZonedDateTime end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static int numberNonBusinessDates(final Instant start, final Instant end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static int numberNonBusinessDates(final LocalDate start, final LocalDate end) {
//    }
//
//    public static int numberNonBusinessDates(final String start, final String end) {
//    }
//
//    public static int numberNonBusinessDates(final ZonedDateTime start, final ZonedDateTime end) {
//    }
//
//    public static int numberNonBusinessDates(final Instant start, final Instant end) {
//    }
//
//    public static LocalDate[] businessDates(final LocalDate start, final LocalDate end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static LocalDate[] businessDates(final String start, final String end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static LocalDate[] businessDates(final ZonedDateTime start, final ZonedDateTime end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static LocalDate[] businessDates(final Instant start, final Instant end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static LocalDate[] businessDates(final LocalDate start, final LocalDate end) {
//    }
//
//    public static LocalDate[] businessDates(final String start, final String end) {
//    }
//
//    public static LocalDate[] businessDates(final ZonedDateTime start, final ZonedDateTime end) {
//    }
//
//    public static LocalDate[] businessDates(final Instant start, final Instant end) {
//    }
//
//    public static LocalDate[] nonBusinessDates(final LocalDate start, final LocalDate end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static LocalDate[] nonBusinessDates(final String start, final String end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static LocalDate[] nonBusinessDates(final ZonedDateTime start, final ZonedDateTime end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static LocalDate[] nonBusinessDates(final Instant start, final Instant end, final boolean startInclusive, final boolean endInclusive) {
//    }
//
//    public static LocalDate[] nonBusinessDates(final LocalDate start, final LocalDate end) {
//    }
//
//    public static LocalDate[] nonBusinessDates(final String start, final String end) {
//    }
//
//    public static LocalDate[] nonBusinessDates(final ZonedDateTime start, final ZonedDateTime end) {
//    }
//
//    public static LocalDate[] nonBusinessDates(final Instant start, final Instant end) {
//    }
//
//    public static long diffBusinessNanos(final Instant start, final Instant end) {
//    }
//
//    public static long diffBusinessNanos(final ZonedDateTime start, final ZonedDateTime end) {
//    }
//
//    public static long diffNonBusinessNanos(final Instant start, final Instant end) {
//    }
//
//    public static long diffNonBusinessNanos(final ZonedDateTime start, final ZonedDateTime end) {
//    }
//
//    public static double diffBusinessDays(final Instant start, final Instant end) {
//    }
//
//    public static double diffBusinessDays(final ZonedDateTime start, final ZonedDateTime end) {
//    }
//
//    public static double diffBusinessYears(final Instant start, final Instant end) {
//    }
//
//    public static double diffBusinessYears(final ZonedDateTime start, final ZonedDateTime end) {
//    }
//
//    public static LocalDate plusBusinessDays(final LocalDate date, final int days) {
//    }
//
//    public static LocalDate plusBusinessDays(final String date, final int days) {
//    }
//
//    public static LocalDate plusBusinessDays(final Instant time, final int days) {
//    }
//
//    public static LocalDate plusBusinessDays(final ZonedDateTime time, final int days) {
//    }
//
//    public static LocalDate minusBusinessDays(final LocalDate date, final int days) {
//    }
//
//    public static LocalDate minusBusinessDays(final String date, final int days) {
//    }
//
//    public static LocalDate minusBusinessDays(final Instant time, final int days) {
//    }
//
//    public static LocalDate minusBusinessDays(final ZonedDateTime time, final int days) {
//    }
//
//    public static LocalDate plusNonBusinessDays(final LocalDate date, final int days) {
//    }
//
//    public static LocalDate plusNonBusinessDays(final String date, final int days) {
//    }
//
//    public static LocalDate plusNonBusinessDays(final Instant time, final int days) {
//    }
//
//    public static LocalDate plusNonBusinessDays(final ZonedDateTime time, final int days) {
//    }
//
//    public static LocalDate minusNonBusinessDays(final LocalDate date, final int days) {
//    }
//
//    public static LocalDate minusNonBusinessDays(final String date, final int days) {
//    }
//
//    public static LocalDate minusNonBusinessDays(final Instant time, final int days) {
//    }
//
//    public static LocalDate minusNonBusinessDays(final ZonedDateTime time, final int days) {
//    }
//
//    public static LocalDate futureBusinessDate(final int days) {
//    }
//
//    public static LocalDate pastBusinessDate(final int days) {
//    }
//
//    public static LocalDate futureNonBusinessDate(final int days) {
//    }
//
//    public static LocalDate pastNonBusinessDate(final int days) {
//    }
//
//
//                                                                                                                *****
//
//    public static String name() {
//        return Calendars.calendar().name();
//    }
//
//    public static String currentDay() {
//        return Calendars.calendar().currentDate();
//    }
//
//    public static String previousDay() {
//        return Calendars.calendar().pastDate();
//    }
//
//    public static String previousDay(int days) {
//        return Calendars.calendar().pastDate(days);
//    }
//
//    public static String previousDay(final Instant time) {
//        return Calendars.calendar().pastDate(time);
//    }
//
//    public static String previousDay(final Instant time, final int days) {
//        return Calendars.calendar().pastDate(time, days);
//    }
//
//    public static String previousDay(final String date) {
//        return Calendars.calendar().pastDate(date);
//    }
//
//    public static String previousDay(final String date, final int days) {
//        return Calendars.calendar().pastDate(date, days);
//    }
//
//    public static String nextDay() {
//        return Calendars.calendar().futureDate();
//    }
//
//    public static String nextDay(int days) {
//        return Calendars.calendar().futureDate(days);
//    }
//
//    public static String nextDay(final Instant time) {
//        return Calendars.calendar().futureDate(time);
//    }
//
//    public static String nextDay(final Instant time, final int days) {
//        return Calendars.calendar().futureDate(time, days);
//    }
//
//    public static String nextDay(final String date) {
//        return Calendars.calendar().futureDate(date);
//    }
//
//    public static String nextDay(final String date, final int days) {
//        return Calendars.calendar().futureDate(date, days);
//    }
//
//    public static String[] daysInRange(Instant start, Instant end) {
//        return Calendars.calendar().calendarDates(start, end);
//    }
//
//    public static String[] daysInRange(final String start, final String end) {
//        return Calendars.calendar().calendarDates(start, end);
//    }
//
//    public static int numberOfDays(final Instant start, final Instant end) {
//        return Calendars.calendar().numberCalendarDates(start, end);
//    }
//
//    public static int numberOfDays(final Instant start, final Instant end, final boolean endInclusive) {
//        return Calendars.calendar().numberCalendarDates(start, end, endInclusive);
//    }
//
//    public static int numberOfDays(final String start, final String end) {
//        return Calendars.calendar().numberCalendarDates(start, end);
//    }
//
//    public static int numberOfDays(final String start, final String end, final boolean endInclusive) {
//        return Calendars.calendar().numberCalendarDates(start, end, endInclusive);
//    }
//
//    public static DayOfWeek dayOfWeek() {
//        return Calendars.calendar().dayOfWeek();
//    }
//
//    public static DayOfWeek dayOfWeek(final Instant time) {
//        return Calendars.calendar().dayOfWeek(time);
//    }
//
//    public static DayOfWeek dayOfWeek(final String date) {
//        return Calendars.calendar().dayOfWeek(date);
//    }
//
//    public static ZoneId calendarTimeZone() {
//        return Calendars.calendar().timeZone();
//    }
//
//    public static boolean isBusinessDay() {
//        return Calendars.calendar().isBusinessDay();
//    }
//
//    public static boolean isBusinessDay(Instant time) {
//        return Calendars.calendar().isBusinessDay(time);
//    }
//
//    public static boolean isBusinessDay(String date) {
//        return Calendars.calendar().isBusinessDay(date);
//    }
//
//    public static boolean isBusinessDay(LocalDate date) {
//        return Calendars.calendar().isBusinessDay(date);
//    }
//
//    public static boolean isBusinessTime(Instant time) {
//        return Calendars.calendar().isBusinessTime(time);
//    }
//
//    public static String previousBusinessDay() {
//        return Calendars.calendar().pastBusinessDate();
//    }
//
//    public static String previousBusinessDay(int days) {
//        return Calendars.calendar().pastBusinessDate(days);
//    }
//
//    public static String previousBusinessDay(Instant time) {
//        return Calendars.calendar().pastBusinessDate(time);
//    }
//
//    public static String previousBusinessDay(Instant time, int days) {
//        return Calendars.calendar().pastBusinessDate(time, days);
//    }
//
//    public static String previousBusinessDay(String date) {
//        return Calendars.calendar().pastBusinessDate(date);
//    }
//
//    public static String previousBusinessDay(String date, int days) {
//        return Calendars.calendar().pastBusinessDate(date, days);
//    }
//
//    public static BusinessSchedule previousBusinessSchedule() {
//        return Calendars.calendar().previousBusinessSchedule();
//    }
//
//    public static BusinessSchedule previousBusinessSchedule(int days) {
//        return Calendars.calendar().previousBusinessSchedule(days);
//    }
//
//    public static BusinessSchedule previousBusinessSchedule(Instant time) {
//        return Calendars.calendar().previousBusinessSchedule(time);
//    }
//
//    public static BusinessSchedule previousBusinessSchedule(Instant time, int days) {
//        return Calendars.calendar().previousBusinessSchedule(time, days);
//    }
//
//    public static BusinessSchedule previousBusinessSchedule(String date) {
//        return Calendars.calendar().previousBusinessSchedule(date);
//    }
//
//    public static BusinessSchedule previousBusinessSchedule(String date, int days) {
//        return Calendars.calendar().previousBusinessSchedule(date, days);
//    }
//
//    public static String previousNonBusinessDay() {
//        return Calendars.calendar().pastNonBusinessDate();
//    }
//
//    public static String previousNonBusinessDay(int days) {
//        return Calendars.calendar().pastNonBusinessDate(days);
//    }
//
//    public static String previousNonBusinessDay(Instant time) {
//        return Calendars.calendar().pastNonBusinessDate(time);
//    }
//
//    public static String previousNonBusinessDay(Instant time, int days) {
//        return Calendars.calendar().pastNonBusinessDate(time, days);
//    }
//
//    public static String previousNonBusinessDay(String date) {
//        return Calendars.calendar().pastNonBusinessDate(date);
//    }
//
//    public static String previousNonBusinessDay(String date, int days) {
//        return Calendars.calendar().pastNonBusinessDate(date, days);
//    }
//
//    public static String nextBusinessDay() {
//        return Calendars.calendar().futureBusinessDate();
//    }
//
//    public static String nextBusinessDay(int days) {
//        return Calendars.calendar().futureBusinessDate(days);
//    }
//
//    public static String nextBusinessDay(Instant time) {
//        return Calendars.calendar().futureBusinessDate(time);
//    }
//
//    public static String nextBusinessDay(Instant time, int days) {
//        return Calendars.calendar().futureBusinessDate(time, days);
//    }
//
//    public static String nextBusinessDay(String date) {
//        return Calendars.calendar().futureBusinessDate(date);
//    }
//
//    public static String nextBusinessDay(String date, int days) {
//        return Calendars.calendar().futureBusinessDate(date, days);
//    }
//
//    public static BusinessSchedule nextBusinessSchedule() {
//        return Calendars.calendar().nextBusinessSchedule();
//    }
//
//    public static BusinessSchedule nextBusinessSchedule(int days) {
//        return Calendars.calendar().nextBusinessSchedule(days);
//    }
//
//    public static BusinessSchedule nextBusinessSchedule(Instant time) {
//        return Calendars.calendar().nextBusinessSchedule(time);
//    }
//
//    public static BusinessSchedule nextBusinessSchedule(Instant time, int days) {
//        return Calendars.calendar().nextBusinessSchedule(time, days);
//    }
//
//    public static BusinessSchedule nextBusinessSchedule(String date) {
//        return Calendars.calendar().nextBusinessSchedule(date);
//    }
//
//    public static BusinessSchedule nextBusinessSchedule(String date, int days) {
//        return Calendars.calendar().nextBusinessSchedule(date, days);
//    }
//
//    public static String nextNonBusinessDay() {
//        return Calendars.calendar().futureNonBusinessDate();
//    }
//
//    public static String nextNonBusinessDay(int days) {
//        return Calendars.calendar().futureNonBusinessDate(days);
//    }
//
//    public static String nextNonBusinessDay(Instant time) {
//        return Calendars.calendar().futureNonBusinessDate(time);
//    }
//
//    public static String nextNonBusinessDay(Instant time, int days) {
//        return Calendars.calendar().futureNonBusinessDate(time, days);
//    }
//
//    public static String nextNonBusinessDay(String date) {
//        return Calendars.calendar().futureNonBusinessDate(date);
//    }
//
//    public static String nextNonBusinessDay(String date, int days) {
//        return Calendars.calendar().futureNonBusinessDate(date, days);
//    }
//
//    public static String[] businessDaysInRange(Instant start, Instant end) {
//        return Calendars.calendar().businessDates(start, end);
//    }
//
//    public static String[] businessDaysInRange(String start, String end) {
//        return Calendars.calendar().businessDates(start, end);
//    }
//
//    public static String[] nonBusinessDaysInRange(Instant start, Instant end) {
//        return Calendars.calendar().nonBusinessDates(start, end);
//    }
//
//    public static String[] nonBusinessDaysInRange(String start, String end) {
//        return Calendars.calendar().nonBusinessDates(start, end);
//    }
//
//    public static long standardBusinessDayLengthNanos() {
//        return Calendars.calendar().standardBusinessNanos();
//    }
//
//    public static long diffBusinessNanos(Instant start, Instant end) {
//        return Calendars.calendar().diffBusinessNanos(start, end);
//    }
//
//    public static long diffNonBusinessNanos(Instant start, Instant end) {
//        return Calendars.calendar().diffNonBusinessNanos(start, end);
//    }
//
//    public static double diffBusinessDay(Instant start, Instant end) {
//        return Calendars.calendar().diffBusinessDays(start, end);
//    }
//
//    public static double diffNonBusinessDay(Instant start, Instant end) {
//        return Calendars.calendar().diffNonBusinessDays(start, end);
//    }
//
//    public static double diffBusinessYear(Instant start, Instant end) {
//        return Calendars.calendar().diffBusinessYears(start, end);
//    }
//
//    public static int numberOfBusinessDays(Instant start, Instant end) {
//        return Calendars.calendar().numberOfBusinessDays(start, end);
//    }
//
//    public static int numberOfBusinessDays(Instant start, Instant end, boolean endInclusive) {
//        return Calendars.calendar().numberBusinessDates(start, end, endInclusive);
//    }
//
//    public static int numberOfBusinessDays(String start, String end) {
//        return Calendars.calendar().numberOfBusinessDays(start, end);
//    }
//
//    public static int numberOfBusinessDays(String start, String end, boolean endInclusive) {
//        return Calendars.calendar().numberBusinessDates(start, end, endInclusive);
//    }
//
//    public static int numberOfNonBusinessDays(Instant start, Instant end) {
//        return Calendars.calendar().numberOfNonBusinessDays(start, end);
//    }
//
//    public static int numberOfNonBusinessDays(Instant start, Instant end, boolean endInclusive) {
//        return Calendars.calendar().numberNonBusinessDates(start, end, endInclusive);
//    }
//
//    public static int numberOfNonBusinessDays(String start, String end) {
//        return Calendars.calendar().numberOfNonBusinessDays(start, end);
//    }
//
//    public static int numberOfNonBusinessDays(String start, String end, boolean endInclusive) {
//        return Calendars.calendar().numberNonBusinessDates(start, end, endInclusive);
//    }
//
//    public static double fractionOfStandardBusinessDay() {
//        return Calendars.calendar().fractionStandardBusinessDay();
//    }
//
//    public static double fractionOfStandardBusinessDay(Instant time) {
//        return Calendars.calendar().fractionStandardBusinessDay(time);
//    }
//
//    public static double fractionOfStandardBusinessDay(String date) {
//        return Calendars.calendar().fractionStandardBusinessDay(date);
//    }
//
//    public static double fractionOfBusinessDayRemaining(Instant time) {
//        return Calendars.calendar().fractionBusinessDayRemaining(time);
//    }
//
//    public static double fractionOfBusinessDayComplete(Instant time) {
//        return Calendars.calendar().fractionBusinessDayComplete(time);
//    }
//
//    public static boolean isLastBusinessDayOfMonth() {
//        return Calendars.calendar().isLastBusinessDayOfMonth();
//    }
//
//    public static boolean isLastBusinessDayOfMonth(Instant time) {
//        return Calendars.calendar().isLastBusinessDayOfMonth(time);
//    }
//
//    public static boolean isLastBusinessDayOfMonth(String date) {
//        return Calendars.calendar().isLastBusinessDayOfMonth(date);
//    }
//
//    public static boolean isLastBusinessDayOfWeek() {
//        return Calendars.calendar().isLastBusinessDayOfWeek();
//    }
//
//    public static boolean isLastBusinessDayOfWeek(Instant time) {
//        return Calendars.calendar().isLastBusinessDayOfWeek(time);
//    }
//
//    public static boolean isLastBusinessDayOfWeek(String date) {
//        return Calendars.calendar().isLastBusinessDayOfWeek(date);
//    }
//
//    public static BusinessSchedule getBusinessSchedule(Instant time) {
//        return Calendars.calendar().businessSchedule(time);
//    }
//
//    public static BusinessSchedule getBusinessSchedule(String date) {
//        return Calendars.calendar().businessSchedule(date);
//    }
//
//    public static BusinessSchedule getBusinessSchedule(LocalDate date) {
//        return Calendars.calendar().businessSchedule(date);
//    }
}
