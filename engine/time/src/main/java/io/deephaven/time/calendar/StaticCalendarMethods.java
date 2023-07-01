/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

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

    public static String name() {
        return Calendars.calendar().name();
    }

    public static String currentDay() {
        return Calendars.calendar().currentDate();
    }

    public static String previousDay() {
        return Calendars.calendar().pastDate();
    }

    public static String previousDay(int days) {
        return Calendars.calendar().pastDate(days);
    }

    public static String previousDay(final Instant time) {
        return Calendars.calendar().pastDate(time);
    }

    public static String previousDay(final Instant time, final int days) {
        return Calendars.calendar().pastDate(time, days);
    }

    public static String previousDay(final String date) {
        return Calendars.calendar().pastDate(date);
    }

    public static String previousDay(final String date, final int days) {
        return Calendars.calendar().pastDate(date, days);
    }

    public static String nextDay() {
        return Calendars.calendar().futureDate();
    }

    public static String nextDay(int days) {
        return Calendars.calendar().futureDate(days);
    }

    public static String nextDay(final Instant time) {
        return Calendars.calendar().futureDate(time);
    }

    public static String nextDay(final Instant time, final int days) {
        return Calendars.calendar().futureDate(time, days);
    }

    public static String nextDay(final String date) {
        return Calendars.calendar().futureDate(date);
    }

    public static String nextDay(final String date, final int days) {
        return Calendars.calendar().futureDate(date, days);
    }

    public static String[] daysInRange(Instant start, Instant end) {
        return Calendars.calendar().calendarDates(start, end);
    }

    public static String[] daysInRange(final String start, final String end) {
        return Calendars.calendar().calendarDates(start, end);
    }

    public static int numberOfDays(final Instant start, final Instant end) {
        return Calendars.calendar().numberCalendarDates(start, end);
    }

    public static int numberOfDays(final Instant start, final Instant end, final boolean endInclusive) {
        return Calendars.calendar().numberCalendarDates(start, end, endInclusive);
    }

    public static int numberOfDays(final String start, final String end) {
        return Calendars.calendar().numberCalendarDates(start, end);
    }

    public static int numberOfDays(final String start, final String end, final boolean endInclusive) {
        return Calendars.calendar().numberCalendarDates(start, end, endInclusive);
    }

    public static DayOfWeek dayOfWeek() {
        return Calendars.calendar().dayOfWeek();
    }

    public static DayOfWeek dayOfWeek(final Instant time) {
        return Calendars.calendar().dayOfWeek(time);
    }

    public static DayOfWeek dayOfWeek(final String date) {
        return Calendars.calendar().dayOfWeek(date);
    }

    public static ZoneId calendarTimeZone() {
        return Calendars.calendar().timeZone();
    }

    public static boolean isBusinessDay() {
        return Calendars.calendar().isBusinessDay();
    }

    public static boolean isBusinessDay(Instant time) {
        return Calendars.calendar().isBusinessDay(time);
    }

    public static boolean isBusinessDay(String date) {
        return Calendars.calendar().isBusinessDay(date);
    }

    public static boolean isBusinessDay(LocalDate date) {
        return Calendars.calendar().isBusinessDay(date);
    }

    public static boolean isBusinessTime(Instant time) {
        return Calendars.calendar().isBusinessTime(time);
    }

    public static String previousBusinessDay() {
        return Calendars.calendar().pastBusinessDate();
    }

    public static String previousBusinessDay(int days) {
        return Calendars.calendar().pastBusinessDate(days);
    }

    public static String previousBusinessDay(Instant time) {
        return Calendars.calendar().pastBusinessDate(time);
    }

    public static String previousBusinessDay(Instant time, int days) {
        return Calendars.calendar().pastBusinessDate(time, days);
    }

    public static String previousBusinessDay(String date) {
        return Calendars.calendar().pastBusinessDate(date);
    }

    public static String previousBusinessDay(String date, int days) {
        return Calendars.calendar().pastBusinessDate(date, days);
    }

    public static BusinessSchedule previousBusinessSchedule() {
        return Calendars.calendar().previousBusinessSchedule();
    }

    public static BusinessSchedule previousBusinessSchedule(int days) {
        return Calendars.calendar().previousBusinessSchedule(days);
    }

    public static BusinessSchedule previousBusinessSchedule(Instant time) {
        return Calendars.calendar().previousBusinessSchedule(time);
    }

    public static BusinessSchedule previousBusinessSchedule(Instant time, int days) {
        return Calendars.calendar().previousBusinessSchedule(time, days);
    }

    public static BusinessSchedule previousBusinessSchedule(String date) {
        return Calendars.calendar().previousBusinessSchedule(date);
    }

    public static BusinessSchedule previousBusinessSchedule(String date, int days) {
        return Calendars.calendar().previousBusinessSchedule(date, days);
    }

    public static String previousNonBusinessDay() {
        return Calendars.calendar().pastNonBusinessDate();
    }

    public static String previousNonBusinessDay(int days) {
        return Calendars.calendar().pastNonBusinessDate(days);
    }

    public static String previousNonBusinessDay(Instant time) {
        return Calendars.calendar().pastNonBusinessDate(time);
    }

    public static String previousNonBusinessDay(Instant time, int days) {
        return Calendars.calendar().pastNonBusinessDate(time, days);
    }

    public static String previousNonBusinessDay(String date) {
        return Calendars.calendar().pastNonBusinessDate(date);
    }

    public static String previousNonBusinessDay(String date, int days) {
        return Calendars.calendar().pastNonBusinessDate(date, days);
    }

    public static String nextBusinessDay() {
        return Calendars.calendar().futureBusinessDate();
    }

    public static String nextBusinessDay(int days) {
        return Calendars.calendar().futureBusinessDate(days);
    }

    public static String nextBusinessDay(Instant time) {
        return Calendars.calendar().futureBusinessDate(time);
    }

    public static String nextBusinessDay(Instant time, int days) {
        return Calendars.calendar().futureBusinessDate(time, days);
    }

    public static String nextBusinessDay(String date) {
        return Calendars.calendar().futureBusinessDate(date);
    }

    public static String nextBusinessDay(String date, int days) {
        return Calendars.calendar().futureBusinessDate(date, days);
    }

    public static BusinessSchedule nextBusinessSchedule() {
        return Calendars.calendar().nextBusinessSchedule();
    }

    public static BusinessSchedule nextBusinessSchedule(int days) {
        return Calendars.calendar().nextBusinessSchedule(days);
    }

    public static BusinessSchedule nextBusinessSchedule(Instant time) {
        return Calendars.calendar().nextBusinessSchedule(time);
    }

    public static BusinessSchedule nextBusinessSchedule(Instant time, int days) {
        return Calendars.calendar().nextBusinessSchedule(time, days);
    }

    public static BusinessSchedule nextBusinessSchedule(String date) {
        return Calendars.calendar().nextBusinessSchedule(date);
    }

    public static BusinessSchedule nextBusinessSchedule(String date, int days) {
        return Calendars.calendar().nextBusinessSchedule(date, days);
    }

    public static String nextNonBusinessDay() {
        return Calendars.calendar().futureNonBusinessDate();
    }

    public static String nextNonBusinessDay(int days) {
        return Calendars.calendar().futureNonBusinessDate(days);
    }

    public static String nextNonBusinessDay(Instant time) {
        return Calendars.calendar().futureNonBusinessDate(time);
    }

    public static String nextNonBusinessDay(Instant time, int days) {
        return Calendars.calendar().futureNonBusinessDate(time, days);
    }

    public static String nextNonBusinessDay(String date) {
        return Calendars.calendar().futureNonBusinessDate(date);
    }

    public static String nextNonBusinessDay(String date, int days) {
        return Calendars.calendar().futureNonBusinessDate(date, days);
    }

    public static String[] businessDaysInRange(Instant start, Instant end) {
        return Calendars.calendar().businessDates(start, end);
    }

    public static String[] businessDaysInRange(String start, String end) {
        return Calendars.calendar().businessDates(start, end);
    }

    public static String[] nonBusinessDaysInRange(Instant start, Instant end) {
        return Calendars.calendar().nonBusinessDates(start, end);
    }

    public static String[] nonBusinessDaysInRange(String start, String end) {
        return Calendars.calendar().nonBusinessDates(start, end);
    }

    public static long standardBusinessDayLengthNanos() {
        return Calendars.calendar().standardBusinessDayLengthNanos();
    }

    public static long diffBusinessNanos(Instant start, Instant end) {
        return Calendars.calendar().diffBusinessNanos(start, end);
    }

    public static long diffNonBusinessNanos(Instant start, Instant end) {
        return Calendars.calendar().diffNonBusinessNanos(start, end);
    }

    public static double diffBusinessDay(Instant start, Instant end) {
        return Calendars.calendar().diffBusinessDays(start, end);
    }

    public static double diffNonBusinessDay(Instant start, Instant end) {
        return Calendars.calendar().diffNonBusinessDays(start, end);
    }

    public static double diffBusinessYear(Instant start, Instant end) {
        return Calendars.calendar().diffBusinessYears(start, end);
    }

    public static int numberOfBusinessDays(Instant start, Instant end) {
        return Calendars.calendar().numberOfBusinessDays(start, end);
    }

    public static int numberOfBusinessDays(Instant start, Instant end, boolean endInclusive) {
        return Calendars.calendar().numberBusinessDates(start, end, endInclusive);
    }

    public static int numberOfBusinessDays(String start, String end) {
        return Calendars.calendar().numberOfBusinessDays(start, end);
    }

    public static int numberOfBusinessDays(String start, String end, boolean endInclusive) {
        return Calendars.calendar().numberBusinessDates(start, end, endInclusive);
    }

    public static int numberOfNonBusinessDays(Instant start, Instant end) {
        return Calendars.calendar().numberOfNonBusinessDays(start, end);
    }

    public static int numberOfNonBusinessDays(Instant start, Instant end, boolean endInclusive) {
        return Calendars.calendar().numberNonBusinessDates(start, end, endInclusive);
    }

    public static int numberOfNonBusinessDays(String start, String end) {
        return Calendars.calendar().numberOfNonBusinessDays(start, end);
    }

    public static int numberOfNonBusinessDays(String start, String end, boolean endInclusive) {
        return Calendars.calendar().numberNonBusinessDates(start, end, endInclusive);
    }

    public static double fractionOfStandardBusinessDay() {
        return Calendars.calendar().fractionOfStandardBusinessDay();
    }

    public static double fractionOfStandardBusinessDay(Instant time) {
        return Calendars.calendar().fractionOfStandardBusinessDay(time);
    }

    public static double fractionOfStandardBusinessDay(String date) {
        return Calendars.calendar().fractionOfStandardBusinessDay(date);
    }

    public static double fractionOfBusinessDayRemaining(Instant time) {
        return Calendars.calendar().fractionOfBusinessDayRemaining(time);
    }

    public static double fractionOfBusinessDayComplete(Instant time) {
        return Calendars.calendar().fractionOfBusinessDayComplete(time);
    }

    public static boolean isLastBusinessDayOfMonth() {
        return Calendars.calendar().isLastBusinessDayOfMonth();
    }

    public static boolean isLastBusinessDayOfMonth(Instant time) {
        return Calendars.calendar().isLastBusinessDayOfMonth(time);
    }

    public static boolean isLastBusinessDayOfMonth(String date) {
        return Calendars.calendar().isLastBusinessDayOfMonth(date);
    }

    public static boolean isLastBusinessDayOfWeek() {
        return Calendars.calendar().isLastBusinessDayOfWeek();
    }

    public static boolean isLastBusinessDayOfWeek(Instant time) {
        return Calendars.calendar().isLastBusinessDayOfWeek(time);
    }

    public static boolean isLastBusinessDayOfWeek(String date) {
        return Calendars.calendar().isLastBusinessDayOfWeek(date);
    }

    public static BusinessSchedule getBusinessSchedule(Instant time) {
        return Calendars.calendar().businessSchedule(time);
    }

    public static BusinessSchedule getBusinessSchedule(String date) {
        return Calendars.calendar().businessSchedule(date);
    }

    public static BusinessSchedule getBusinessSchedule(LocalDate date) {
        return Calendars.calendar().businessSchedule(date);
    }
}
