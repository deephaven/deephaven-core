package io.deephaven.time.calendar;

import io.deephaven.time.DateTime;
import io.deephaven.time.TimeZone;

import java.time.DayOfWeek;
import java.time.LocalDate;

/**
 * Convenience methods for {@link BusinessCalendar} and {@link Calendar}.
 */
@SuppressWarnings({"WeakerAccess", "SameParameterValue"})
public class StaticCalendarMethods {


    // These methods are imported from DateTimeUtils, so are not included here
    // public static long diffNanos(final DateTime start, final DateTime end) {
    // public static long diffDay(final DateTime start, final DateTime end) {
    // public static long diffYear(final DateTime start, final DateTime end) {

    private StaticCalendarMethods() {

    }

    public static String name() {
        return Calendars.calendar().name();
    }

    public static String currentDay() {
        return Calendars.calendar().currentDay();
    }

    public static String previousDay() {
        return Calendars.calendar().previousDay();
    }

    public static String previousDay(int days) {
        return Calendars.calendar().previousDay(days);
    }

    public static String previousDay(final DateTime time) {
        return Calendars.calendar().previousDay(time);
    }

    public static String previousDay(final DateTime time, final int days) {
        return Calendars.calendar().previousDay(time, days);
    }

    public static String previousDay(final String date) {
        return Calendars.calendar().previousDay(date);
    }

    public static String previousDay(final String date, final int days) {
        return Calendars.calendar().previousDay(date, days);
    }

    public static String nextDay() {
        return Calendars.calendar().nextDay();
    }

    public static String nextDay(int days) {
        return Calendars.calendar().nextDay(days);
    }

    public static String nextDay(final DateTime time) {
        return Calendars.calendar().nextDay(time);
    }

    public static String nextDay(final DateTime time, final int days) {
        return Calendars.calendar().nextDay(time, days);
    }

    public static String nextDay(final String date) {
        return Calendars.calendar().nextDay(date);
    }

    public static String nextDay(final String date, final int days) {
        return Calendars.calendar().nextDay(date, days);
    }

    public static String[] daysInRange(DateTime start, DateTime end) {
        return Calendars.calendar().daysInRange(start, end);
    }

    public static String[] daysInRange(final String start, final String end) {
        return Calendars.calendar().daysInRange(start, end);
    }

    public static int numberOfDays(final DateTime start, final DateTime end) {
        return Calendars.calendar().numberOfDays(start, end);
    }

    public static int numberOfDays(final DateTime start, final DateTime end, final boolean endInclusive) {
        return Calendars.calendar().numberOfDays(start, end, endInclusive);
    }

    public static int numberOfDays(final String start, final String end) {
        return Calendars.calendar().numberOfDays(start, end);
    }

    public static int numberOfDays(final String start, final String end, final boolean endInclusive) {
        return Calendars.calendar().numberOfDays(start, end, endInclusive);
    }

    public static DayOfWeek dayOfWeek() {
        return Calendars.calendar().dayOfWeek();
    }

    public static DayOfWeek dayOfWeek(final DateTime time) {
        return Calendars.calendar().dayOfWeek(time);
    }

    public static DayOfWeek dayOfWeek(final String date) {
        return Calendars.calendar().dayOfWeek(date);
    }

    public static TimeZone timeZone() {
        return Calendars.calendar().timeZone();
    }

    public static boolean isBusinessDay() {
        return Calendars.calendar().isBusinessDay();
    }

    public static boolean isBusinessDay(DateTime time) {
        return Calendars.calendar().isBusinessDay(time);
    }

    public static boolean isBusinessDay(String date) {
        return Calendars.calendar().isBusinessDay(date);
    }

    public static boolean isBusinessDay(LocalDate date) {
        return Calendars.calendar().isBusinessDay(date);
    }

    public static boolean isBusinessTime(DateTime time) {
        return Calendars.calendar().isBusinessTime(time);
    }

    public static String previousBusinessDay() {
        return Calendars.calendar().previousBusinessDay();
    }

    public static String previousBusinessDay(int days) {
        return Calendars.calendar().previousBusinessDay(days);
    }

    public static String previousBusinessDay(DateTime time) {
        return Calendars.calendar().previousBusinessDay(time);
    }

    public static String previousBusinessDay(DateTime time, int days) {
        return Calendars.calendar().previousBusinessDay(time, days);
    }

    public static String previousBusinessDay(String date) {
        return Calendars.calendar().previousBusinessDay(date);
    }

    public static String previousBusinessDay(String date, int days) {
        return Calendars.calendar().previousBusinessDay(date, days);
    }

    public static BusinessSchedule previousBusinessSchedule() {
        return Calendars.calendar().previousBusinessSchedule();
    }

    public static BusinessSchedule previousBusinessSchedule(int days) {
        return Calendars.calendar().previousBusinessSchedule(days);
    }

    public static BusinessSchedule previousBusinessSchedule(DateTime time) {
        return Calendars.calendar().previousBusinessSchedule(time);
    }

    public static BusinessSchedule previousBusinessSchedule(DateTime time, int days) {
        return Calendars.calendar().previousBusinessSchedule(time, days);
    }

    public static BusinessSchedule previousBusinessSchedule(String date) {
        return Calendars.calendar().previousBusinessSchedule(date);
    }

    public static BusinessSchedule previousBusinessSchedule(String date, int days) {
        return Calendars.calendar().previousBusinessSchedule(date, days);
    }

    public static String previousNonBusinessDay() {
        return Calendars.calendar().previousNonBusinessDay();
    }

    public static String previousNonBusinessDay(int days) {
        return Calendars.calendar().previousNonBusinessDay(days);
    }

    public static String previousNonBusinessDay(DateTime time) {
        return Calendars.calendar().previousNonBusinessDay(time);
    }

    public static String previousNonBusinessDay(DateTime time, int days) {
        return Calendars.calendar().previousNonBusinessDay(time, days);
    }

    public static String previousNonBusinessDay(String date) {
        return Calendars.calendar().previousNonBusinessDay(date);
    }

    public static String previousNonBusinessDay(String date, int days) {
        return Calendars.calendar().previousNonBusinessDay(date, days);
    }

    public static String nextBusinessDay() {
        return Calendars.calendar().nextBusinessDay();
    }

    public static String nextBusinessDay(int days) {
        return Calendars.calendar().nextBusinessDay(days);
    }

    public static String nextBusinessDay(DateTime time) {
        return Calendars.calendar().nextBusinessDay(time);
    }

    public static String nextBusinessDay(DateTime time, int days) {
        return Calendars.calendar().nextBusinessDay(time, days);
    }

    public static String nextBusinessDay(String date) {
        return Calendars.calendar().nextBusinessDay(date);
    }

    public static String nextBusinessDay(String date, int days) {
        return Calendars.calendar().nextBusinessDay(date, days);
    }

    public static BusinessSchedule nextBusinessSchedule() {
        return Calendars.calendar().nextBusinessSchedule();
    }

    public static BusinessSchedule nextBusinessSchedule(int days) {
        return Calendars.calendar().nextBusinessSchedule(days);
    }

    public static BusinessSchedule nextBusinessSchedule(DateTime time) {
        return Calendars.calendar().nextBusinessSchedule(time);
    }

    public static BusinessSchedule nextBusinessSchedule(DateTime time, int days) {
        return Calendars.calendar().nextBusinessSchedule(time, days);
    }

    public static BusinessSchedule nextBusinessSchedule(String date) {
        return Calendars.calendar().nextBusinessSchedule(date);
    }

    public static BusinessSchedule nextBusinessSchedule(String date, int days) {
        return Calendars.calendar().nextBusinessSchedule(date, days);
    }

    public static String nextNonBusinessDay() {
        return Calendars.calendar().nextNonBusinessDay();
    }

    public static String nextNonBusinessDay(int days) {
        return Calendars.calendar().nextNonBusinessDay(days);
    }

    public static String nextNonBusinessDay(DateTime time) {
        return Calendars.calendar().nextNonBusinessDay(time);
    }

    public static String nextNonBusinessDay(DateTime time, int days) {
        return Calendars.calendar().nextNonBusinessDay(time, days);
    }

    public static String nextNonBusinessDay(String date) {
        return Calendars.calendar().nextNonBusinessDay(date);
    }

    public static String nextNonBusinessDay(String date, int days) {
        return Calendars.calendar().nextNonBusinessDay(date, days);
    }

    public static String[] businessDaysInRange(DateTime start, DateTime end) {
        return Calendars.calendar().businessDaysInRange(start, end);
    }

    public static String[] businessDaysInRange(String start, String end) {
        return Calendars.calendar().businessDaysInRange(start, end);
    }

    public static String[] nonBusinessDaysInRange(DateTime start, DateTime end) {
        return Calendars.calendar().nonBusinessDaysInRange(start, end);
    }

    public static String[] nonBusinessDaysInRange(String start, String end) {
        return Calendars.calendar().nonBusinessDaysInRange(start, end);
    }

    public static long standardBusinessDayLengthNanos() {
        return Calendars.calendar().standardBusinessDayLengthNanos();
    }

    public static long diffBusinessNanos(DateTime start, DateTime end) {
        return Calendars.calendar().diffBusinessNanos(start, end);
    }

    public static long diffNonBusinessNanos(DateTime start, DateTime end) {
        return Calendars.calendar().diffNonBusinessNanos(start, end);
    }

    public static double diffBusinessDay(DateTime start, DateTime end) {
        return Calendars.calendar().diffBusinessDay(start, end);
    }

    public static double diffNonBusinessDay(DateTime start, DateTime end) {
        return Calendars.calendar().diffNonBusinessDay(start, end);
    }

    public static double diffBusinessYear(DateTime start, DateTime end) {
        return Calendars.calendar().diffBusinessYear(start, end);
    }

    public static int numberOfBusinessDays(DateTime start, DateTime end) {
        return Calendars.calendar().numberOfBusinessDays(start, end);
    }

    public static int numberOfBusinessDays(DateTime start, DateTime end, boolean endInclusive) {
        return Calendars.calendar().numberOfBusinessDays(start, end, endInclusive);
    }

    public static int numberOfBusinessDays(String start, String end) {
        return Calendars.calendar().numberOfBusinessDays(start, end);
    }

    public static int numberOfBusinessDays(String start, String end, boolean endInclusive) {
        return Calendars.calendar().numberOfBusinessDays(start, end, endInclusive);
    }

    public static int numberOfNonBusinessDays(DateTime start, DateTime end) {
        return Calendars.calendar().numberOfNonBusinessDays(start, end);
    }

    public static int numberOfNonBusinessDays(DateTime start, DateTime end, boolean endInclusive) {
        return Calendars.calendar().numberOfNonBusinessDays(start, end, endInclusive);
    }

    public static int numberOfNonBusinessDays(String start, String end) {
        return Calendars.calendar().numberOfNonBusinessDays(start, end);
    }

    public static int numberOfNonBusinessDays(String start, String end, boolean endInclusive) {
        return Calendars.calendar().numberOfNonBusinessDays(start, end, endInclusive);
    }

    public static double fractionOfStandardBusinessDay() {
        return Calendars.calendar().fractionOfStandardBusinessDay();
    }

    public static double fractionOfStandardBusinessDay(DateTime time) {
        return Calendars.calendar().fractionOfStandardBusinessDay(time);
    }

    public static double fractionOfStandardBusinessDay(String date) {
        return Calendars.calendar().fractionOfStandardBusinessDay(date);
    }

    public static double fractionOfBusinessDayRemaining(DateTime time) {
        return Calendars.calendar().fractionOfBusinessDayRemaining(time);
    }

    public static double fractionOfBusinessDayComplete(DateTime time) {
        return Calendars.calendar().fractionOfBusinessDayComplete(time);
    }

    public static boolean isLastBusinessDayOfMonth() {
        return Calendars.calendar().isLastBusinessDayOfMonth();
    }

    public static boolean isLastBusinessDayOfMonth(DateTime time) {
        return Calendars.calendar().isLastBusinessDayOfMonth(time);
    }

    public static boolean isLastBusinessDayOfMonth(String date) {
        return Calendars.calendar().isLastBusinessDayOfMonth(date);
    }

    public static boolean isLastBusinessDayOfWeek() {
        return Calendars.calendar().isLastBusinessDayOfWeek();
    }

    public static boolean isLastBusinessDayOfWeek(DateTime time) {
        return Calendars.calendar().isLastBusinessDayOfWeek(time);
    }

    public static boolean isLastBusinessDayOfWeek(String date) {
        return Calendars.calendar().isLastBusinessDayOfWeek(date);
    }

    public static BusinessSchedule getBusinessSchedule(DateTime time) {
        return Calendars.calendar().getBusinessSchedule(time);
    }

    public static BusinessSchedule getBusinessSchedule(String date) {
        return Calendars.calendar().getBusinessSchedule(date);
    }

    public static BusinessSchedule getBusinessSchedule(LocalDate date) {
        return Calendars.calendar().getBusinessSchedule(date);
    }
}
