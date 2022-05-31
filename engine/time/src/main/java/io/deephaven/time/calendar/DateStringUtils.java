/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.time.calendar;

import io.deephaven.util.QueryConstants;

import java.time.LocalDate;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Basic utilities for Date Strings.
 *
 * To comply with the ISO-8601 standard for dates, Strings should be of the form "yyyy-MM-dd"
 *
 * Quiet methods are functionally equivalent to their counterparts, but assume Date String validity to remove the
 * overhead of checking.
 */
@SuppressWarnings("WeakerAccess")
public class DateStringUtils {
    private DateStringUtils() {}

    private static final Locale DATE_STRING_LOCALE = new Locale("en", "US");
    private static final DateTimeFormatter DATE_STRING_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd")
            .withLocale(DATE_STRING_LOCALE)
            .withChronology(IsoChronology.INSTANCE)
            .withResolverStyle(ResolverStyle.STRICT);
    private static final Pattern DATE_STRING_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");

    /**
     * Subtract a number of days from a given date.
     *
     * @param date date
     * @param days days to subtract
     * @return the day {@code days} before {@code date}
     */
    public static String minusDays(final String date, final int days) {
        return plusDays(date, -days);
    }

    /**
     * Subtract a number of days from a given date.
     *
     * This does not check that dates are formatted correctly.
     *
     * @param date date
     * @param days days to subtract
     * @return the day {@code days} before {@code date}
     */
    public static String minusDaysQuiet(final String date, final int days) {
        return plusDaysQuiet(date, -days);
    }

    /**
     * Add a number of days from a given date.
     *
     * @param date date
     * @param days days to add
     * @return the day {@code days} after {@code date}
     */
    public static String plusDays(final String date, final int days) {
        if (date == null) {
            return null;
        }

        LocalDate localDate = parseLocalDate(date);
        localDate = localDate.plusDays(days);
        return localDate.toString();
    }

    /**
     * Add a number of days from a given date.
     *
     * This does not check that dates are formatted correctly.
     *
     * @param date date
     * @param days days to add
     * @return the day {@code days} after {@code date}
     */
    public static String plusDaysQuiet(final String date, final int days) {
        if (date == null) {
            return null;
        }

        LocalDate localDate = LocalDate.parse(date);
        localDate = localDate.plusDays(days);
        return localDate.toString();
    }

    /**
     * Is the year a leap year?
     *
     * @param year year
     * @return true if {@code year} is a leap year; false otherwise.
     */
    static boolean isLeapYear(final int year) {
        return ((year % 4 == 0) &&
                (!(year % 100 == 0) || (year % 400) == 0));
    }

    /**
     * Is one date before another?
     *
     * @param date1 if {@code null} return false
     * @param date2 if {@code null} return false
     * @return true if {@code date1} is chronologically before {@code date2}; false otherwise.
     */
    public static boolean isBefore(final String date1, final String date2) {
        if (date1 == null || date2 == null) {
            return false;
        }

        LocalDate localDate1 = parseLocalDate(date1);
        LocalDate localDate2 = parseLocalDate(date2);

        return localDate1.isBefore(localDate2);
    }

    /**
     * Is one date before another?
     *
     * This does not check that dates are formatted correctly. Could be disastrous if {@code date1} and {@code date2}
     * are not ISO-8601 compliant!
     *
     * @param date1 if {@code null} return false
     * @param date2 if {@code null} return false
     * @return true if {@code date1} is chronologically before {@code date2}; false otherwise.
     */
    public static boolean isBeforeQuiet(final String date1, final String date2) {
        return !(date1 == null || date2 == null) && date1.compareTo(date2) < 0;

    }

    /**
     * Is one date after another?
     *
     * @param date1 if {@code null} return false
     * @param date2 if {@code null} return false
     * @return true if {@code date1} is chronologically after {@code date2}; false otherwise.
     */
    public static boolean isAfter(final String date1, final String date2) {
        if (date1 == null || date2 == null) {
            return false;
        }

        LocalDate localDate1 = parseLocalDate(date1);
        LocalDate localDate2 = parseLocalDate(date2);

        return localDate1.isAfter(localDate2);
    }

    /**
     * Is one date after another?
     *
     * This does not check that dates are formatted correctly. Could be disastrous if {@code date1} and {@code date2}
     * are not ISO-8601 compliant!
     *
     * @param date1 if {@code null} return false
     * @param date2 if {@code null} return false
     * @return true if {@code date1} is chronologically after {@code date2}; false otherwise.
     */
    public static boolean isAfterQuiet(final String date1, final String date2) {
        return !(date1 == null || date2 == null) && date1.compareTo(date2) > 0;

    }

    /**
     * Gets the month of the year for the date.
     *
     * @param date date; if {@code null} return {@code NULL_INT}
     * @return month of the year for the date. Jan = 1, Dec = 12
     */
    public static int monthOfYear(final String date) {
        if (date == null) {
            return QueryConstants.NULL_INT;
        }

        return parseLocalDate(date).getMonthValue();
    }

    /**
     * Parses a string as a local date. If the string is not a valid ISO-8601 Date String, throws an exception.
     *
     * This method can beused to verify that a date string is properly formed.
     *
     * @param date date
     * @throws IllegalArgumentException improper date string
     */
    static LocalDate parseLocalDate(final String date) {
        if (date == null) {
            throw new IllegalArgumentException("Date can not be null");
        }

        final boolean matchesPattern = DATE_STRING_PATTERN.matcher(date).matches();
        if (!matchesPattern) {
            throw new IllegalArgumentException(
                    "Text '" + date + "' could not be parsed as a date: format must be yyyy-MM-dd");
        }

        try {
            return LocalDate.parse(date, DATE_STRING_FORMATTER);
        } catch (Exception e) {
            throw new IllegalArgumentException("Text '" + date + "' could not be parsed as a date: " + e.getMessage());
        }
    }

    /**
     * Formats a date as an ISO-8601 Date String (yyyy-MM-dd).
     *
     * @param date date
     * @return ISO-8601 formatted string (yyyy-MM-dd)
     */
    static String format(final LocalDate date) {
        return DATE_STRING_FORMATTER.format(date);
    }
}
