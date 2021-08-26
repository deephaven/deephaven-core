/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.base.StringUtils;
import io.deephaven.base.clock.TimeConstants;
import io.deephaven.base.clock.TimeZones;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.v2.utils.TimeProvider;
import io.deephaven.libs.primitives.LongNumericPrimitives;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.calendar.BusinessCalendar;
import io.deephaven.util.calendar.Calendars;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateMidnight;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.DurationFieldType;

import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities for Deephaven date/time storage and manipulation.
 */
@SuppressWarnings("UnusedDeclaration")
public class DBTimeUtils {

    // The following 3 patterns support LocalDate literals. Note all LocalDate patterns must not
    // have characters after
    // the date, to avoid confusion with DBDateTime literals.

    /** Matches yyyy-MM-dd. */
    private static final Pattern STD_DATE_PATTERN =
        Pattern.compile("^(?<year>[0-9][0-9][0-9][0-9])-(?<month>[0-9][0-9])-(?<day>[0-9][0-9])$");
    /** Matches yyyyMMdd (consistent with ISO dates). */
    private static final Pattern STD_DATE_PATTERN2 =
        Pattern.compile("^(?<year>[0-9][0-9][0-9][0-9])(?<month>[0-9][0-9])(?<day>[0-9][0-9])$");
    /**
     * Matches variations of month/day/year or day/month/year or year/month/day - how this is
     * interpreted depends on the DBTimeUtils.dateStyle system property.
     */
    private static final Pattern SLASH_DATE_PATTERN =
        Pattern.compile(
            "^(?<part1>[0-9]?[0-9](?<part1sub2>[0-9][0-9])?)\\/(?<part2>[0-9]?[0-9])\\/(?<part3>[0-9]?[0-9](?<part3sub2>[0-9][0-9])?)$");

    /** for use when interpreting two digit years (we use Java's rules). */
    private static final DateTimeFormatter TWO_DIGIT_YR_FORMAT = DateTimeFormatter.ofPattern("yy");

    /**
     * for LocalTime literals. Note these must begin with "L" to avoid ambiguity with the older
     * TIME_AND_DURATION_PATTERN
     */
    private static final Pattern LOCAL_TIME_PATTERN =
        Pattern.compile("^L([0-9][0-9]):?([0-9][0-9])?:?([0-9][0-9])?(\\.([0-9]{1,9}))?");

    // DBDateTime literals
    private static final Pattern DATETIME_PATTERN = Pattern.compile(
        "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9](T[0-9][0-9]?:[0-9][0-9](:[0-9][0-9])?(\\.[0-9][0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?)?)? [a-zA-Z]+");
    private static final Pattern JIM_DATETIME_PATTERN = Pattern.compile(
        "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][\\+-][0-9][0-9][0-9][0-9]");
    private static final Pattern JIM_MICROS_DATETIME_PATTERN = Pattern.compile(
        "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9][\\+-][0-9][0-9][0-9][0-9]");
    private static final Pattern TIME_AND_DURATION_PATTERN = Pattern.compile(
        "\\-?([0-9]+T)?([0-9]+):([0-9]+)(:[0-9]+)?(\\.[0-9][0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?)?");
    private static final Pattern PERIOD_PATTERN = Pattern.compile(
        "\\-?([0-9]+[Yy])?([0-9]+[Mm])?([0-9]+[Ww])?([0-9]+[Dd])?(T([0-9]+[Hh])?([0-9]+[Mm])?([0-9]+[Ss])?)?");
    private static final String DATE_COLUMN_PARTITION_FORMAT_STRING = "yyyy-MM-dd";

    private static final boolean ENABLE_MICROTIME_HACK =
        Configuration.getInstance().getBooleanWithDefault("DBTimeUtils.enableMicrotimeHack", false);

    /**
     * Date formatting styles for use in conversion functions such as
     * {@link #convertDateQuiet(String, DateStyle)}.
     */
    public enum DateStyle {
        MDY, DMY, YMD
    }

    private static final DateStyle DATE_STYLE = DateStyle.valueOf(Configuration.getInstance()
        .getStringWithDefault("DBTimeUtils.dateStyle", DateStyle.MDY.name()));

    /**
     * Constant value of one second in nanoseconds.
     */
    public static final long SECOND = 1_000_000_000;

    /**
     * Constant value of one minute in nanoseconds.
     */
    public static final long MINUTE = 60 * SECOND;

    /**
     * Constant value of one hour in nanoseconds.
     */
    public static final long HOUR = 60 * MINUTE;

    /**
     * Constant value of one day in nanoseconds.
     */
    public static final long DAY = 24 * HOUR;

    /**
     * Constant value of one week in nanoseconds.
     */
    public static final long WEEK = 7 * DAY;

    /**
     * Constant value of one year (365 days) in nanoseconds.
     */
    public static final long YEAR = 365 * DAY;

    private static final Pattern CAPTURING_DATETIME_PATTERN = Pattern.compile(
        "(([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9])T?)?(([0-9][0-9]?)(?::([0-9][0-9])(?::([0-9][0-9]))?(?:\\.([0-9][0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?))?)?)?( [a-zA-Z]+)?");

    private enum DateGroupId {
        // Date(1),
        Year(2, ChronoField.YEAR), Month(3, ChronoField.MONTH_OF_YEAR), Day(4,
            ChronoField.DAY_OF_MONTH),
        // Tod(5),
        Hours(6, ChronoField.HOUR_OF_DAY), Minutes(7, ChronoField.MINUTE_OF_HOUR), Seconds(8,
            ChronoField.SECOND_OF_MINUTE), Fraction(9, ChronoField.MILLI_OF_SECOND);

        public final int id;
        public final ChronoField field;

        DateGroupId(int id, ChronoField field) {
            this.id = id;
            this.field = field;
        }
    }

    /**
     * Maximum time in microseconds that can be converted to a {@link DBDateTime} without overflow.
     */
    private static final long MAX_CONVERTIBLE_MICROS = Long.MAX_VALUE / 1_000L;

    /**
     * Maximum time in milliseconds that can be converted to a {@link DBDateTime} without overflow.
     */
    private static final long MAX_CONVERTIBLE_MILLIS = Long.MAX_VALUE / 1_000_000L;

    /**
     * Maximum time in seconds that can be converted to a {@link DBDateTime} without overflow.
     */
    private static final long MAX_CONVERTIBLE_SECONDS = Long.MAX_VALUE / 1_000_000_000L;

    private static final double YEARS_PER_NANO = 1. / (double) YEAR;

    /**
     * Allows setting an alternate date instead of "today" to be returned from
     * {@link #currentDateNy}. This is mainly used when setting up for a replay simulation.
     */
    public static String currentDateNyOverride;

    /**
     * Allows setting an alternate date instead of the business day before "today" to be returned
     * from {@link #lastBusinessDateNy}. This is mainly used when setting up for a replay
     * simulation.
     */
    @SuppressWarnings("WeakerAccess")
    public static String lastBusinessDayNyOverride;

    /**
     * Allows setting a custom time provider instead of actual current time. This is mainly used
     * when setting up for a replay simulation.
     */
    public static TimeProvider timeProvider;

    /**
     * Returns milliseconds since Epoch for a {@link DBDateTime} value.
     * 
     * @param dateTime The {@link DBDateTime} for which the milliseconds offset should be returned.
     * @return A long value of milliseconds since Epoch, or a {@link QueryConstants#NULL_LONG} value
     *         if the {@link DBDateTime} is null.
     */
    public static long millis(DBDateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }

        return dateTime.getMillis();
    }

    /**
     * Returns nanoseconds since Epoch for a {@link DBDateTime} value.
     * 
     * @param dateTime The {@link DBDateTime} for which the nanoseconds offset should be returned.
     * @return A long value of nanoseconds since Epoch, or a NULL_LONG value if the
     *         {@link DBDateTime} is null.
     */
    public static long nanos(DBDateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }

        return dateTime.getNanos();
    }

    public static long nanos(Instant instant) {
        if (instant == null) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }
        return Math.addExact(TimeUnit.SECONDS.toNanos(instant.getEpochSecond()), instant.getNano());
    }

    /**
     * Evaluates whether one {@link DBDateTime} value is earlier than a second {@link DBDateTime}
     * value.
     * 
     * @param d1 The first {@link DBDateTime} value to compare.
     * @param d2 The second {@link DBDateTime} value to compare.
     * @return Boolean true if d1 is earlier than d2, false if either value is null, or if d2 is
     *         equal to or earlier than d1.
     */
    public static boolean isBefore(DBDateTime d1, DBDateTime d2) {
        if (d1 == null || d2 == null) {
            return false;
        }

        return d1.getNanos() < d2.getNanos();
    }

    /**
     * Evaluates whether one {@link DBDateTime} value is later than a second {@link DBDateTime}
     * value.
     * 
     * @param d1 The first {@link DBDateTime} value to compare.
     * @param d2 The second {@link DBDateTime} value to compare.
     * @return Boolean true if d1 is later than d2, false if either value is null, or if d2 is equal
     *         to or later than d1.
     */
    public static boolean isAfter(DBDateTime d1, DBDateTime d2) {
        if (d1 == null || d2 == null) {
            return false;
        }

        return d1.getNanos() > d2.getNanos();
    }

    /**
     * Adds one time from another.
     *
     * @param dateTime The starting {@link DBDateTime} value.
     * @param nanos The long number of nanoseconds to add to dateTime.
     * @return a null {@link DBDateTime} if either input is null; the starting {@link DBDateTime}
     *         plus the specified number of nanoseconds, if the result is not too large for a
     *         {@link DBDateTime}; or throws a {@link DBDateTimeOverflowException
     *         DBDateTimeOverflowException} if the resultant value is more than max long nanoseconds
     *         from Epoch.
     */
    public static DBDateTime plus(DBDateTime dateTime, long nanos) {
        if (dateTime == null || nanos == io.deephaven.util.QueryConstants.NULL_LONG) {
            return null;
        }

        return new DBDateTime(checkOverflowPlus(dateTime.getNanos(), nanos, false));
    }

    /**
     * Subtracts one time from another.
     *
     * @param dateTime The starting {@link DBDateTime} value.
     * @param nanos The long number of nanoseconds to subtract from dateTime.
     * @return a null {@link DBDateTime} if either input is null; the starting {@link DBDateTime}
     *         minus the specified number of nanoseconds, if the result is not too negative for a
     *         {@link DBDateTime}; or throws a {@link DBDateTimeOverflowException
     *         DBDateTimeOverflowException} if the resultant value is more than min long nanoseconds
     *         from Epoch.
     */
    public static DBDateTime minus(DBDateTime dateTime, long nanos) {
        if (dateTime == null || -nanos == io.deephaven.util.QueryConstants.NULL_LONG) {
            return null;
        }

        return new DBDateTime(checkUnderflowMinus(dateTime.getNanos(), nanos, true));
    }

    /**
     * Adds one time from another.
     *
     * @param dateTime The starting {@link DBDateTime} value.
     * @param period The {@link DBPeriod} to add to dateTime.
     * @return a null {@link DBDateTime} if either input is null; the starting {@link DBDateTime}
     *         plus the specified period, if the result is not too large for a DBDateTime; or throws
     *         a {@link DBDateTimeOverflowException DBDateTimeOverflowException} if the resultant
     *         value is more than max long nanoseconds from Epoch.
     */
    public static DBDateTime plus(DBDateTime dateTime, DBPeriod period) {
        if (dateTime == null || period == null) {
            return null;
        }

        if (period.isPositive()) {
            return new DBDateTime(
                millisToNanos(dateTime.getJodaDateTime().plus(period.getJodaPeriod()).getMillis())
                    + dateTime.getNanosPartial());
        } else {
            return new DBDateTime(
                millisToNanos(dateTime.getJodaDateTime().minus(period.getJodaPeriod()).getMillis())
                    + dateTime.getNanosPartial());
        }
    }

    /**
     * Subtracts one time from another.
     *
     * @param dateTime The starting {@link DBDateTime} value.
     * @param period The {@link DBPeriod} to subtract from dateTime.
     * @return a null {@link DBDateTime} if either input is null; the starting {@link DBDateTime}
     *         minus the specified period, if the result is not too negative for a
     *         {@link DBDateTime}; or throws a {@link DBDateTimeOverflowException
     *         DBDateTimeOverflowException} if the resultant value is more than min long nanoseconds
     *         from Epoch.
     */
    public static DBDateTime minus(DBDateTime dateTime, DBPeriod period) {
        if (dateTime == null || period == null) {
            return null;
        }

        if (period.isPositive()) {
            return new DBDateTime(
                millisToNanos(dateTime.getJodaDateTime().minus(period.getJodaPeriod()).getMillis())
                    + dateTime.getNanosPartial());
        } else {
            return new DBDateTime(
                millisToNanos(dateTime.getJodaDateTime().plus(period.getJodaPeriod()).getMillis())
                    + dateTime.getNanosPartial());
        }
    }

    /**
     * Subtracts one time from another.
     *
     * @param d1 The first {@link DBDateTime}.
     * @param d2 The {@link DBDateTime} to subtract from d1.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; the long nanoseconds from
     *         Epoch value of the first {@link DBDateTime} minus d2, if the result is not out of
     *         range for a long value; or throws a {@link DBDateTimeOverflowException
     *         DBDateTimeOverflowException} if the resultant value would be more than min long or
     *         max long nanoseconds from Epoch.
     *         <P>
     *         Note that the subtraction is done based the nanosecond offsets of the two dates from
     *         Epoch, so, if either date is before Epoch (negative offset), the result may be
     *         unexpected.
     *         </P>
     */
    public static long minus(DBDateTime d1, DBDateTime d2) {
        if (d1 == null || d2 == null) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }

        return checkUnderflowMinus(d1.getNanos(), d2.getNanos(), true);
    }

    @Deprecated
    public static long diff(DBDateTime d1, DBDateTime d2) {
        return diffNanos(d1, d2);
    }

    @Deprecated
    public static double yearDiff(DBDateTime start, DBDateTime end) {
        return diffYear(start, end);
    }

    @Deprecated
    public static double dayDiff(DBDateTime start, DBDateTime end) {
        return diffDay(start, end);
    }

    /**
     * Returns the difference in nanoseconds between two {@link DBDateTime} values.
     * 
     * @param d1 The first {@link DBDateTime}.
     * @param d2 The second {@link DBDateTime}.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; the long nanoseconds from
     *         Epoch value of the first {@link DBDateTime} minus d2, if the result is not out of
     *         range for a long value; or throws a {@link DBDateTimeOverflowException
     *         DBDateTimeOverflowException} if the resultant value would be more than min long or
     *         max long nanoseconds from Epoch.
     *         <P>
     *         Note that the subtraction is done based the nanosecond offsets of the two dates from
     *         Epoch, so, if either date is before Epoch (negative offset), the result may be
     *         unexpected.
     *         </P>
     *         If the second value is greater than the first value, the result will be negative.
     */
    @SuppressWarnings("WeakerAccess")
    public static long diffNanos(DBDateTime d1, DBDateTime d2) {
        return minus(d2, d1);
    }

    /**
     * Returns a double value of the number of 365 day units difference between two
     * {@link DBDateTime} values.
     * 
     * @param start The first {@link DBDateTime}.
     * @param end The second {@link DBDateTime}.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; a double value of the
     *         number of 365 day periods obtained from the first {@link DBDateTime} value minus d2,
     *         if the intermediate value of nanoseconds difference between the two dates is not out
     *         of range for a long value; or throws a {@link DBDateTimeOverflowException} if the
     *         intermediate value would be more than min long or max long nanoseconds from Epoch.
     *         <P>
     *         Note that the subtraction is done based the nanosecond offsets of the two dates from
     *         Epoch, so, if either date is before Epoch (negative offset), the result may be
     *         unexpected.
     *         </P>
     *         If the second value is greater than the first value, the result will be negative.
     */
    public static double diffYear(DBDateTime start, DBDateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) * YEARS_PER_NANO;
    }

    /**
     * Returns a double value of the number of days difference between two {@link DBDateTime}
     * values.
     * 
     * @param start The first {@link DBDateTime}.
     * @param end The second {@link DBDateTime}.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; a double value of the
     *         number of days obtained from the first {@link DBDateTime} value minus d2, if the
     *         intermediate value of nanoseconds difference between the two dates is not out of
     *         range for a long value; or throws a {@link DBDateTimeOverflowException
     *         DBDateTimeOverflowException} if the intermediate value would be more than min long or
     *         max long nanoseconds from Epoch.
     *         <P>
     *         Note that the subtraction is done based the nanosecond offsets of the two dates from
     *         Epoch, so, if either date is before Epoch (negative offset), the result may be
     *         unexpected.
     *         </P>
     *         If the second value is greater than the first value, the result will be negative.
     */
    @SuppressWarnings("WeakerAccess")
    public static double diffDay(DBDateTime start, DBDateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / DAY;
    }

    /**
     * Returns a {@link DBDateTime} for the requested {@link DBDateTime} at midnight in the
     * specified time zone.
     * 
     * @param dateTime {@link DBDateTime} for which the new value at midnight should be calculated.
     * @param timeZone {@link DBTimeZone} for which the new value at midnight should be calculated.
     * @return A null {@link DBDateTime} if either input is null, otherwise a {@link DBDateTime}
     *         representing midnight for the date and time zone of the inputs.
     */
    public static DBDateTime dateAtMidnight(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return null;
        }

        return new DBDateTime(millisToNanos(
            new DateMidnight(dateTime.getMillis(), timeZone.getTimeZone()).getMillis())
            + dateTime.getNanosPartial());
    }

    /**
     * Returns a {@link DBDateTime} representing midnight in New York time zone on the date
     * specified by the a number of milliseconds from Epoch.
     * 
     * @param millis A long value of the number of milliseconds from Epoch for which the
     *        {@link DBDateTime} is to be calculated.
     * @return A {@link DBDateTime} rounded down to midnight in the New York time zone for the
     *         specified number of milliseconds from Epoch.
     */
    @SuppressWarnings("WeakerAccess")
    public static DBDateTime millisToDateAtMidnightNy(final long millis) {
        return millisToDateAtMidnight(millis, DBTimeZone.TZ_NY);
    }

    /**
     * Returns a {@link DBDateTime} representing midnight in a selected time zone on the date
     * specified by the a number of milliseconds from Epoch.
     * 
     * @param millis A long value of the number of milliseconds from Epoch for which the
     *        {@link DBDateTime} is to be calculated.
     * @param timeZone {@link DBTimeZone} for which the new value at midnight should be calculated.
     * @return A {@link DBDateTime} rounded down to midnight in the selected time zone for the
     *         specified number of milliseconds from Epoch.
     */
    @SuppressWarnings("WeakerAccess")
    public static DBDateTime millisToDateAtMidnight(final long millis, final DBTimeZone timeZone) {
        if (millis == io.deephaven.util.QueryConstants.NULL_LONG) {
            return null;
        }

        return new DBDateTime(
            millisToNanos(new DateMidnight(millis, timeZone.getTimeZone()).getMillis()));
    }

    /**
     * Returns a String date/time representation.
     *
     * @param dateTime The {@link DBDateTime} to format as a String.
     * @param timeZone The {@link DBTimeZone} to use when formatting the String.
     * @return A null String if either input is null, otherwise a String formatted as
     *         yyyy-MM-ddThh:mm:ss.nnnnnnnnn TZ.
     */
    public static String format(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return null;
        }

        return dateTime.toString(timeZone);
    }

    /**
     * Returns a String date/time representation of a {@link DBDateTime} interpreted for the New
     * York time zone.
     *
     * @param dateTime The {@link DBDateTime} to format as a String.
     * @return A null String if the input is null, otherwise a String formatted as
     *         yyyy-MM-ddThh:mm:ss.nnnnnnnnn NY.
     */
    public static String formatNy(DBDateTime dateTime) {
        return format(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns a String date representation of a {@link DBDateTime} interpreted for a specified time
     * zone.
     *
     * @param dateTime The {@link DBDateTime} to format as a String.
     * @param timeZone The {@link DBTimeZone} to use when formatting the String.
     * @return A null String if either input is null, otherwise a String formatted as yyyy-MM-dd.
     */
    @SuppressWarnings("WeakerAccess")
    public static String formatDate(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return null;
        }

        return dateTime.toDateString(timeZone);
    }

    /**
     * Returns a String date representation of a {@link DBDateTime} interpreted for the New York
     * time zone.
     *
     * @param dateTime The {@link DBDateTime} to format as a String.
     * @return A null String if the input is null, otherwise a String formatted as yyyy-MM-dd.
     */
    public static String formatDateNy(DBDateTime dateTime) {
        return formatDate(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns a String date/time representation.
     *
     * @param nanos The long number of nanoseconds offset from Epoch.
     * @return A String of varying format depending on the offset.
     *         <p>
     *         For values greater than one day, the output will start with dddT
     *         </p>
     *         <p>
     *         For values with fractional seconds, the output will be trailed by .nnnnnnnnn
     *         </p>
     *         <p>
     *         e.g. output may be dddThh:mm:ss.nnnnnnnnn or subsets of this.
     *         </p>
     */
    public static String format(long nanos) {
        StringBuilder buf = new StringBuilder(25);

        if (nanos < 0) {
            buf.append('-');
            nanos = -nanos;
        }

        int days = (int) (nanos / 86400000000000L);

        nanos %= 86400000000000L;

        int hours = (int) (nanos / 3600000000000L);

        nanos %= 3600000000000L;

        int minutes = (int) (nanos / 60000000000L);

        nanos %= 60000000000L;

        int seconds = (int) (nanos / 1000000000L);

        nanos %= 1000000000L;

        if (days != 0) {
            buf.append(days).append('T');
        }

        buf.append(hours).append(':').append(StringUtils.pad(String.valueOf(minutes), 2, '0'))
            .append(':').append(StringUtils.pad(String.valueOf(seconds), 2, '0'));

        if (nanos != 0) {
            buf.append('.').append(StringUtils.pad(String.valueOf(nanos), 9, '0'));
        }

        return buf.toString();
    }

    /**
     * Returns an int value of the day of the month for a {@link DBDateTime} and specified time
     * zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the day of the month.
     * @param timeZone The {@link DBTimeZone} to use when interpreting the date/time.
     * @return A {@link QueryConstants#NULL_INT} if either input is null, otherwise, an int value of
     *         the day of the month represented by the {@link DBDateTime} when interpreted in the
     *         specified time zone.
     */
    @SuppressWarnings("WeakerAccess")
    public static int dayOfMonth(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getDayOfMonth();
    }

    /**
     * Returns an int value of the day of the month for a {@link DBDateTime} in the New York time
     * zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the day of the month.
     * @return A {@link QueryConstants#NULL_INT} if the input is null, otherwise, an int value of
     *         the day of the month represented by the {@link DBDateTime} when interpreted in the
     *         New York time zone.
     */
    public static int dayOfMonthNy(DBDateTime dateTime) {
        return dayOfMonth(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns an int value of the day of the week for a {@link DBDateTime} in the specified time
     * zone, with 1 being Monday and 7 being Sunday.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the day of the week.
     * @param timeZone The {@link DBTimeZone} to use when interpreting the date/time.
     * @return A {@link QueryConstants#NULL_INT} if either input is null, otherwise, an int value of
     *         the day of the week represented by the {@link DBDateTime} when interpreted in the
     *         specified time zone.
     */
    public static int dayOfWeek(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getDayOfWeek();
    }

    /**
     * Returns an int value of the day of the week for a {@link DBDateTime} in the New York time
     * zone, with 1 being Monday and 7 being Sunday.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the day of the week.
     * @return A {@link QueryConstants#NULL_INT} if the input is null, otherwise, an int value of
     *         the day of the week represented by the {@link DBDateTime} when interpreted in the New
     *         York time zone.
     */
    public static int dayOfWeekNy(DBDateTime dateTime) {
        return dayOfWeek(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns an int value of the day of the year (Julian date) for a {@link DBDateTime} in the
     * specified time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the day of the year.
     * @param timeZone The {@link DBTimeZone} to use when interpreting the date/time.
     * @return A {@link QueryConstants#NULL_INT} if either input is null, otherwise, an int value of
     *         the day of the year represented by the {@link DBDateTime} when interpreted in the
     *         specified time zone.
     */
    public static int dayOfYear(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getDayOfYear();
    }

    /**
     * Returns an int value of the day of the year (Julian date) for a {@link DBDateTime} in the New
     * York time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the day of the year.
     * @return A {@link QueryConstants#NULL_INT} if the input is null, otherwise, an int value of
     *         the day of the year represented by the {@link DBDateTime} when interpreted in the New
     *         York time zone.
     */
    public static int dayOfYearNy(DBDateTime dateTime) {
        return dayOfYear(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns an int value of the hour of the day for a {@link DBDateTime} in the specified time
     * zone. The hour is on a 24 hour clock (0 - 23).
     * 
     * @param dateTime The {@link DBDateTime} for which to find the hour of the day.
     * @param timeZone The {@link DBTimeZone} to use when interpreting the date/time.
     * @return A {@link QueryConstants#NULL_INT} if either input is null, otherwise, an int value of
     *         the hour of the day represented by the {@link DBDateTime} when interpreted in the
     *         specified time zone.
     */
    @SuppressWarnings("WeakerAccess")
    public static int hourOfDay(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getHourOfDay();
    }

    /**
     * Returns an int value of the hour of the day for a {@link DBDateTime} in the New York time
     * zone. The hour is on a 24 hour clock (0 - 23).
     * 
     * @param dateTime The {@link DBDateTime} for which to find the hour of the day.
     * @return A {@link QueryConstants#NULL_INT} if the input is null, otherwise, an int value of
     *         the hour of the day represented by the {@link DBDateTime} when interpreted in the New
     *         York time zone.
     */
    public static int hourOfDayNy(DBDateTime dateTime) {
        return hourOfDay(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns an int value of milliseconds since midnight for a {@link DBDateTime} in the specified
     * time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the milliseconds since midnight.
     * @param timeZone The {@link DBTimeZone} to use when interpreting the date/time.
     * @return A {@link QueryConstants#NULL_INT} if either input is null, otherwise, an int value of
     *         milliseconds since midnight for the date/time represented by the {@link DBDateTime}
     *         when interpreted in the specified time zone.
     */
    @SuppressWarnings("WeakerAccess")
    public static int millisOfDay(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getMillisOfDay();
    }

    /**
     * Returns an int value of milliseconds since midnight for a {@link DBDateTime} in the New York
     * time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the milliseconds since midnight.
     * @return A {@link QueryConstants#NULL_INT} if the input is null, otherwise, an int value of
     *         milliseconds since midnight for the date/time represented by the {@link DBDateTime}
     *         when interpreted in the New York time zone.
     */
    public static int millisOfDayNy(DBDateTime dateTime) {
        return millisOfDay(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns an int value of milliseconds since the top of the second for a {@link DBDateTime} in
     * the specified time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the milliseconds.
     * @param timeZone The {@link DBTimeZone} to use when interpreting the date/time.
     * @return A {@link QueryConstants#NULL_INT} if either input is null, otherwise, an int value of
     *         milliseconds since the top of the second for the date/time represented by the
     *         {@link DBDateTime} when interpreted in the specified time zone.
     */
    @SuppressWarnings("WeakerAccess")
    public static int millisOfSecond(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getMillisOfSecond();
    }

    /**
     * Returns an int value of milliseconds since the top of the second for a {@link DBDateTime} in
     * the New York time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the milliseconds.
     * @return A {@link QueryConstants#NULL_INT} if the input is null, otherwise, an int value of
     *         milliseconds since the top of the second for the date/time represented by the
     *         {@link DBDateTime} when interpreted in the New York time zone.
     */
    public static int millisOfSecondNy(DBDateTime dateTime) {
        return millisOfSecond(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns a long value of nanoseconds since midnight for a {@link DBDateTime} in the specified
     * time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the nanoseconds since midnight.
     * @param timeZone The {@link DBTimeZone} to use when interpreting the date/time.
     * @return A {@link QueryConstants#NULL_LONG} if either input is null, otherwise, a long value
     *         of nanoseconds since midnight for the date/time represented by the {@link DBDateTime}
     *         when interpreted in the specified time zone.
     */
    @SuppressWarnings("WeakerAccess")
    public static long nanosOfDay(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }

        return millisToNanos(dateTime.getJodaDateTime(timeZone).getMillisOfDay())
            + dateTime.getNanosPartial();
    }

    /**
     * Returns a long value of nanoseconds since midnight for a {@link DBDateTime} in the New York
     * time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the nanoseconds since midnight.
     * @return A {@link QueryConstants#NULL_LONG} if the input is null, otherwise, a long value of
     *         nanoseconds since midnight for the date/time represented by the {@link DBDateTime}
     *         when interpreted in the New York time zone.
     */
    public static long nanosOfDayNy(DBDateTime dateTime) {
        return nanosOfDay(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns a long value of nanoseconds since the top of the second for a {@link DBDateTime} in
     * the specified time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the nanoseconds.
     * @param timeZone The {@link DBTimeZone} to use when interpreting the date/time.
     * @return A {@link QueryConstants#NULL_LONG} if either input is null, otherwise, a long value
     *         of nanoseconds since the top of the second for the date/time represented by the
     *         {@link DBDateTime} when interpreted in the specified time zone.
     */
    @SuppressWarnings("WeakerAccess")
    public static long nanosOfSecond(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }

        return millisToNanos(dateTime.getJodaDateTime(timeZone).getMillisOfSecond())
            + dateTime.getNanosPartial();
    }

    /**
     * Returns a long value of nanoseconds since the top of the second for a {@link DBDateTime} in
     * the New York time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the nanoseconds.
     * @return A {@link QueryConstants#NULL_LONG} if the input is null, otherwise, a long value of
     *         nanoseconds since the top of the second for the date/time represented by the
     *         {@link DBDateTime} when interpreted in the New York time zone.
     */
    public static long nanosOfSecondNy(DBDateTime dateTime) {
        return nanosOfSecond(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns the number of microseconds that have elapsed since the start of the millisecond
     * represented by the provided {@code dateTime} in the specified time zone. Nanoseconds are
     * rounded, not dropped -- '20:41:39.123456700' has 457 micros, not 456.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the microseconds.
     * @param timeZone The {@link DBTimeZone} to use when interpreting the date/time.
     * @return A {@link QueryConstants#NULL_INT} if either input is null, otherwise, an int value of
     *         microseconds since the top of the millisecond for the date/time represented by the
     *         {@link DBDateTime} when interpreted in the specified time zone.
     */
    @SuppressWarnings("WeakerAccess")
    public static int microsOfMilli(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return (int) Math.round(dateTime.getNanosPartial() / 1000d);
    }

    /**
     * Returns the number of microseconds that have elapsed since the start of the millisecond
     * represented by the provided {@code dateTime} in the New York time zone. Nanoseconds are
     * rounded, not dropped -- '20:41:39.123456700' has 457 micros, not 456.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the microseconds.
     * @return A {@link QueryConstants#NULL_INT} if the input is null, otherwise, an int value of
     *         microseconds since the top of the millisecond for the date/time represented by the
     *         {@link DBDateTime} when interpreted in the New York time zone.
     */
    @SuppressWarnings("WeakerAccess")
    public static int microsOfMilliNy(DBDateTime dateTime) {
        return microsOfMilli(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns an int value of minutes since midnight for a {@link DBDateTime} in the specified time
     * zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the minutes.
     * @param timeZone The {@link DBTimeZone} to use when interpreting the date/time.
     * @return A {@link QueryConstants#NULL_INT} if either input is null, otherwise, an int value of
     *         minutes since midnight for the date/time represented by the {@link DBDateTime} when
     *         interpreted in the specified time zone.
     */
    @SuppressWarnings("WeakerAccess")
    public static int minuteOfDay(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getMinuteOfDay();
    }

    /**
     * Returns an int value of minutes since midnight for a {@link DBDateTime} in the New York time
     * zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the milliseconds since midnight.
     * @return A {@link QueryConstants#NULL_INT} if the input is null, otherwise, an int value of
     *         minutes since midnight for the date/time represented by the {@link DBDateTime} when
     *         interpreted in the New York time zone.
     */
    public static int minuteOfDayNy(DBDateTime dateTime) {
        return minuteOfDay(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns an int value of minutes since the top of the hour for a {@link DBDateTime} in the
     * specified time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the minutes.
     * @param timeZone The {@link DBTimeZone} to use when interpreting the date/time.
     * @return A {@link QueryConstants#NULL_INT} if either input is null, otherwise, an int value of
     *         minutes since the top of the hour for the date/time represented by the
     *         {@link DBDateTime} when interpreted in the specified time zone.
     */
    @SuppressWarnings("WeakerAccess")
    public static int minuteOfHour(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getMinuteOfHour();
    }

    /**
     * Returns an int value of minutes since the top of the hour for a {@link DBDateTime} in the New
     * York time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the minutes.
     * @return A {@link QueryConstants#NULL_INT} if the input is null, otherwise, an int value of
     *         minutes since the top of the hour for the date/time represented by the
     *         {@link DBDateTime} when interpreted in the New York time zone.
     */
    public static int minuteOfHourNy(DBDateTime dateTime) {
        return minuteOfHour(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns an int value for the month of a {@link DBDateTime} in the specified time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the month.
     * @param timeZone The {@link DBTimeZone} to use when interpreting the date/time.
     * @return A {@link QueryConstants#NULL_INT} if either input is null, otherwise, an int value of
     *         the month for the date/time represented by the {@link DBDateTime} when interpreted in
     *         the specified time zone. January is 1, February is 2, etc.
     */
    @SuppressWarnings("WeakerAccess")
    public static int monthOfYear(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getMonthOfYear();
    }

    /**
     * Returns an int value for the month of a {@link DBDateTime} in the New York time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the month.
     * @return A {@link QueryConstants#NULL_INT} if the input is null, otherwise, an int value of
     *         the month for the date/time represented by the {@link DBDateTime} when interpreted in
     *         the New York time zone.
     */
    public static int monthOfYearNy(DBDateTime dateTime) {
        return monthOfYear(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns an int value of seconds since midnight for a {@link DBDateTime} in the specified time
     * zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the seconds.
     * @param timeZone The {@link DBTimeZone} to use when interpreting the date/time.
     * @return A {@link QueryConstants#NULL_INT} if either input is null, otherwise, an int value of
     *         seconds since midnight for the date/time represented by the {@link DBDateTime} when
     *         interpreted in the specified time zone.
     */
    @SuppressWarnings("WeakerAccess")
    public static int secondOfDay(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getSecondOfDay();
    }

    /**
     * Returns an int value of seconds since midnight for a {@link DBDateTime} in the New York time
     * zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the seconds.
     * @return A {@link QueryConstants#NULL_INT} if either input is null, otherwise, an int value of
     *         seconds since midnight for the date/time represented by the {@link DBDateTime} when
     *         interpreted in the New York time zone.
     */
    public static int secondOfDayNy(DBDateTime dateTime) {
        return secondOfDay(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns an int value of seconds since the top of the minute for a {@link DBDateTime} in the
     * specified time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the seconds.
     * @param timeZone The {@link DBTimeZone} to use when interpreting the date/time.
     * @return A {@link QueryConstants#NULL_INT} if either input is null, otherwise, an int value of
     *         seconds since the top of the minute for the date/time represented by the
     *         {@link DBDateTime} when interpreted in the specified time zone.
     */
    @SuppressWarnings("WeakerAccess")
    public static int secondOfMinute(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getSecondOfMinute();
    }

    /**
     * Returns an int value of seconds since the top of the minute for a {@link DBDateTime} in the
     * New York time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the seconds.
     * @return A {@link QueryConstants#NULL_INT} if the input is null, otherwise, an int value of
     *         seconds since the top of the minute for the date/time represented by the
     *         {@link DBDateTime} when interpreted in the New York time zone.
     */
    public static int secondOfMinuteNy(DBDateTime dateTime) {
        return secondOfMinute(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns an int value of the year for a {@link DBDateTime} in the specified time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the year.
     * @param timeZone The {@link DBTimeZone} to use when interpreting the date/time.
     * @return A {@link QueryConstants#NULL_INT} if either input is null, otherwise, an int value of
     *         the year for the date/time represented by the {@link DBDateTime} when interpreted in
     *         the specified time zone.
     */
    public static int year(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getYear();
    }

    /**
     * Returns an int value of the year for a {@link DBDateTime} in the New York time zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the year.
     * @return A {@link QueryConstants#NULL_INT} if either input is null, otherwise, an int value of
     *         the year for the date/time represented by the {@link DBDateTime} when interpreted in
     *         the New York time zone.
     */
    @SuppressWarnings("WeakerAccess")
    public static int yearNy(DBDateTime dateTime) {
        return year(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns an int value of the two-digit year for a {@link DBDateTime} in the specified time
     * zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the year.
     * @param timeZone The {@link DBTimeZone} to use when interpreting the date/time.
     * @return A {@link QueryConstants#NULL_INT} if either input is null, otherwise, an int value of
     *         the two-digit year for the date/time represented by the {@link DBDateTime} when
     *         interpreted in the specified time zone.
     */
    @SuppressWarnings("WeakerAccess")
    public static int yearOfCentury(DBDateTime dateTime, DBTimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getYearOfCentury();
    }

    /**
     * Returns an int value of the two-digit year for a {@link DBDateTime} in the New York time
     * zone.
     * 
     * @param dateTime The {@link DBDateTime} for which to find the year.
     * @return A {@link QueryConstants#NULL_INT} if either input is null, otherwise, an int value of
     *         the two-digit year for the date/time represented by the {@link DBDateTime} when
     *         interpreted in the New York time zone.
     */
    public static int yearOfCenturyNy(DBDateTime dateTime) {
        return yearOfCentury(dateTime, DBTimeZone.TZ_NY);
    }

    /**
     * Returns the Excel double time format representation of a {@link DBDateTime}.
     *
     * @param dateTime The {@link DBDateTime} to convert.
     * @param timeZone The {@link DBTimeZone} to use when interpreting the date/time.
     * @return 0.0 if either input is null, otherwise, a double value containing the Excel double
     *         format representation of a {@link DBDateTime} in the specified time zone.
     */
    @SuppressWarnings("WeakerAccess")
    public static double getExcelDateTime(DBDateTime dateTime, DBTimeZone timeZone) {
        return getExcelDateTime(dateTime, timeZone.getTimeZone().toTimeZone());
    }

    /**
     * Returns the Excel double time format representation of a {@link DBDateTime}.
     *
     * @param dateTime The {@link DBDateTime} to convert.
     * @param timeZone The {@link TimeZone} to use when interpreting the date/time.
     * @return 0.0 if either input is null, otherwise, a double value containing the Excel double
     *         format representation of a {@link DBDateTime} in the specified time zone.
     */
    @SuppressWarnings("WeakerAccess")
    public static double getExcelDateTime(DBDateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return 0.0d;
        }
        long millis = dateTime.getMillis();

        return (double) (millis + timeZone.getOffset(millis)) / 86400000 + 25569;
    }

    /**
     * Returns the Excel double time format representation of a {@link DBDateTime}.
     *
     * @param dateTime The {@link DBDateTime} to convert.
     * @return 0.0 if the input is null, otherwise, a double value containing the Excel double
     *         format representation of a {@link DBDateTime} in the New York time zone.
     */
    @SuppressWarnings("WeakerAccess")
    public static double getExcelDateTime(DBDateTime dateTime) {
        return getExcelDateTime(dateTime, TimeZones.TZ_NEWYORK);
    }

    /**
     * Converts microseconds to nanoseconds.
     * 
     * @param micros The long value of microseconds to convert.
     * @return A {@link QueryConstants#NULL_LONG} if the input is null. Throws a
     *         {@link DBDateTimeOverflowException} if the resultant value would exceed the range
     *         that can be stored in a long. Otherwise, returns a long containing the equivalent
     *         number of nanoseconds for the input in microseconds.
     */
    public static long microsToNanos(long micros) {
        if (micros == io.deephaven.util.QueryConstants.NULL_LONG) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }
        if (Math.abs(micros) > MAX_CONVERTIBLE_MICROS) {
            throw new DBDateTimeOverflowException(
                "Converting " + micros + " micros to nanos would overflow");
        }
        return micros * 1000;
    }

    /**
     * Converts nanoseconds to microseconds.
     * 
     * @param nanos The long value of nanoseconds to convert.
     * @return A {@link QueryConstants#NULL_LONG} if the input is null. Otherwise, returns a long
     *         containing the equivalent number of microseconds for the input in nanoseconds.
     */
    @SuppressWarnings("WeakerAccess")
    public static long nanosToMicros(long nanos) {
        if (nanos == io.deephaven.util.QueryConstants.NULL_LONG) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }
        return nanos / 1000;
    }

    /**
     * Converts a value of microseconds from Epoch in the UTC time zone to a {@link DBDateTime}.
     * 
     * @param micros The long microseconds value to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is null, otherwise, a
     *         {@link DBDateTime} representation of the input.
     */
    public static DBDateTime microsToTime(long micros) {
        return nanosToTime(microsToNanos(micros));
    }

    /**
     * Converts milliseconds to nanoseconds.
     * 
     * @param millis The long milliseconds value to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is equal to
     *         {@link QueryConstants#NULL_LONG}. Throws {@link DBDateTimeOverflowException} if the
     *         input is too large for conversion. Otherwise returns a long of the equivalent number
     *         of nanoseconds to the input.
     */
    public static long millisToNanos(long millis) {
        if (millis == io.deephaven.util.QueryConstants.NULL_LONG) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }
        if (ENABLE_MICROTIME_HACK) {
            // hack hack, check to see if this is actually microtime
            if (millis > TimeConstants.MICROTIME_THRESHOLD) {
                return millis * 1000;
            }
        } else if (Math.abs(millis) > MAX_CONVERTIBLE_MILLIS) {
            throw new DBDateTimeOverflowException(
                "Converting " + millis + " millis to nanos would overflow");
        }
        return millis * 1000000;
    }

    /**
     * Converts seconds to nanoseconds.
     * 
     * @param seconds The long value of seconds to convert.
     * @return A {@link QueryConstants#NULL_LONG} if the input is null. Throws a
     *         {@link DBDateTimeOverflowException} if the resultant value would exceed the range
     *         that can be stored in a long. Otherwise, returns a long containing the equivalent
     *         number of nanoseconds for the input in seconds.
     */
    public static long secondsToNanos(long seconds) {
        if (seconds == io.deephaven.util.QueryConstants.NULL_LONG) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }
        if (Math.abs(seconds) > MAX_CONVERTIBLE_SECONDS) {
            throw new DBDateTimeOverflowException(
                "Converting " + seconds + " seconds to nanos would overflow");
        }

        return seconds * 1000000000L;
    }

    /**
     * Converts nanoseconds to milliseconds.
     * 
     * @param nanos The long value of nanoseconds to convert.
     * @return A {@link QueryConstants#NULL_LONG} if the input is null. Otherwise, returns a long
     *         containing the equivalent number of milliseconds for the input in nanoseconds.
     */
    public static long nanosToMillis(long nanos) {
        if (nanos == io.deephaven.util.QueryConstants.NULL_LONG) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }

        return nanos / 1000000;
    }

    /**
     * Converts a value of milliseconds from Epoch in the UTC time zone to a {@link DBDateTime}.
     * 
     * @param millis The long milliseconds value to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is null, otherwise, a
     *         {@link DBDateTime} representation of the input.
     */
    public static DBDateTime millisToTime(long millis) {
        return nanosToTime(millisToNanos(millis));
    }

    /**
     * Converts a value of seconds from Epoch in the UTC time zone to a {@link DBDateTime}.
     * 
     * @param seconds The long seconds value to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is null, otherwise, a
     *         {@link DBDateTime} representation of the input.
     */
    public static DBDateTime secondsToTime(long seconds) {
        return nanosToTime(secondsToNanos(seconds));
    }

    /**
     * Provides the current date/time, or, if a custom {@link #timeProvider} has been configured,
     * provides the current time according to the custom provider.
     * 
     * @return A {@link DBDateTime} of the current date and time from the system or from the
     *         configured alternate time provider.
     */
    public static DBDateTime currentTime() {
        if (timeProvider != null) {
            return timeProvider.currentTime();
        }
        return nanosToTime(millisToNanos(System.currentTimeMillis()));
    }

    // TODO: Revoke public access to these fields and retire them! Use getCurrentDate(), maybe hold
    // on to the CachedCurrentDate to skip a map lookup.
    public static String currentDateNy = null;

    public static long endOfCurrentDateNy = 0;

    /**
     * Provides a String representing the current date in the New York time zone or, if a custom
     * {@link #currentDateNyOverride} has been set, the date provided by that override.
     * 
     * @return A String in yyyy-MM-dd format.
     */
    public static String currentDateNy() {
        if (currentDateNyOverride != null) {
            return currentDateNyOverride;
        }
        final long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis > endOfCurrentDateNy) {
            final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            format.setTimeZone(TimeZones.TZ_NEWYORK);
            currentDateNy = format.format(new Date(currentTimeMillis));

            // Calculate when this cached value expires
            endOfCurrentDateNy = getMillisAtMidnightNy(currentTimeMillis);
        }
        return currentDateNy;
    }

    /**
     * Sets the {@link #lastBusinessDayNyOverride} to the previous business day from a currently set
     * {@link #currentDateNyOverride} value. If {@link #currentDateNyOverride} has not been set,
     * this method has no effect.
     */
    public static void overrideLastBusinessDateNyFromCurrentDateNy() {
        if (currentDateNyOverride != null) {
            final BusinessCalendar bc = Calendars.calendar("USNYSE");
            lastBusinessDayNyOverride =
                bc.previousBusinessDay(currentDateNyOverride.substring(0, 10));
        }
    }

    /**
     * Cached value of lastBusinessDateNy, which expires after milliseconds from Epoch value of
     * {@link #endOfCurrentDateNyLastBusinessDay}
     */
    public static String lastBusinessDateNy = null;
    /**
     * Expiration for cached {@link #lastBusinessDateNy} as milliseconds from Epoch.
     */
    public static long endOfCurrentDateNyLastBusinessDay = 0;

    /**
     * Provides a String representing the previous business date in the New York time zone using the
     * NYSE calendar, or, if a custom {@link #lastBusinessDayNyOverride} has been set, the date
     * provided by that override.
     * 
     * @return A String in yyyy-MM-dd format.
     */
    public static String lastBusinessDateNy() {
        return lastBusinessDateNy(System.currentTimeMillis());
    }

    /**
     * Provides a String representing the previous business date in the New York time zone using the
     * NYSE calendar, or, if a custom {@link #lastBusinessDayNyOverride} has been set, the date
     * provided by that override.
     * 
     * @param currentTimeMillis The current date/time in milliseconds from Epoch to be used when
     *        determining the previous business date. Typically this is System.currentTimeMillis()
     *        and is passed in by calling the niladic variant of this method.
     * @return A String in yyyy-MM-dd format.
     */
    public static String lastBusinessDateNy(final long currentTimeMillis) {
        if (lastBusinessDayNyOverride != null) {
            return lastBusinessDayNyOverride;
        }

        if (currentTimeMillis > endOfCurrentDateNyLastBusinessDay) {
            final BusinessCalendar bc = Calendars.calendar("USNYSE");

            lastBusinessDateNy =
                bc.previousBusinessDay(DBTimeUtils.millisToTime(currentTimeMillis));

            // Calculate when this cached value expires
            endOfCurrentDateNyLastBusinessDay = getMillisAtMidnightNy(currentTimeMillis);
        }
        return lastBusinessDateNy;
    }

    /**
     * Returns the number of milliseconds from Epoch for midnight at the beginning of the next day
     * in the New York time zone relative to the date represented by a passed milliseconds from
     * Epoch date/time.
     * 
     * @param currentTimeMillis A long value of milliseconds from Epoch which is the date/time from
     *        which the next New York time zone midnight value should be calculated.
     * @return A long of milliseconds from Epoch for midnight at the beginning of the next day in
     *         the New York time zone.
     */
    private static long getMillisAtMidnightNy(final long currentTimeMillis) {
        final Calendar calendar = Calendar.getInstance(TimeZones.TZ_NEWYORK);
        calendar.setTimeInMillis(currentTimeMillis);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.add(Calendar.DAY_OF_YEAR, 1); // should handle daylight savings
        return calendar.getTimeInMillis();
    }

    private abstract static class CachedDate {

        final DBTimeZone timeZone;

        String value;
        long valueExpirationTimeMillis;

        private CachedDate(@NotNull final DBTimeZone timeZone) {
            this.timeZone = timeZone;
        }

        private DBTimeZone getTimeZone() {
            return timeZone;
        }

        public String get() {
            return get(System.currentTimeMillis());
        }

        public synchronized String get(final long currentTimeMillis) {
            if (currentTimeMillis >= valueExpirationTimeMillis) {
                update(currentTimeMillis);
            }
            return value;
        }

        abstract void update(long currentTimeMillis);
    }

    private static class CachedCurrentDate extends CachedDate {

        private CachedCurrentDate(@NotNull final DBTimeZone timeZone) {
            super(timeZone);
        }

        @Override
        void update(final long currentTimeMillis) {
            value = formatDate(millisToTime(currentTimeMillis), timeZone);
            valueExpirationTimeMillis = new DateTime(currentTimeMillis, timeZone.getTimeZone())
                .withFieldAdded(DurationFieldType.days(), 1).withTimeAtStartOfDay().getMillis();
        }
    }

    private static class CachedDateKey<CACHED_DATE_TYPE extends CachedDate>
        extends KeyedObjectKey.Basic<DBTimeZone, CACHED_DATE_TYPE> {

        @Override
        public DBTimeZone getKey(final CACHED_DATE_TYPE cachedDate) {
            return cachedDate.timeZone;
        }
    }

    private static final KeyedObjectHashMap<DBTimeZone, CachedCurrentDate> cachedCurrentDates =
        new KeyedObjectHashMap<>(new CachedDateKey<CachedCurrentDate>());

    /**
     * Returns a String of the current date in the specified {@link DBTimeZone}.
     * 
     * @param timeZone The {@link DBTimeZone} to reference when evaluating the current date for
     *        "now".
     * @return A String in format yyyy-MM-dd.
     */
    public static String currentDate(DBTimeZone timeZone) {
        return cachedCurrentDates.putIfAbsent(timeZone, CachedCurrentDate::new).get();
    }

    /**
     * Converts a value of nanoseconds from Epoch to a {@link DBDateTime}.
     * 
     * @param nanos The long nanoseconds since Epoch value to convert.
     * @return A DBDateTime for {@code nanos}, or {@code null} if {@code nanos} is equal to
     *         {@link QueryConstants#NULL_LONG NULL_LONG}.
     */
    public static DBDateTime nanosToTime(long nanos) {
        return nanos == io.deephaven.util.QueryConstants.NULL_LONG ? null : new DBDateTime(nanos);
    }

    /**
     * Converts a long offset from Epoch value to a {@link DBDateTime}. This method uses expected
     * date ranges to infer whether the passed value is in milliseconds, microseconds, or
     * nanoseconds. Thresholds used are {@link TimeConstants#MICROTIME_THRESHOLD} divided by 1000
     * for milliseconds, as-is for microseconds, and multiplied by 1000 for nanoseconds. The value
     * is tested to see if its ABS exceeds the threshold. E.g. a value whose ABS is greater than
     * 1000 * {@link TimeConstants#MICROTIME_THRESHOLD} will be treated as nanoseconds.
     * 
     * @param epoch The long Epoch offset value to convert.
     * @return null, if the input is equal to {@link QueryConstants#NULL_LONG}, otherwise a
     *         {@link DBDateTime} based on the inferred conversion.
     */
    @SuppressWarnings("WeakerAccess")
    public static DBDateTime autoEpochToTime(long epoch) {
        if (epoch == io.deephaven.util.QueryConstants.NULL_LONG) {
            return new DBDateTime(epoch);
        }

        if (Math.abs(epoch) > (TimeConstants.MICROTIME_THRESHOLD * 1000)) // Nanoseconds
        {
            return nanosToTime(epoch);
        } else if (Math.abs(epoch) > TimeConstants.MICROTIME_THRESHOLD) // Microseconds
        {
            return microsToTime(epoch);
        } else if (Math.abs(epoch) > TimeConstants.MICROTIME_THRESHOLD / 1000) // Milliseconds
        {
            return millisToTime(epoch);
        } else // Seconds
        {
            return millisToTime(epoch * 1000);
        }
    }

    /**
     * Returns a {@link DBDateTime} value based on a starting value and a {@link DBPeriod} to add to
     * it, but with a cap max value which is returned in case the starting value plus period exceeds
     * the cap.
     * 
     * @param original The starting {@link DBDateTime} value.
     * @param period The {@link DBPeriod} to add to dateTime.
     * @param cap A {@link DBDateTime} value to use as the maximum return value.
     * @return a null {@link DBDateTime} if either original or period are null; the starting
     *         {@link DBDateTime} plus the specified period, if the result is not too large for a
     *         DBDateTime and does not exceed the cap value; the cap value if this is less than
     *         offset plus period. Throws a {@link DBDateTimeOverflowException
     *         DBDateTimeOverflowException} if the resultant value is more than max long nanoseconds
     *         from Epoch.
     */
    public static DBDateTime cappedTimeOffset(DBDateTime original, DBPeriod period,
        DBDateTime cap) {
        DBDateTime offset = DBTimeUtils.plus(original, period);
        return (offset.compareTo(cap) > 0) ? cap : offset;
    }

    /**
     * Returns a {@link DBDateTime} value, which is at the starting (lower) end of a time range
     * defined by the interval nanoseconds. For example, a 5*MINUTE intervalNanos value would return
     * the date/time value for the start of the five minute window that contains the input date
     * time.
     *
     * @param dateTime The {@link DBDateTime} for which to evaluate the start of the containing
     *        window.
     * @param intervalNanos The size of the window in nanoseconds.
     * @return Null if either input is null, otherwise a {@link DBDateTime} representing the start
     *         of the window.
     */
    public static DBDateTime lowerBin(DBDateTime dateTime, long intervalNanos) {
        if (dateTime == null || intervalNanos == io.deephaven.util.QueryConstants.NULL_LONG) {
            return null;
        }

        return nanosToTime(LongNumericPrimitives.lowerBin(dateTime.getNanos(), intervalNanos));
    }

    /**
     * Returns a {@link DBDateTime} value, which is at the starting (lower) end of a time range
     * defined by the interval nanoseconds. For example, a 5*MINUTE intervalNanos value would return
     * the date/time value for the start of the five minute window that contains the input date
     * time.
     *
     * @param dateTime The {@link DBDateTime} for which to evaluate the start of the containing
     *        window.
     * @param intervalNanos The size of the window in nanoseconds.
     * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would
     *        offset all windows by one minute.
     * @return Null if either input is null, otherwise a {@link DBDateTime} representing the start
     *         of the window.
     */
    public static DBDateTime lowerBin(DBDateTime dateTime, long intervalNanos, long offset) {
        if (dateTime == null || intervalNanos == QueryConstants.NULL_LONG
            || offset == QueryConstants.NULL_LONG) {
            return null;
        }

        return nanosToTime(
            LongNumericPrimitives.lowerBin(dateTime.getNanos() - offset, intervalNanos) + offset);
    }

    /**
     * Returns a {@link DBDateTime} value, which is at the ending (upper) end of a time range
     * defined by the interval nanoseconds. For example, a 5*MINUTE intervalNanos value would return
     * the date/time value for the end of the five minute window that contains the input date time.
     *
     * @param dateTime The {@link DBDateTime} for which to evaluate the end of the containing
     *        window.
     * @param intervalNanos The size of the window in nanoseconds.
     * @return Null if either input is null, otherwise a {@link DBDateTime} representing the end of
     *         the window.
     */
    public static DBDateTime upperBin(DBDateTime dateTime, long intervalNanos) {
        if (dateTime == null || intervalNanos == io.deephaven.util.QueryConstants.NULL_LONG) {
            return null;
        }

        return nanosToTime(LongNumericPrimitives.upperBin(dateTime.getNanos(), intervalNanos));
    }

    /**
     * Returns a {@link DBDateTime} value, which is at the ending (upper) end of a time range
     * defined by the interval nanoseconds. For example, a 5*MINUTE intervalNanos value would return
     * the date/time value for the end of the five minute window that contains the input date time.
     *
     * @param dateTime The {@link DBDateTime} for which to evaluate the end of the containing
     *        window.
     * @param intervalNanos The size of the window in nanoseconds.
     * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would
     *        offset all windows by one minute.
     * @return Null if either input is null, otherwise a {@link DBDateTime} representing the end of
     *         the window.
     */
    public static DBDateTime upperBin(DBDateTime dateTime, long intervalNanos, long offset) {
        if (dateTime == null || intervalNanos == io.deephaven.util.QueryConstants.NULL_LONG
            || offset == io.deephaven.util.QueryConstants.NULL_LONG) {
            return null;
        }

        return nanosToTime(
            LongNumericPrimitives.upperBin(dateTime.getNanos() - offset, intervalNanos) + offset);
    }

    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

    // + can only result in flow if both positive or both negative
    private static long checkOverflowPlus(final long l1, final long l2,
        final boolean minusOperation) {
        if (l1 > 0 && l2 > 0 && Long.MAX_VALUE - l1 < l2) {
            final String message = minusOperation
                ? "Subtracting " + -l2 + " nanos from " + l1 + " would overflow"
                : "Adding " + l2 + " nanos to " + l1 + " would overflow";
            throw new DBDateTimeOverflowException(message);
        }

        if (l1 < 0 && l2 < 0) {
            return checkUnderflowMinus(l1, -l2, false);
        }

        return l1 + l2;
    }

    // - can only result in flow if one is positive and one is negative
    private static long checkUnderflowMinus(final long l1, final long l2,
        final boolean minusOperation) {
        if (l1 < 0 && l2 > 0 && Long.MIN_VALUE + l2 > -l1) {
            final String message = minusOperation
                ? "Subtracting " + l2 + " nanos from " + l1 + " would underflow"
                : "Adding " + -l2 + " nanos to " + l1 + " would underflow";
            throw new DBDateTimeOverflowException(message);
        }

        if (l1 > 0 && l2 < 0) {
            return checkOverflowPlus(l1, -l2, true);
        }

        return l1 - l2;
    }

    /**
     * Converts an expression, replacing DBDateTime and DBPeriod literals with references to
     * constant DBDateTime/DBPeriod instances.
     *
     * @param formula The formula to convert.
     * @return A {@link Result} object, which includes the converted formula string, a string of
     *         instance variable declarations, and a map describing the names and types of these
     *         instance variables.
     *
     * @throws Exception If any error occurs or a literal value cannot be parsed.
     */
    // TODO: This should probably be handled in DBLanguageParser.accept(CharLiteralExpr,
    // StringBuilder).
    public static Result convertExpression(String formula) throws Exception { // TODO: Why throw
                                                                              // Exception?
        final StringBuilder instanceVariablesString = new StringBuilder();
        final HashMap<String, Class> newVariables = new HashMap<>();

        final StringBuffer convertedFormula = new StringBuffer();

        int localDateIndex = 0;
        int dateTimeIndex = 0;
        int timeIndex = 0;
        int periodIndex = 0;

        final Matcher matcher = Pattern.compile("'[^']*'").matcher(formula);

        while (matcher.find()) {
            String s = formula.substring(matcher.start() + 1, matcher.end() - 1);

            if (s.length() <= 1) {
                // leave chars and also bad empty ones alone
                continue;
            }

            if (convertDateTimeQuiet(s) != null) {
                matcher.appendReplacement(convertedFormula, "_date" + dateTimeIndex);
                instanceVariablesString.append("        private DBDateTime _date")
                    .append(dateTimeIndex).append("=DBTimeUtils.convertDateTime(\"")
                    .append(formula, matcher.start() + 1, matcher.end() - 1).append("\");\n");
                newVariables.put("_date" + dateTimeIndex, DBDateTime.class);

                dateTimeIndex++;
            } else if (convertDateQuiet(s) != null) {
                matcher.appendReplacement(convertedFormula, "_localDate" + localDateIndex);
                instanceVariablesString.append("        private java.time.LocalDate _localDate")
                    .append(localDateIndex).append("=DBTimeUtils.convertDate(\"")
                    .append(formula, matcher.start() + 1, matcher.end() - 1).append("\");\n");
                newVariables.put("_localDate" + localDateIndex, LocalDate.class);
                localDateIndex++;
            } else if (convertTimeQuiet(s) != io.deephaven.util.QueryConstants.NULL_LONG) {
                matcher.appendReplacement(convertedFormula, "_time" + timeIndex);
                instanceVariablesString.append("        private long _time").append(timeIndex)
                    .append("=DBTimeUtils.convertTime(\"")
                    .append(formula, matcher.start() + 1, matcher.end() - 1).append("\");\n");
                newVariables.put("_time" + timeIndex, long.class);

                timeIndex++;
            } else if (convertPeriodQuiet(s) != null) {
                matcher.appendReplacement(convertedFormula, "_period" + periodIndex);
                instanceVariablesString.append("        private DBPeriod _period")
                    .append(periodIndex).append("=DBTimeUtils.convertPeriod(\"")
                    .append(formula, matcher.start() + 1, matcher.end() - 1).append("\");\n");
                newVariables.put("_period" + periodIndex, DBPeriod.class);

                periodIndex++;
            } else if (convertLocalTimeQuiet(s) != null) {
                matcher.appendReplacement(convertedFormula, "_localTime" + timeIndex);
                instanceVariablesString.append("        private java.time.LocalTime _localTime")
                    .append(timeIndex).append("=DBTimeUtils.convertLocalTime(\"")
                    .append(formula, matcher.start() + 1, matcher.end() - 1).append("\");\n");
                newVariables.put("_localTime" + timeIndex, LocalTime.class);
                timeIndex++;
            } else {
                throw new Exception("Cannot parse datetime/time/period : " + s);
            }
        }

        matcher.appendTail(convertedFormula);

        return new Result(convertedFormula.toString(), instanceVariablesString.toString(),
            newVariables);
    }

    /**
     * Converts a String date/time to nanoseconds from Epoch or a nanoseconds period. Three patterns
     * are supported:
     * <p>
     * yyyy-MM-ddThh:mm:ss[.nnnnnnnnn] TZ for date/time values
     * </p>
     * <p>
     * hh:mm:ss[.nnnnnnnnn] for time values
     * </p>
     * <p>
     * Period Strings in the form of numbertype, e.g. 1W for one week, and Tnumbertype for times,
     * e.g. T1M for one minute
     * </p>
     * 
     * @param formula The String to be evaluated and converted. Optionally, but preferred, enclosed
     *        in straight single ticks.
     * @return A long value representing an Epoch offset in nanoseconds for a time or date/time, or
     *         a duration in nanoseconds for a period. Throws {@link DBDateTimeOverflowException} if
     *         the resultant value would be longer than max long, or
     *         {@link IllegalArgumentException} if expression cannot be evaluated.
     */
    public static long expressionToNanos(String formula) {
        if (!formula.startsWith("'")) {
            formula = '\'' + formula + '\'';
        }
        Matcher matcher = Pattern.compile("'[^'][^']+'").matcher(formula);

        boolean result = matcher.find();

        String s = formula.substring(matcher.start() + 1, matcher.end() - 1);
        final DBDateTime dateTime = convertDateTimeQuiet(s);
        if (dateTime != null) {
            return dateTime.getNanos();
        }
        long time = convertTimeQuiet(s);
        if (time != io.deephaven.util.QueryConstants.NULL_LONG) {
            return time;
        }
        final DBPeriod period = convertPeriodQuiet(s);
        if (period != null) {
            try {
                return StrictMath.multiplyExact(
                    period.getJodaPeriod().toStandardDuration().getMillis(), 1_000_000L);
            } catch (ArithmeticException ex) {
                throw new DBDateTimeOverflowException(
                    "Period length in nanoseconds exceeds Long.MAX_VALUE : " + s, ex);
            }
        }
        throw new IllegalArgumentException("Cannot parse datetime/time/period : " + s);
    }

    /**
     * Attempt to convert the given string to a LocalDate. This should <b>not</b> accept dates with
     * times, as we want those to be interpreted as DBDateTime values. The ideal date format is
     * YYYY-MM-DD since it's the least ambiguous, but this method also parses slash-delimited dates
     * according to the system "date style".
     *
     * @param s the date string to convert
     * @throws RuntimeException if the date cannot be converted, otherwise returns a
     *         {@link LocalDate}
     */
    @SuppressWarnings("WeakerAccess")
    public static LocalDate convertDate(String s) {
        final LocalDate ret = convertDateQuiet(s);

        if (ret == null) {
            throw new RuntimeException("Cannot parse date : " + s);
        }

        return ret;
    }

    /**
     * Converts a DateTime String from a few specific zoned formats to a DBDateTime
     *
     * @param s String to be converted, usually in the form yyyy-MM-ddThh:mm:ss and with optional
     *        sub-seconds after an optional decimal point, followed by a mandatory time zone
     *        character code
     * @throws RuntimeException if the String cannot be converted, otherwise a {@link DBDateTime}
     *         from the parsed String.
     */
    public static DBDateTime convertDateTime(String s) {
        DBDateTime ret = convertDateTimeQuiet(s);

        if (ret == null) {
            throw new RuntimeException("Cannot parse datetime : " + s);
        }

        return ret;
    }

    /**
     * Converts a String time to nanoseconds from Epoch. The format for the String is:
     * <p>
     * hh:mm:ss[.nnnnnnnnn].
     * 
     * @param s The String to be evaluated and converted.
     * @return A long value representing an Epoch offset in nanoseconds. Throws
     *         {@link RuntimeException} if the String cannot be parsed.
     */
    public static long convertTime(String s) {
        long ret = convertTimeQuiet(s);

        if (ret == io.deephaven.util.QueryConstants.NULL_LONG) {
            throw new RuntimeException("Cannot parse time : " + s);
        }

        return ret;
    }

    /**
     * Converts a String into a {@link DBPeriod} object.
     * 
     * @param s The String to convert in the form of numbertype, e.g. 1W for one week, and
     *        Tnumbertype for times, e.g. T1M for one minute.
     * @throws RuntimeException if the String cannot be parsed, otherwise a {@link DBPeriod} object.
     */
    @SuppressWarnings("WeakerAccess")
    public static DBPeriod convertPeriod(String s) {
        DBPeriod ret = convertPeriodQuiet(s);

        if (ret == null) {
            throw new RuntimeException("Cannot parse period : " + s);
        }

        return ret;
    }

    private static int extractTwoDigitNum(String s, int startIndex) {
        return (s.charAt(startIndex) - '0') * 10 + (s.charAt(startIndex + 1) - '0');
    }

    private static int extractThreeDigitNum(String s, int startIndex) {
        return (s.charAt(startIndex) - '0') * 100 + (s.charAt(startIndex + 1) - '0') * 10
            + (s.charAt(startIndex + 2) - '0');
    }

    private static int extractFourDigitNum(String s, int startIndex) {
        return (s.charAt(startIndex) - '0') * 1000 + (s.charAt(startIndex + 1) - '0') * 100
            + (s.charAt(startIndex + 2) - '0') * 10 + (s.charAt(startIndex + 3) - '0');
    }

    private static int extractSixDigitNum(String s, int startIndex) {
        int result = 0;
        for (int i = startIndex; i < startIndex + 6; i++) {
            result = result * 10 + s.charAt(i) - '0';
        }
        return result;
    }

    /**
     * Converts a time String in the form hh:mm:ss[.nnnnnnnnn] to a {@link LocalTime}.
     * 
     * @param s The String to convert.
     * @return null if the String cannot be parsed, otherwise a {@link LocalTime}.
     */
    public static LocalTime convertLocalTimeQuiet(String s) {
        try {
            // private static final Pattern LOCAL_TIME_PATTERN =
            // Pattern.compile("([0-9][0-9]):?([0-9][0-9])?:?([0-9][0-9])?(\\.([0-9]{1,9}))?");
            final Matcher matcher = LOCAL_TIME_PATTERN.matcher(s);
            if (matcher.matches()) {
                final int hour = Integer.parseInt(matcher.group(1)); // hour is the only required
                                                                     // field
                final int minute =
                    matcher.group(2) != null ? Integer.parseInt(matcher.group(2)) : 0;
                final int second =
                    matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : 0;
                final int nanos;
                if (matcher.group(4) != null) {
                    final String fractionStr = matcher.group(5); // group 5 excludes the decimal pt
                    nanos = Integer.parseInt(fractionStr)
                        * (int) Math.pow(10, 9 - fractionStr.length());
                } else {
                    nanos = 0;
                }
                return LocalTime.of(hour, minute, second, nanos);
            }
        } catch (Exception ex) {
            return null;
        }
        return null;
    }

    /**
     * Attempt to convert the given string to a LocalDate. This should <b>not</b> accept dates with
     * times, as we want those to be interpreted as DBDateTime values. The ideal date format is
     * YYYY-MM-DD since it's the least ambiguous.
     *
     * @param s the date string to convert
     * @return the LocalDate formatted using the default date style.
     */
    public static LocalDate convertDateQuiet(String s) {
        return convertDateQuiet(s, DATE_STYLE);
    }

    private static LocalDate matchStdDate(Pattern pattern, String s) {
        final Matcher matcher = pattern.matcher(s);
        if (matcher.matches()) {
            final int year = Integer.parseInt(matcher.group("year"));
            final int month = Integer.parseInt(matcher.group("month"));
            final int dayOfMonth = Integer.parseInt(matcher.group("day"));
            return LocalDate.of(year, month, dayOfMonth);
        }
        return null;
    }

    /**
     * Attempt to convert the given string to a LocalDate. This should <b>not</b> accept dates with
     * times, as we want those to be interpreted as DBDateTime values. The ideal date format is
     * YYYY-MM-DD since it's the least ambiguous.
     *
     * @param s the date string
     * @param dateStyle indicates how to interpret slash-delimited dates
     * @return the LocalDate
     */
    public static LocalDate convertDateQuiet(String s, DateStyle dateStyle) {
        try {
            LocalDate localDate = matchStdDate(STD_DATE_PATTERN, s);
            if (localDate != null) {
                return localDate;
            }
            localDate = matchStdDate(STD_DATE_PATTERN2, s);
            if (localDate != null) {
                return localDate;
            }

            // see if we can match one of the slash-delimited styles, the interpretation of which
            // requires knowing the
            // system date style setting (for example Europeans often write dates as d/m/y).
            final Matcher slashMatcher = SLASH_DATE_PATTERN.matcher(s);
            if (slashMatcher.matches()) {
                final String yearGroup, monthGroup, dayGroup, yearFinal2DigitsGroup;
                // note we have nested groups which allow us to detect 2 vs 4 digit year
                // (groups 2 and 5 are the optional last 2 digits)
                switch (dateStyle) {
                    case MDY:
                        dayGroup = "part2";
                        monthGroup = "part1";
                        yearGroup = "part3";
                        yearFinal2DigitsGroup = "part3sub2";
                        break;
                    case DMY:
                        dayGroup = "part1";
                        monthGroup = "part2";
                        yearGroup = "part3";
                        yearFinal2DigitsGroup = "part3sub2";
                        break;
                    case YMD:
                        dayGroup = "part3";
                        monthGroup = "part2";
                        yearGroup = "part1";
                        yearFinal2DigitsGroup = "part1sub2";
                        break;
                    default:
                        throw new IllegalStateException("Unsupported DateStyle: " + DATE_STYLE);
                }
                final int year;
                // for 2 digit years, lean on java's standard interpretation
                if (slashMatcher.group(yearFinal2DigitsGroup) == null) {
                    year =
                        Year.parse(slashMatcher.group(yearGroup), TWO_DIGIT_YR_FORMAT).getValue();
                } else {
                    year = Integer.parseInt(slashMatcher.group(yearGroup));
                }
                final int month = Integer.parseInt(slashMatcher.group(monthGroup));
                final int dayOfMonth = Integer.parseInt(slashMatcher.group(dayGroup));
                return LocalDate.of(year, month, dayOfMonth);
            }
        } catch (Exception ex) {
            return null;
        }
        return null;
    }

    /*
     * This version assumes you know what date it is and that the format is correct and just want
     * the time, so we can save time (e.g. 2010-09-02T08:17:17.502-0400)
     * 0123456789012345678901234567 1 2
     */

    @SuppressWarnings("WeakerAccess")
    public static DBDateTime convertJimDateTimeQuiet(String s) {
        int year = extractFourDigitNum(s, 0);
        int month = extractTwoDigitNum(s, 5);
        int day = extractTwoDigitNum(s, 8);
        int hour = extractTwoDigitNum(s, 11);
        int min = extractTwoDigitNum(s, 14);
        int sec = extractTwoDigitNum(s, 17);
        int millis = extractThreeDigitNum(s, 20);
        int tzHours = (s.charAt(23) == '-' ? -1 : 1) * extractTwoDigitNum(s, 24);
        DateTimeZone timeZone = DateTimeZone.forOffsetHours(tzHours);
        DateTime d = new DateTime(year, month, day, hour, min, sec, millis, timeZone);
        return new DBDateTime(millisToNanos(d.getMillis()));
    }

    @SuppressWarnings("WeakerAccess")
    public static DBDateTime convertJimMicrosDateTimeQuiet(String s) {
        int year = extractFourDigitNum(s, 0);
        int month = extractTwoDigitNum(s, 5);
        int day = extractTwoDigitNum(s, 8);
        int hour = extractTwoDigitNum(s, 11);
        int min = extractTwoDigitNum(s, 14);
        int sec = extractTwoDigitNum(s, 17);
        int micros = extractSixDigitNum(s, 20);
        int tzHours = (s.charAt(26) == '-' ? -1 : 1) * extractTwoDigitNum(s, 27);
        DateTimeZone timeZone = DateTimeZone.forOffsetHours(tzHours);
        DateTime d = new DateTime(year, month, day, hour, min, sec, micros / 1000, timeZone);
        return new DBDateTime(millisToNanos(d.getMillis()) + (micros % 1000) * 1000);
    }

    /**
     * Converts a DateTime String from a few specific zoned formats to a DBDateTime
     *
     * @param s String to be converted, usually in the form yyyy-MM-ddThh:mm:ss and with optional
     *        sub-seconds after an optional decimal point, followed by a mandatory time zone
     *        character code
     * @return A DBDateTime from the parsed String, or null if the format is not recognized or an
     *         exception occurs
     */
    public static DBDateTime convertDateTimeQuiet(final String s) {
        try {
            DBTimeZone timeZone = null;
            String dateTimeString = null;
            if (DATETIME_PATTERN.matcher(s).matches()) {
                int spaceIndex = s.indexOf(' ');
                if (spaceIndex == -1) { // no timezone
                    return null;
                }
                timeZone =
                    DBTimeZone.valueOf("TZ_" + s.substring(spaceIndex + 1).trim().toUpperCase());
                dateTimeString = s.substring(0, spaceIndex);
            } else if (JIM_DATETIME_PATTERN.matcher(s).matches()) {
                return convertJimDateTimeQuiet(s);
            } else if (JIM_MICROS_DATETIME_PATTERN.matcher(s).matches()) {
                return convertJimMicrosDateTimeQuiet(s);
            }

            if (timeZone == null) {
                return null;
            }
            int decimalIndex = dateTimeString.indexOf('.');
            if (decimalIndex == -1) {
                return new DBDateTime(millisToNanos(
                    new DateTime(dateTimeString, timeZone.getTimeZone()).getMillis()));
            } else {
                final long subsecondNanos = parseNanos(dateTimeString.substring(decimalIndex + 1));

                return new DBDateTime(
                    millisToNanos(new DateTime(dateTimeString.substring(0, decimalIndex),
                        timeZone.getTimeZone()).getMillis()) + subsecondNanos);
            }
        } catch (Exception e) {
            // shouldn't get here too often, but somehow something snuck through. we'll just return
            // null below...
        }

        return null;
    }

    /**
     * Converts a String of digits of any length to a nanoseconds long value. Will ignore anything
     * longer than 9 digits, and will throw a NumberFormatException if any non-numeric character is
     * found. Strings shorter than 9 digits will be interpreted as sub-second values to the right of
     * the decimal point.
     *
     * @param input The String to convert
     * @return long value in nanoseconds
     */
    private static long parseNanos(@NotNull final String input) {
        long result = 0;
        for (int i = 0; i < 9; i++) {
            result *= 10;
            final int digit;
            if (i >= input.length()) {
                digit = 0;
            } else {
                digit = Character.digit(input.charAt(i), 10);
                if (digit < 0) {
                    throw new NumberFormatException(
                        "Invalid character for nanoseconds conversion: " + input.charAt(i));
                }
            }
            result += digit;
        }
        return result;
    }

    // This function and the next are FAR faster than convertJimMicrosDateTimeQuiet provided you can
    // reuse the time zone across calls. Helpful for log file parsing.
    public static DBDateTime convertJimMicrosDateTimeQuietFast(String s, DateTimeZone timeZone) {
        int year = extractFourDigitNum(s, 0);
        int month = extractTwoDigitNum(s, 5);
        int day = extractTwoDigitNum(s, 8);
        int hour = extractTwoDigitNum(s, 11);
        int min = extractTwoDigitNum(s, 14);
        int sec = extractTwoDigitNum(s, 17);
        int micros = extractSixDigitNum(s, 20);
        DateTime d = new DateTime(year, month, day, hour, min, sec, micros / 1000, timeZone);
        return new DBDateTime(millisToNanos(d.getMillis()) + (micros % 1000) * 1000);
    }

    // This function is very slow. If you can call it once and reuse the result across many calls to
    // the above, this is FAR faster than convertJimMicrosDateTimeQuiet
    public static DateTimeZone convertJimMicrosDateTimeQuietFastTz(String s) {
        int tzHours = (s.charAt(26) == '-' ? -1 : 1) * extractTwoDigitNum(s, 27);
        return DateTimeZone.forOffsetHours(tzHours);
    }

    /**
     * Converts a time String in the form hh:mm:ss[.nnnnnnnnn] to a long nanoseconds offset from
     * Epoch.
     * 
     * @param s The String to convert.
     * @return {@link QueryConstants#NULL_LONG} if the String cannot be parsed, otherwise long
     *         nanoseconds offset from Epoch.
     */
    public static long convertTimeQuiet(String s) {
        try {
            if (TIME_AND_DURATION_PATTERN.matcher(s).matches()) {
                long multiplier = 1;
                long dayNanos = 0;
                long subsecondNanos = 0;

                if (s.charAt(0) == '-') {
                    multiplier = -1;

                    s = s.substring(1);
                }

                int tIndex = s.indexOf('T');

                if (tIndex != -1) {
                    dayNanos = 86400000000000L * Integer.parseInt(s.substring(0, tIndex));

                    s = s.substring(tIndex + 1);
                }

                int decimalIndex = s.indexOf('.');

                if (decimalIndex != -1) {
                    subsecondNanos = parseNanos(s.substring(decimalIndex + 1));

                    s = s.substring(0, decimalIndex);
                }

                String[] tokens = s.split(":");

                if (tokens.length == 2) { // hh:mm
                    return multiplier * (1000000000L
                        * (3600 * Integer.parseInt(tokens[0]) + 60 * Integer.parseInt(tokens[1]))
                        + dayNanos + subsecondNanos);
                } else if (tokens.length == 3) { // hh:mm:ss
                    return multiplier * (1000000000L * (3600 * Integer.parseInt(tokens[0])
                        + 60 * Integer.parseInt(tokens[1]) + Integer.parseInt(tokens[2])) + dayNanos
                        + subsecondNanos);
                }
            }
        } catch (Exception e) {
            // shouldn't get here too often, but somehow something snuck through. we'll just return
            // null below...
        }

        return io.deephaven.util.QueryConstants.NULL_LONG;
    }

    /**
     * Converts a String into a {@link DBPeriod} object.
     * 
     * @param s The String to convert in the form of numbertype, e.g. 1W for one week, and
     *        Tnumbertype for times, e.g. T1M for one minute.
     * @return null if the String cannot be parsed, otherwise a {@link DBPeriod} object.
     */
    public static DBPeriod convertPeriodQuiet(String s) {
        if (s.length() <= 1) {
            return null;
        }

        try {
            if (PERIOD_PATTERN.matcher(s).matches()) {
                return new DBPeriod(s);
            }
        } catch (Exception e) {
            // shouldn't get here too often, but somehow something snuck through. we'll just return
            // null below...
        }

        return null;
    }

    /**
     * Converts a {@link DBDateTime} to a {@link ZonedDateTime}.
     *
     * @param dateTime The a {@link DBDateTime} to convert.
     * @return A {@link ZonedDateTime} using the default time zone for the session as indicated by
     *         {@link DBTimeZone#TZ_DEFAULT}.
     */
    public static ZonedDateTime getZonedDateTime(DBDateTime dateTime) {
        return getZonedDateTime(dateTime, DBTimeZone.TZ_DEFAULT);
    }

    /**
     * Converts a {@link DBDateTime} to a {@link ZonedDateTime}.
     *
     * @param dateTime The a {@link DBDateTime} to convert.
     * @param timeZone The {@link DBTimeZone} to use for the conversion.
     * @return A {@link ZonedDateTime} using the specified time zone.
     */
    public static ZonedDateTime getZonedDateTime(DBDateTime dateTime, DBTimeZone timeZone) {
        Instant millis = dateTime.getInstant();
        ZoneId zone = timeZone.getTimeZone().toTimeZone().toZoneId();
        return ZonedDateTime.ofInstant(millis, zone);
    }

    /**
     * Converts a {@link ZonedDateTime} to a {@link DBDateTime}.
     * 
     * @param zonedDateTime The a {@link ZonedDateTime} to convert.
     * @throws DBDateTimeOverflowException if the input is out of the range for a
     *         {@link DBDateTime}, otherwise, a {@link DBDateTime} version of the input.
     */
    public static DBDateTime toDateTime(ZonedDateTime zonedDateTime) {
        int nanos = zonedDateTime.getNano();
        long seconds = zonedDateTime.toEpochSecond();

        long limit = (Long.MAX_VALUE - nanos) / DBTimeUtils.SECOND;
        if (seconds >= limit) {
            throw new DBDateTimeOverflowException(
                "Overflow: cannot convert " + zonedDateTime + " to new DBDateTime");
        }

        return new DBDateTime(nanos + (seconds * DBTimeUtils.SECOND));
    }

    /**
     * Returns a {@link ChronoField} indicating the level of precision in a String time value.
     * 
     * @param timeDef The time String to evaluate.
     * @return null if the time String cannot be parsed, otherwise a {@link ChronoField} for the
     *         finest units in the String (e.g. "10:00:00" would yield SecondOfMinute).
     */
    public static ChronoField getFinestDefinedUnit(String timeDef) {
        Matcher dtMatcher = CAPTURING_DATETIME_PATTERN.matcher(timeDef);
        if (dtMatcher.matches()) {
            DateGroupId[] parts = DateGroupId.values();
            for (int i = parts.length - 1; i >= 0; i--) {
                String part = dtMatcher.group(parts[i].id);
                if (part != null && !part.isEmpty()) {
                    return parts[i].field;
                }
            }
        }

        return null;
    }

    /**
     * A container object for the result of {@link #convertExpression(String)}, which includes the
     * converted formula String, a String of instance variable declarations, and a map describing
     * the names and types of these instance variables.
     */
    public static class Result {
        private final String convertedFormula;
        private final String instanceVariablesString;
        private final HashMap<String, Class> newVariables;

        public Result(String convertedFormula, String instanceVariablesString,
            HashMap<String, Class> newVariables) {
            this.convertedFormula = convertedFormula;
            this.instanceVariablesString = instanceVariablesString;
            this.newVariables = newVariables;
        }

        public String getConvertedFormula() {
            return convertedFormula;
        }

        public String getInstanceVariablesString() {
            return instanceVariablesString;
        }

        public HashMap<String, Class> getNewVariables() {
            return newVariables;
        }
    }

    /**
     * A type of RuntimeException thrown when operations resulting in {@link DBDateTime} values
     * would exceed the range available by max or min long nanoseconds.
     */
    public static class DBDateTimeOverflowException extends RuntimeException {
        private DBDateTimeOverflowException() {
            super("Operation failed due to overflow");
        }

        private DBDateTimeOverflowException(String s) {
            super(s);
        }

        private DBDateTimeOverflowException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Create a DateTimeFormatter formatter with the specified time zone name using the standard
     * yyyy-MM-dd format.
     *
     * @param timeZoneName the time zone name
     * @return a formatter set for the specified time zone
     */
    public static DateTimeFormatter createFormatter(final String timeZoneName) {
        final ZoneId zoneId = ZoneId.of(timeZoneName);
        return DateTimeFormatter.ofPattern(DATE_COLUMN_PARTITION_FORMAT_STRING).withZone(zoneId);
    }

    /**
     * Given a DateTimeFormatter and a timestamp in millis, return the date as a String in standard
     * column-partition format of yyyy-MM-dd. A timestamp of NULL_LONG means use the system current
     * time.
     *
     * @param dateTimeFormatter the date formatter
     * @param timestampMillis the timestamp in millis
     * @return the formatted date
     */
    public static String getPartitionFromTimestampMillis(
        @NotNull final DateTimeFormatter dateTimeFormatter, final long timestampMillis) {
        if (timestampMillis == io.deephaven.util.QueryConstants.NULL_LONG) {
            return dateTimeFormatter.format(Instant.ofEpochMilli(System.currentTimeMillis()));
        }
        return dateTimeFormatter.format(Instant.ofEpochMilli(timestampMillis));
    }

    /**
     * Given a DateTimeFormatter and a timestamp in micros from epoch, return the date as a String
     * in standard column-partition format of yyyy-MM-dd. A timestamp of NULL_LONG means use the
     * system current time.
     *
     * @param dateTimeFormatter the date formatter
     * @param timestampMicros the timestamp in micros
     * @return the formatted date
     */
    public static String getPartitionFromTimestampMicros(
        @NotNull final DateTimeFormatter dateTimeFormatter, final long timestampMicros) {
        if (timestampMicros == io.deephaven.util.QueryConstants.NULL_LONG) {
            return dateTimeFormatter.format(Instant.ofEpochMilli(System.currentTimeMillis()));
        }
        return dateTimeFormatter.format(Instant.ofEpochMilli(timestampMicros / 1_000));
    }

    /**
     * Given a DateTimeFormatter and a timestamp in nanos from epoch, return the date as a String in
     * standard column-partition format of yyyy-MM-dd. A timestamp of NULL_LONG means use the system
     * current time.
     *
     * @param dateTimeFormatter the date formatter
     * @param timestampNanos the timestamp in nanos
     * @return the formatted date
     */
    public static String getPartitionFromTimestampNanos(
        @NotNull final DateTimeFormatter dateTimeFormatter, final long timestampNanos) {
        if (timestampNanos == io.deephaven.util.QueryConstants.NULL_LONG) {
            return dateTimeFormatter.format(Instant.ofEpochMilli(System.currentTimeMillis()));
        }
        return dateTimeFormatter.format(Instant.ofEpochMilli(timestampNanos / 1_000_000));
    }

    /**
     * Given a DateTimeFormatter and a timestamp in seconds from epoch, return the date as a String
     * in standard column-partition format of yyyy-MM-dd. A timestamp of NULL_LONG means use the
     * system current time.
     *
     * @param dateTimeFormatter the date formatter
     * @param timestampSeconds the timestamp in seconds
     * @return the formatted date
     */
    public static String getPartitionFromTimestampSeconds(
        @NotNull final DateTimeFormatter dateTimeFormatter, final long timestampSeconds) {
        if (timestampSeconds == io.deephaven.util.QueryConstants.NULL_LONG) {
            return dateTimeFormatter.format(Instant.ofEpochMilli(System.currentTimeMillis()));
        }
        return dateTimeFormatter.format(Instant.ofEpochMilli(timestampSeconds * 1_000));
    }
}
