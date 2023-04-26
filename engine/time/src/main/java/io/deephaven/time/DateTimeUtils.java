/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.clock.TimeConstants;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.configuration.Configuration;
import io.deephaven.function.Numeric;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateMidnight;
import org.joda.time.DurationFieldType;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Utilities for Deephaven date/time storage and manipulation.
 */
//@SuppressWarnings("unused")
@SuppressWarnings("RegExpRedundantEscape")
public class DateTimeUtils {
    //TODO: document
    //TODO: remove Joda exposure
    //TODO: curate API
    //TODO: review public vs private
    //TODO: test coverage
    //TODO: variable and function naming consistency

    //TODO: reorganize functions into better groupings

    // region Format Patterns

    // The following 3 patterns support LocalDate literals. Note all LocalDate patterns must not have characters after
    // the date, to avoid confusion with DateTime literals.

    /** Matches yyyy-MM-dd. */
    private static final Pattern STD_DATE_PATTERN =
            Pattern.compile("^(?<year>[0-9][0-9][0-9][0-9])-(?<month>[0-9][0-9])-(?<day>[0-9][0-9])$");

    /** Matches yyyyMMdd (consistent with ISO dates). */
    private static final Pattern STD_DATE_PATTERN2 =
            Pattern.compile("^(?<year>[0-9][0-9][0-9][0-9])(?<month>[0-9][0-9])(?<day>[0-9][0-9])$");
    /**
     * Matches variations of month/day/year or day/month/year or year/month/day - how this is interpreted depends on the
     * DateTimeUtils.dateStyle system property.
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

    // DateTime literals
    private static final Pattern DATETIME_PATTERN = Pattern.compile(
            "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9](T[0-9][0-9]?:[0-9][0-9](:[0-9][0-9])?(\\.[0-9][0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?)?)? [a-zA-Z]+");
    private static final Pattern TIME_AND_DURATION_PATTERN = Pattern.compile(
            "\\-?([0-9]+T)?([0-9]+):([0-9]+)(:[0-9]+)?(\\.[0-9][0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?)?");
    private static final Pattern PERIOD_PATTERN = Pattern.compile(
            "\\-?([0-9]+[Yy])?([0-9]+[Mm])?([0-9]+[Ww])?([0-9]+[Dd])?(T([0-9]+[Hh])?([0-9]+[Mm])?([0-9]+[Ss])?)?");
    private static final String DATE_COLUMN_PARTITION_FORMAT_STRING = "yyyy-MM-dd";

    private static final Pattern CAPTURING_DATETIME_PATTERN = Pattern.compile(
            "(([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9])T?)?(([0-9][0-9]?)(?::([0-9][0-9])(?::([0-9][0-9]))?(?:\\.([0-9][0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?))?)?)?( [a-zA-Z]+)?");

    // endregion

    // region Time Constants

    //TODO: no equivalent
    /**
     * One microsecond in nanoseconds.
     */
    public static final long MICRO = 1_000;

    //TODO: no equivalent
    /**
     * One millisecond in nanoseconds.
     */
    public static final long MILLI = 1_000_000;

    /**
     * One second in nanoseconds.
     */
    public static final long SECOND = 1_000_000_000;

    /**
     * One minute in nanoseconds.
     */
    public static final long MINUTE = 60 * SECOND;

    /**
     * One hour in nanoseconds.
     */
    public static final long HOUR = 60 * MINUTE;

    /**
     * One day in nanoseconds.
     */
    public static final long DAY = 24 * HOUR;

    /**
     * One week in nanoseconds.
     */
    public static final long WEEK = 7 * DAY;

    /**
     * One year (365 days) in nanoseconds.
     */
    public static final long YEAR = 365 * DAY;

    /**
     * Maximum time in microseconds that can be converted to a {@link DateTime} without overflow.
     */
    private static final long MAX_CONVERTIBLE_MICROS = Long.MAX_VALUE / 1_000L;

    /**
     * Maximum time in milliseconds that can be converted to a {@link DateTime} without overflow.
     */
    private static final long MAX_CONVERTIBLE_MILLIS = Long.MAX_VALUE / 1_000_000L;

    /**
     * Maximum time in seconds that can be converted to a {@link DateTime} without overflow.
     */
    private static final long MAX_CONVERTIBLE_SECONDS = Long.MAX_VALUE / 1_000_000_000L;

    /**
     * Number of seconds per nanosecond.
     */
    private static final double SECONDS_PER_NANO = 1. / (double) SECOND;

    /**
     * Number of minutes per nanosecond.
     */
    private static final double MINUTES_PER_NANO = 1. / (double) MINUTE;

    /**
     * Number of hours per nanosecond.
     */
    private static final double HOURS_PER_NANO = 1. / (double) HOUR;

    /**
     * Number of days per nanosecond.
     */
    private static final double DAYS_PER_NANO = 1. / (double) DAY;

    /**
     * Number of years per nanosecond.
     */
    private static final double YEARS_PER_NANO = 1. / (double) YEAR;

    // endregion

    // region Overflow / Underflow

    /**
     * A type of RuntimeException thrown when operations resulting in {@link DateTime} values would exceed the range
     * available by max or min long nanoseconds.
     */
    public static class DateTimeOverflowException extends RuntimeException {
        private DateTimeOverflowException() {
            super("Operation failed due to overflow");
        }

        private DateTimeOverflowException(String s) {
            super(s);
        }

        private DateTimeOverflowException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // + can only result in flow if both positive or both negative
    private static long checkOverflowPlus(final long l1, final long l2, final boolean minusOperation) {
        if (l1 > 0 && l2 > 0 && Long.MAX_VALUE - l1 < l2) {
            final String message = minusOperation
                    ? "Subtracting " + -l2 + " nanos from " + l1 + " would overflow"
                    : "Adding " + l2 + " nanos to " + l1 + " would overflow";
            throw new DateTimeOverflowException(message);
        }

        if (l1 < 0 && l2 < 0) {
            return checkUnderflowMinus(l1, -l2, false);
        }

        return l1 + l2;
    }

    // - can only result in flow if one is positive and one is negative
    private static long checkUnderflowMinus(final long l1, final long l2, final boolean minusOperation) {
        if (l1 < 0 && l2 > 0 && Long.MIN_VALUE + l2 > -l1) {
            final String message = minusOperation
                    ? "Subtracting " + l2 + " nanos from " + l1 + " would underflow"
                    : "Adding " + -l2 + " nanos to " + l1 + " would underflow";
            throw new DateTimeOverflowException(message);
        }

        if (l1 > 0 && l2 < 0) {
            return checkOverflowPlus(l1, -l2, true);
        }

        return l1 - l2;
    }

    // endregion

    // region Clock

    // TODO(deephaven-core#3044): Improve scaffolding around full system replay
    /**
     * Clock used to compute the current time.  This allows a custom clock to be used instead of the current system clock.
     * This is mainly used for replay simulations.
     */
    private static Clock clock;

    /**
     * Set the clock used to compute the current time.  This allows a custom clock to be used instead of the current system clock.
     * This is mainly used for replay simulations.
     * @param clock clock used to compute the current time.
     */
    public static void setClock( final Clock clock) {
        DateTimeUtils.clock = clock;
    }

    //TODO: no equivalent
    /**
     * Returns the clock used to compute the current time.  This may be the current system clock, or it may be an alternative
     * clock used for replay simulations.
     *
     * @return current clock.
     */
    public static Clock currentClock() {
        return Objects.requireNonNullElse(clock, Clock.system());
    }

    //TODO: separate now() methods for the current clock and the system clock? -- adjust docs accordingly
    /**
     * Provides the current {@link DateTime} according to the current clock.
     * Under most circumstances, this method will return the current system time, but during replay simulations,
     * this method can return the replay time.
     *
     * @see #currentClock()
     * @return the current {@link DateTime} according to the current clock.
     */
    @ScriptApi
    public static DateTime now() {
        return DateTime.of(currentClock());
    }

    //TODO: no equivalent
    /**
     * Provides the current {@link DateTime}, with millisecond resolution, according to the current clock.
     * Under most circumstances, this method will return the current system time, but during replay simulations,
     * this method can return the replay time.
     *
     * @see #currentClock()
     * @return the current {@link DateTime}, with millisecond resolution, according to the current clock.
     */
    public static DateTime nowMillisResolution() {
        return DateTime.ofMillis(currentClock());
    }

    // endregion

    // region Time Conversions

    //TODO: no equivalent
    /**
     * Converts microseconds to nanoseconds.
     *
     * @param micros microseconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *      microseconds converted to nanoseconds.
     */
    public static long microsToNanos(long micros) {
        if (micros == NULL_LONG) {
            return NULL_LONG;
        }
        if (Math.abs(micros) > MAX_CONVERTIBLE_MICROS) {
            throw new DateTimeOverflowException("Converting " + micros + " micros to nanos would overflow");
        }
        return micros * 1000;
    }

    /**
     * Converts milliseconds to nanoseconds.
     *
     * @param millis milliseconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *      milliseconds converted to nanoseconds.
     */
    public static long millisToNanos(long millis) {
        if (millis == NULL_LONG) {
            return NULL_LONG;
        }
        if (Math.abs(millis) > MAX_CONVERTIBLE_MILLIS) {
            throw new DateTimeOverflowException("Converting " + millis + " millis to nanos would overflow");
        }
        return millis * 1000000;
    }

    //TODO: no equivalent
    /**
     * Converts seconds to nanoseconds.
     *
     * @param seconds seconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *      seconds converted to nanoseconds.
     */
    public static long secondsToNanos(long seconds) {
        if (seconds == NULL_LONG) {
            return NULL_LONG;
        }
        if (Math.abs(seconds) > MAX_CONVERTIBLE_SECONDS) {
            throw new DateTimeOverflowException("Converting " + seconds + " seconds to nanos would overflow");
        }

        return seconds * 1000000000L;
    }

    //TODO: no equivalent
    /**
     * Converts nanoseconds to microseconds.
     *
     * @param nanos nanoseconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *      nanoseconds converted to microseconds, rounded down.
     */
    @SuppressWarnings("WeakerAccess")
    public static long nanosToMicros(long nanos) {
        if (nanos == NULL_LONG) {
            return NULL_LONG;
        }
        return nanos / MICRO;
    }

    /**
     * Converts nanoseconds to milliseconds.
     *
     * @param nanos nanoseconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *      nanoseconds converted to milliseconds, rounded down.
     */
    public static long nanosToMillis(long nanos) {
        if (nanos == NULL_LONG) {
            return NULL_LONG;
        }

        return nanos / MILLI;
    }

    //TODO: no equivalent
    /**
     * Converts nanoseconds to seconds.
     *
     * @param nanos nanoseconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *      nanoseconds converted to seconds, rounded down.
     */
    @SuppressWarnings("WeakerAccess")
    public static long nanosToSeconds(long nanos) {
        if (nanos == NULL_LONG) {
            return NULL_LONG;
        }
        return nanos / SECOND;
    }

    //TODO: add epoch to the name?
    /**
     * Returns nanoseconds from the Epoch for a {@link DateTime} value.
     *
     * @param dateTime {@link DateTime} to compute the Epoch offset for.
     * @return nanoseconds since Epoch, or a NULL_LONG value if the {@link DateTime} is null.
     */
    public static long nanos(DateTime dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return dateTime.getNanos();
    }

    //TODO: add epoch to the name?
    /**
     * Returns milliseconds from the Epoch for a {@link DateTime} value.
     *
     * @param dateTime {@link DateTime} to compute the Epoch offset for.
     * @return milliseconds since Epoch, or a NULL_LONG value if the {@link DateTime} is null.
     */
    public static long millis(DateTime dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return dateTime.getMillis();
    }

    //TODO: add epoch to the name?
    //TODO: no equivalent
    /**
     * Returns seconds since from the Epoch for a {@link DateTime} value.
     *
     * @param dateTime {@link DateTime} to compute the Epoch offset for.
     * @return seconds since Epoch, or a NULL_LONG value if the {@link DateTime} is null.
     */
    public static long seconds(DateTime dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return dateTime.getMillis() / 1000;
    }

    //TODO: add epoch to the name?
    /**
     * Converts nanoseconds from the Epoch to a {@link DateTime}.
     *
     * @param nanos nanoseconds since Epoch.
     * @return null if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *      nanoseconds from the Epoch converted to a {@link DateTime}.
     * @throws DateTimeOverflowException if the resultant {@link DateTime} exceeds the supported range.
     */
    public static DateTime nanosToDateTime(long nanos) {
        return nanos == NULL_LONG ? null : new DateTime(nanos);
    }

    //TODO: add epoch to the name?
    //TODO: no equivalent
    /**
     * Converts microseconds from the Epoch to a {@link DateTime}.
     *
     * @param micros microseconds since Epoch.
     * @return null if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *      microseconds from the Epoch converted to a {@link DateTime}.
     * @throws DateTimeOverflowException if the resultant {@link DateTime} exceeds the supported range.
     */
    public static DateTime microsToDateTime(long micros) {
        return nanosToDateTime(microsToNanos(micros));
    }

    //TODO: add epoch to the name?
    /**
     * Converts milliseconds from the Epoch to a {@link DateTime}.
     *
     * @param millis milliseconds since Epoch.
     * @return null if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *      milliseconds from the Epoch converted to a {@link DateTime}.
     * @throws DateTimeOverflowException if the resultant {@link DateTime} exceeds the supported range.
     */
    public static DateTime millisToDateTime(long millis) {
        return nanosToDateTime(millisToNanos(millis));
    }

    //TODO: add epoch to the name?
    //TODO: rename seconds to sec in methods?
    //TODO: no equivalent
    /**
     * Converts seconds from the Epoch to a {@link DateTime}.
     *
     * @param seconds seconds since Epoch.
     * @return null if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *      seconds from the Epoch converted to a {@link DateTime}.
     * @throws DateTimeOverflowException if the resultant {@link DateTime} exceeds the supported range.
     */
    public static DateTime secondsToDateTime(long seconds) {
        return nanosToDateTime(secondsToNanos(seconds));
    }

    //TODO: no equivalent
    //TODO: make this method private b/c of timezone?
    /**
     * Converts a {@link DateTime} to an Excel time represented as a double.
     *
     * @param dateTime {@link DateTime} to convert.
     * @param timeZone {@link java.util.TimeZone} to use when interpreting the {@link DateTime}.
     * @return 0.0 if either input is null; otherwise, the input {@link DateTime} converted to an Excel time represented as a double.
     */
    @SuppressWarnings("WeakerAccess")
    public static double dateTimeToExcel(DateTime dateTime, java.util.TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return 0.0d;
        }
        long millis = dateTime.getMillis();

        return (double) (millis + timeZone.getOffset(millis)) / 86400000 + 25569;
    }

    //TODO: no equivalent
    /**
     * Converts a {@link DateTime} to an Excel time represented as a double.
     *
     * @param dateTime {@link DateTime} to convert.
     * @param timeZone {@link TimeZone} to use when interpreting the {@link DateTime}.
     */
    @SuppressWarnings("WeakerAccess")
    public static double dateTimeToExcel(DateTime dateTime, TimeZone timeZone) {
        return dateTimeToExcel(dateTime, timeZone.getTimeZone().toTimeZone());
    }

    //TODO: add excelToDateTime

    // endregion

    // region Time Arithmetic

    /**
     * Adds nanoseconds to a {@link DateTime}.
     *
     * @param dateTime starting {@link DateTime} value.
     * @param nanos number of nanoseconds to add.
     * @return null if either input is null or {@link QueryConstants#NULL_LONG}; otherwise the starting {@link DateTime} plus the specified number
     *      of nanoseconds.
     * @throws DateTimeOverflowException if the resultant {@link DateTime} exceeds the supported range.
     */
    public static DateTime plus(DateTime dateTime, long nanos) {
        if (dateTime == null || nanos == NULL_LONG) {
            return null;
        }

        return new DateTime(checkOverflowPlus(dateTime.getNanos(), nanos, false));
    }

    /**
     * Adds a time period to a {@link DateTime}.
     *
     * @param dateTime starting {@link DateTime} value.
     * @param period time period.
     * @return null if either input is null or {@link QueryConstants#NULL_LONG}; otherwise the starting {@link DateTime} plus the specified time period.
     * @throws DateTimeOverflowException if the resultant {@link DateTime} exceeds the supported range.
     */
    public static DateTime plus(DateTime dateTime, Period period) {
        if (dateTime == null || period == null) {
            return null;
        }

        if (period.isPositive()) {
            return new DateTime(millisToNanos(dateTime.getJodaDateTime().plus(period.getJodaPeriod()).getMillis())
                    + dateTime.getNanosPartial());
        } else {
            return new DateTime(millisToNanos(dateTime.getJodaDateTime().minus(period.getJodaPeriod()).getMillis())
                    + dateTime.getNanosPartial());
        }
    }

    /**
     * Subtracts nanoseconds from a {@link DateTime}.
     *
     * @param dateTime starting {@link DateTime} value.
     * @param nanos number of nanoseconds to subtract.
     * @return null if either input is null or {@link QueryConstants#NULL_LONG}; otherwise the starting {@link DateTime} minus the specified number
     *      of nanoseconds.
     * @throws DateTimeOverflowException if the resultant {@link DateTime} exceeds the supported range.
     */
    public static DateTime minus(DateTime dateTime, long nanos) {
        if (dateTime == null || -nanos == NULL_LONG) {
            return null;
        }

        return new DateTime(checkUnderflowMinus(dateTime.getNanos(), nanos, true));
    }

    /**
     * Subtracts a time period from a {@link DateTime}.
     *
     * @param dateTime starting {@link DateTime} value.
     * @param period time period.
     * @return null if either input is null or {@link QueryConstants#NULL_LONG}; otherwise the starting {@link DateTime} minus the specified time period.
     * @throws DateTimeOverflowException if the resultant {@link DateTime} exceeds the supported range.
     */
    public static DateTime minus(DateTime dateTime, Period period) {
        if (dateTime == null || period == null) {
            return null;
        }

        if (period.isPositive()) {
            return new DateTime(millisToNanos(dateTime.getJodaDateTime().minus(period.getJodaPeriod()).getMillis())
                    + dateTime.getNanosPartial());
        } else {
            return new DateTime(millisToNanos(dateTime.getJodaDateTime().plus(period.getJodaPeriod()).getMillis())
                    + dateTime.getNanosPartial());
        }
    }

    /**
     * Subtract one time from another and return the difference in nanoseconds.
     *
     * @param dateTime1 first {@link DateTime}.
     * @param dateTime2 second {@link DateTime}.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise the difference in dateTime1 and dateTime2 in nanoseconds.
     * @throws DateTimeOverflowException if the datetime arithemetic overflows or underflows.
     */
    public static long minus(DateTime dateTime1, DateTime dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return NULL_LONG;
        }

        return checkUnderflowMinus(dateTime1.getNanos(), dateTime2.getNanos(), true);
    }

    /**
     * Returns the difference in nanoseconds between two {@link DateTime} values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise the difference in start and end in nanoseconds.
     * @throws DateTimeOverflowException if the datetime arithemetic overflows or underflows.
     */
    @SuppressWarnings("WeakerAccess")
    public static long diffNanos(DateTime start, DateTime end) {
        return minus(end, start);
    }

    /**
     * Returns the difference in microseconds between two {@link DateTime} values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise the difference in start and end in microseconds.
     * @throws DateTimeOverflowException if the datetime arithemetic overflows or underflows.
     */
    public static long diffMicros(DateTime start, DateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }

        return nanosToMicros(diffNanos(start, end));
    }

    /**
     * Returns the difference in milliseconds between two {@link DateTime} values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise the difference in start and end in milliseconds.
     * @throws DateTimeOverflowException if the datetime arithemetic overflows or underflows.
     */
    public static long diffMillis(DateTime start, DateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }

        return nanosToMillis(diffNanos(start, end));
    }

    /**
     * Returns the difference in seconds between two {@link DateTime} values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is null; otherwise the difference in start and end in seconds.
     * @throws DateTimeOverflowException if the datetime arithemetic overflows or underflows.
     */
    public static double diffSeconds(DateTime start, DateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / SECOND;
    }

    /**
     * Returns the difference in minutes between two {@link DateTime} values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is null; otherwise the difference in start and end in minutes.
     * @throws DateTimeOverflowException if the datetime arithemetic overflows or underflows.
     */
    public static double diffMinutes(DateTime start, DateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / MINUTE;
    }

    /**
     * Returns the difference in days between two {@link DateTime} values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is null; otherwise the difference in start and end in days.
     * @throws DateTimeOverflowException if the datetime arithemetic overflows or underflows.
     */
    public static double diffDays(DateTime start, DateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / DAY;
    }

    /**
     * Returns the difference in years (365-days) between two {@link DateTime} values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is null; otherwise the difference in start and end in years.
     * @throws DateTimeOverflowException if the datetime arithemetic overflows or underflows.
     */
    public static double diffYears(DateTime start, DateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) * YEARS_PER_NANO;
    }

    // endregion

    // region Comparisons

    /**
     * Evaluates whether one {@link DateTime} value is before a second {@link DateTime} value.
     *
     * @param dateTime1 first {@link DateTime}.
     * @param dateTime2 second {@link DateTime}.
     * @return true if dateTime1 is before dateTime2; otherwise, false if either value is null or if dateTime2 is equal
     *      to or before dateTime1.
     */
    public static boolean isBefore(DateTime dateTime1, DateTime dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return false;
        }

        return dateTime1.getNanos() < dateTime2.getNanos();
    }

    /**
     * Evaluates whether one {@link DateTime} value is before or equal to a second {@link DateTime} value.
     *
     * @param dateTime1 first {@link DateTime}.
     * @param dateTime2 second {@link DateTime}.
     * @return true if dateTime1 is before or equal to dateTime2; otherwise, false if either value is null or if dateTime2
     *      is before dateTime1.
     */
    public static boolean isBeforeOrEqual(DateTime dateTime1, DateTime dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return false;
        }

        return dateTime1.getNanos() <= dateTime2.getNanos();
    }

    /**
     * Evaluates whether one {@link DateTime} value is after a second {@link DateTime} value.
     *
     * @param dateTime1 first {@link DateTime}.
     * @param dateTime2 second {@link DateTime}.
     * @return true if dateTime1 is after dateTime2; otherwise, false if either value is null or if dateTime2 is equal
     *      to or after dateTime1.
     */
    public static boolean isAfter(DateTime dateTime1, DateTime dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return false;
        }

        return dateTime1.getNanos() > dateTime2.getNanos();
    }

    /**
     * Evaluates whether one {@link DateTime} value is after or equal to a second {@link DateTime} value.
     *
     * @param dateTime1 first {@link DateTime}.
     * @param dateTime2 second {@link DateTime}.
     * @return true if dateTime1 is after or equal to dateTime2; otherwise, false if either value is null or if dateTime2
     *      is after dateTime1.
     */
    public static boolean isAfterOrEqual(DateTime dateTime1, DateTime dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return false;
        }

        return dateTime1.getNanos() >= dateTime2.getNanos();
    }

    // endregion

    // region Parse Times

    /**
     * Converts a String of digits of any length to a nanoseconds long value. Will ignore anything longer than 9 digits,
     * and will throw a NumberFormatException if any non-numeric character is found. Strings shorter than 9 digits will
     * be interpreted as sub-second values to the right of the decimal point.
     *
     * @param s string to convert.
     * @return value in nanoseconds.
     * @throws NumberFormatException if any non-numeric character is found.
     */
    private static long parseNanosInternal(@NotNull final String s) {
        long result = 0;
        for (int i = 0; i < 9; i++) {
            result *= 10;
            final int digit;
            if (i >= s.length()) {
                digit = 0;
            } else {
                digit = Character.digit(s.charAt(i), 10);
                if (digit < 0) {
                    throw new NumberFormatException("Invalid character for nanoseconds conversion: " + s.charAt(i));
                }
            }
            result += digit;
        }
        return result;
    }

    //TODO: think through toNanos vs toNanosQuiet
    /**
     * Converts a time string to nanoseconds. The format for the String is "hh:mm:ss[.nnnnnnnnn]".
     *
     * @param s string to be converted.
     * @return the number of nanoseconds represented by the string.
     * @throws RuntimeException if the string cannot be parsed.
     */
    public static long parseNanos(String s) {
        long ret = parseNanosQuiet(s);

        if (ret == NULL_LONG) {
            throw new RuntimeException("Cannot parse time : " + s);
        }

        return ret;
    }

    //TODO: think through toNanos vs toNanosQuiet
    /**
     * Converts a time string to nanoseconds. The format for the String is "hh:mm:ss[.nnnnnnnnn]".
     *
     * @param s string to be converted.
     * @return {@link QueryConstants#NULL_LONG} if the string cannot be parsed, otherwise the number of nanoseconds represented by the string.
     */
    public static long parseNanosQuiet(String s) {
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
                    subsecondNanos = parseNanosInternal(s.substring(decimalIndex + 1));

                    s = s.substring(0, decimalIndex);
                }

                String[] tokens = s.split(":");

                if (tokens.length == 2) { // hh:mm
                    return multiplier
                            * (1000000000L * (3600 * Integer.parseInt(tokens[0]) + 60 * Integer.parseInt(tokens[1]))
                            + dayNanos + subsecondNanos);
                } else if (tokens.length == 3) { // hh:mm:ss
                    return multiplier
                            * (1000000000L * (3600 * Integer.parseInt(tokens[0]) + 60 * Integer.parseInt(tokens[1])
                            + Integer.parseInt(tokens[2])) + dayNanos + subsecondNanos);
                }
            }
        } catch (Exception e) {
            // shouldn't get here too often, but somehow something snuck through. we'll just return null below...
        }

        return NULL_LONG;
    }

    //TODO: think through parseDateTime vs parseDateTimeQuiet
    /**
     * Converts a datetime string to a {@link DateTime}.
     * <p>
     * Supports ISO 8601 format ({@link DateTimeFormatter#ISO_INSTANT}), "yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ", and others.
     *
     * @param s string to be converted
     * @return a {@link DateTime} represented by the input string.
     * @throws RuntimeException if the string cannot be converted, otherwise a {@link DateTime} from the parsed String.
     */
    public static DateTime parseDateTime(String s) {
        DateTime ret = parseDateTimeQuiet(s);

        if (ret == null) {
            throw new RuntimeException("Cannot parse datetime : " + s);
        }

        return ret;
    }

    //TODO: think through parseDateTime vs parseDateTimeQuiet
    /**
     * Converts a datetime string to a {@link DateTime}.
     * <p>
     * Supports ISO 8601 format ({@link DateTimeFormatter#ISO_INSTANT}), "yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ", and others.
     *
     * @param s string to be converted.
     * @return a {@link DateTime} represented by the input string, or null if the format is not recognized or an exception occurs.
     */
    public static DateTime parseDateTimeQuiet(final String s) {
        try {
            return DateTime.of(Instant.parse(s));
        } catch (DateTimeParseException e) {
            // ignore
        }
        try {
            TimeZone timeZone = null;
            String dateTimeString = null;
            if (DATETIME_PATTERN.matcher(s).matches()) {
                int spaceIndex = s.indexOf(' ');
                if (spaceIndex == -1) { // no timezone
                    return null;
                }
                timeZone = TimeZone.valueOf("TZ_" + s.substring(spaceIndex + 1).trim().toUpperCase());
                dateTimeString = s.substring(0, spaceIndex);
            }

            if (timeZone == null) {
                return null;
            }
            int decimalIndex = dateTimeString.indexOf('.');
            if (decimalIndex == -1) {
                return new DateTime(
                        millisToNanos(new org.joda.time.DateTime(dateTimeString, timeZone.getTimeZone()).getMillis()));
            } else {
                final long subsecondNanos = parseNanosInternal(dateTimeString.substring(decimalIndex + 1));

                return new DateTime(millisToNanos(new org.joda.time.DateTime(dateTimeString.substring(0, decimalIndex),
                        timeZone.getTimeZone()).getMillis()) + subsecondNanos);
            }
        } catch (Exception e) {
            // shouldn't get here too often, but somehow something snuck through. we'll just return null below...
        }

        return null;
    }

    //TODO: think through parsePeriod vs parsePeriodQuiet
    /**
     * Converts a string into a time {@link Period}.
     *
     * @param s string in the form of "nYnMnWnDTnHnMnS", with n being numeric values, e.g. 1W for one week, T1M for
     *          one minute, 1WT1H for one week plus one hour.
     * @return time period {@link Period}.
     * @throws RuntimeException if the string cannot be parsed.
     */
    public static Period parsePeriod(String s) {
        Period ret = parsePeriodQuiet(s);

        if (ret == null) {
            throw new RuntimeException("Cannot parse period : " + s);
        }

        return ret;
    }

    //TODO: think through parsePeriod vs parsePeriodQuiet
    /**
     * Converts a string into a time {@link Period}.
     *
     * @param s a string in the form of "nYnMnWnDTnHnMnS", with n being numeric values, e.g. 1W for one week, T1M for
     *          one minute, 1WT1H for one week plus one hour.
     * @return a {@link Period} object, or null if the string can not be parsed.
     */
    public static Period parsePeriodQuiet(String s) {
        if (s.length() <= 1) {
            return null;
        }

        try {
            if (PERIOD_PATTERN.matcher(s).matches()) {
                return new Period(s);
            }
        } catch (Exception e) {
            // shouldn't get here too often, but somehow something snuck through. we'll just return null below...
        }

        return null;
    }

    // endregion

    // region Format Times

    /**
     * Returns a DateTime formatted as a "yyyy-MM-ddThh:mm:ss.SSSSSSSSS TZ" string.
     *
     * @param dateTime time to format as a string.
     * @param timeZone time zone to use when formatting the string.
     * @return null if either input is null; otherwise, the time formatted as a "yyyy-MM-ddThh:mm:ss.nnnnnnnnn TZ" string.
     */
    public static String formatDateTime(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return null;
        }

        return dateTime.toString(timeZone);
    }

    /**
     * Returns a DateTime formatted as a "yyyy-MM-dd" string.
     *
     * @param dateTime time to format as a string.
     * @param timeZone time zone to use when formatting the string.
     * @return null if either input is null; otherwise, the time formatted as a "yyyy-MM-ddThh" string.
    */
    public static String formatDate(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return null;
        }

        return dateTime.toDateString(timeZone);
    }

    /**
     * Returns nanoseconds formatted as a "dddThh:mm:ss.nnnnnnnnn" string.
     *
     * @param nanos nanoseconds.
     * @return the nanoseconds formatted as a "dddThh:mm:ss.nnnnnnnnn" string.
     */
    public static String formatNanos(long nanos) {
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

        buf.append(hours).append(':').append(pad(String.valueOf(minutes), 2)).append(':')
                .append(pad(String.valueOf(seconds), 2));

        if (nanos != 0) {
            buf.append('.').append(pad(String.valueOf(nanos), 9));
        }

        return buf.toString();
    }

    // endregion

    // region Chronology

    //TODO: no equivalent
    /**
     * Returns the number of nanoseconds that have elapsed since the top of the millisecond.
     *
     * @param dateTime time.
     * @return {@link QueryConstants#NULL_INT} if the input is null; otherwise, number of nanoseconds that have
     *      elapsed since the top of the millisecond.
     */
    public static int nanosOfMilli(DateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return (int) dateTime.getNanosPartial();
    }

    //TODO: no equivalent
    /**
     * Returns the number of microseconds that have elapsed since the top of the millisecond.
     * Nanoseconds are rounded, not dropped -- '20:41:39.123456700' has 457 micros, not 456.
     *
     * @param dateTime time.
     * @return {@link QueryConstants#NULL_INT} if the input is null; otherwise, number of microseconds that have
     *      elapsed since the top of the millisecond.
     */
    public static int microsOfMilli(DateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return (int) Math.round(dateTime.getNanosPartial() / 1000d);
    }

    /**
     * Returns the number of nanoseconds that have elapsed since the top of the second.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of nanoseconds that have
     *      elapsed since the top of the second.
     */
    public static long nanosOfSecond(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return NULL_LONG;
        }

        return millisToNanos(dateTime.getJodaDateTime(timeZone).getMillisOfSecond()) + dateTime.getNanosPartial();
    }

    /**
     * Returns the number of microseconds that have elapsed since the top of the second.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of microseconds that have
     *      elapsed since the top of the second.
     */
    public static long microsOfSecond(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return NULL_LONG;
        }

        return nanosToMicros(nanosOfSecond(dateTime, timeZone));
    }

    /**
     * Returns the number of milliseconds that have elapsed since the top of the second.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of milliseconds that have
     *      elapsed since the top of the second.
     */
    public static int millisOfSecond(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getMillisOfSecond();
    }

    /**
     * Returns the number of seconds that have elapsed since the top of the minute.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of seconds that have
     *      elapsed since the top of the minute.
     */
    public static int secondOfMinute(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getSecondOfMinute();
    }

    /**
     * Returns the number of minutes that have elapsed since the top of the hour.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of minutes that have
     *      elapsed since the top of the hour.
     */
    public static int minuteOfHour(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getMinuteOfHour();
    }

    /**
     * Returns the number of nanoseconds that have elapsed since the top of the day.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of nanoseconds that have
     *      elapsed since the top of the day.
     */
    public static long nanosOfDay(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return NULL_LONG;
        }

        return millisToNanos(dateTime.getJodaDateTime(timeZone).getMillisOfDay()) + dateTime.getNanosPartial();
    }

    /**
     * Returns the number of milliseconds that have elapsed since the top of the day.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of milliseconds that have
     *      elapsed since the top of the day.
     */
    public static int millisOfDay(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getMillisOfDay();
    }

    /**
     * Returns the number of seconds that have elapsed since the top of the day.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of seconds that have
     *      elapsed since the top of the day.
     */
    public static int secondOfDay(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getSecondOfDay();
    }

    /**
     * Returns the number of minutes that have elapsed since the top of the day.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of minutes that have
     *      elapsed since the top of the day.
     */
    public static int minuteOfDay(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getMinuteOfDay();
    }

    /**
     * Returns the number of hours that have elapsed since the top of the day.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of hours that have
     *      elapsed since the top of the day.
     */
    public static int hourOfDay(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getHourOfDay();
    }

    /**
     * Returns a 1-based int value of the day of the week for a {@link DateTime} in the specified time zone, with 1 being
     * Monday and 7 being Sunday.
     *
     * @param dateTime time to find the day of the month of.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, the day of the week.
     */
    public static int dayOfWeek(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getDayOfWeek();
    }

    /**
     * Returns a 1-based int value of the day of the month for a {@link DateTime} and specified time zone.
     * The first day of the month returns 1, the second day returns 2, etc.
     *
     * @param dateTime time to find the day of the month of.
     * @param timeZone time zone.
     * @return A {@link QueryConstants#NULL_INT} if either input is null; otherwise, the day of the month.
     */
    public static int dayOfMonth(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getDayOfMonth();
    }


    /**
     * Returns a 1-based int value of the day of the year (Julian date) for a {@link DateTime} in the specified time zone.
     * The first day of the year returns 1, the second day returns 2, etc.
     *
     * @param dateTime time to find the day of the month of.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, the day of the year.
     */
    public static int dayOfYear(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getDayOfYear();
    }

    /**
     * Returns a 1-based int value of the month of the year (Julian date) for a {@link DateTime} in the specified time zone.
     * January is 1, February is 2, etc.
     *
     * @param dateTime time to find the day of the month of.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, the month of the year.
     */
    public static int monthOfYear(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getMonthOfYear();
    }

    /**
     * Returns the year for a {@link DateTime} in the specified time zone.
     *
     * @param dateTime time to find the day of the month of.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, the year.
     */
    public static int year(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getYear();
    }

    /**
     * Returns the year of the century (two-digit year) for a {@link DateTime} in the specified time zone.
     *
     * @param dateTime time to find the day of the month of.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, the year of the century (two-digit year).
     */
    public static int yearOfCentury(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getJodaDateTime(timeZone).getYearOfCentury();
    }

    // endregion

    // region Time Bins

    /**
     * Returns a {@link DateTime} value, which is at the starting (lower) end of a time range defined by the interval
     * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the {@link DateTime} value for the start of the
     * five minute window that contains the input date time.
     *
     * @param dateTime {@link DateTime} for which to evaluate the start of the containing window.
     * @param intervalNanos size of the window in nanoseconds.
     * @return null if either input is null; otherwise, a {@link DateTime} representing the start of the window.
     */
    public static DateTime lowerBin(DateTime dateTime, long intervalNanos) {
        if (dateTime == null || intervalNanos == NULL_LONG) {
            return null;
        }

        return nanosToDateTime(Numeric.lowerBin(dateTime.getNanos(), intervalNanos));
    }

    /**
     * Returns a {@link DateTime} value, which is at the starting (lower) end of a time range defined by the interval
     * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the {@link DateTime} value for the start of the
     * five minute window that contains the input date time.
     *
     * @param dateTime {@link DateTime} for which to evaluate the start of the containing window.
     * @param intervalNanos size of the window in nanoseconds.
     * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
     *        one minute.
     * @return null if either input is null; otherwise, a {@link DateTime} representing the start of the window.
     */
    public static DateTime lowerBin(DateTime dateTime, long intervalNanos, long offset) {
        if (dateTime == null || intervalNanos == NULL_LONG || offset == NULL_LONG) {
            return null;
        }

        return nanosToDateTime(Numeric.lowerBin(dateTime.getNanos() - offset, intervalNanos) + offset);
    }

    /**
     * Returns a {@link DateTime} value, which is at the ending (upper) end of a time range defined by the interval
     * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the {@link DateTime} value for the end of the
     * five minute window that contains the input date time.
     *
     * @param dateTime {@link DateTime} for which to evaluate the start of the containing window.
     * @param intervalNanos size of the window in nanoseconds.
     * @return null if either input is null; otherwise, a {@link DateTime} representing the end of the window.
     */
    public static DateTime upperBin(DateTime dateTime, long intervalNanos) {
        if (dateTime == null || intervalNanos == NULL_LONG) {
            return null;
        }

        return nanosToDateTime(Numeric.upperBin(dateTime.getNanos(), intervalNanos));
    }

    /**
     * Returns a {@link DateTime} value, which is at the ending (upper) end of a time range defined by the interval
     * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the {@link DateTime} value for the end of the
     * five minute window that contains the input date time.
     *
     * @param dateTime {@link DateTime} for which to evaluate the start of the containing window.
     * @param intervalNanos size of the window in nanoseconds.
     * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
     *        one minute.
     * @return null if either input is null; otherwise, a {@link DateTime} representing the end of the window.
     */
    public static DateTime upperBin(DateTime dateTime, long intervalNanos, long offset) {
        if (dateTime == null || intervalNanos == NULL_LONG
                || offset == NULL_LONG) {
            return null;
        }

        return nanosToDateTime(Numeric.upperBin(dateTime.getNanos() - offset, intervalNanos) + offset);
    }

    // endregion

    // ##############################################################################


    //TODO: no equivalent.  Document and move out?
    public static final DateTime[] ZERO_LENGTH_DATETIME_ARRAY = new DateTime[0];


    //TODO: document
    //TODO: add to Format Patterns?
    /**
     * Date formatting styles for use in conversion functions such as {@link #convertDateQuiet(String, DateStyle)}.
     */
    public enum DateStyle {
        MDY, DMY, YMD
    }

    //TODO: add to Format Patterns?
    private static final DateStyle DATE_STYLE = DateStyle
            .valueOf(Configuration.getInstance().getStringWithDefault("DateTimeUtils.dateStyle", DateStyle.MDY.name()));



    private enum DateGroupId {
        // Date(1),
        Year(2, ChronoField.YEAR), Month(3, ChronoField.MONTH_OF_YEAR), Day(4, ChronoField.DAY_OF_MONTH),
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




    //TODO: no equivalent
    /**
     * Returns nanoseconds since Epoch for an {@link Instant} value.
     *
     * @param instant The {@link Instant} for which the nanoseconds offset should be returned.
     * @return A long value of nanoseconds since Epoch, or a NULL_LONG value if the {@link Instant} is null.
     */
    public static long nanos(Instant instant) {
        if (instant == null) {
            return NULL_LONG;
        }
        return Math.addExact(TimeUnit.SECONDS.toNanos(instant.getEpochSecond()), instant.getNano());
    }


    /**
     * Returns a {@link DateTime} for the requested {@link DateTime} at midnight in the specified time zone.
     *
     * @param dateTime {@link DateTime} for which the new value at midnight should be calculated.
     * @param timeZone {@link TimeZone} for which the new value at midnight should be calculated.
     * @return A null {@link DateTime} if either input is null, otherwise a {@link DateTime} representing midnight for
     *         the date and time zone of the inputs.
     */
    public static DateTime dateTimeAtMidnight(DateTime dateTime, TimeZone timeZone) {
        if (dateTime == null || timeZone == null) {
            return null;
        }

        return new DateTime(millisToNanos(new DateMidnight(dateTime.getMillis(), timeZone.getTimeZone()).getMillis())
                + dateTime.getNanosPartial());
    }

    //TODO: no equivalent
    //TODO: remove or support
    /**
     * Returns a {@link DateTime} representing midnight in a selected time zone on the date specified by a number of
     * milliseconds from Epoch.
     *
     * @param millis A long value of the number of milliseconds from Epoch for which the {@link DateTime} is to be
     *        calculated.
     * @param timeZone {@link TimeZone} for which the new value at midnight should be calculated.
     * @return A {@link DateTime} rounded down to midnight in the selected time zone for the specified number of
     *         milliseconds from Epoch.
     */
    @SuppressWarnings("WeakerAccess")
    public static DateTime millisToDateAtMidnight(final long millis, final TimeZone timeZone) {
        if (millis == NULL_LONG) {
            return null;
        }

        return new DateTime(millisToNanos(new DateMidnight(millis, timeZone.getTimeZone()).getMillis()));
    }

    static String pad(@NotNull final String str, final int length) {
        if (length <= str.length()) {
            return str;
        }
        return "0".repeat(length - str.length()) + str;
    }


    // region Base and Unit conversion





    private static long safeComputeNanos(long epochSecond, long nanoOfSecond) {
        if (epochSecond >= MAX_CONVERTIBLE_SECONDS) {
            throw new IllegalArgumentException("Numeric overflow detected during conversion of " + epochSecond
                    + " to nanoseconds");
        }

        return epochSecond * 1_000_000_000L + nanoOfSecond;
    }

    //TODO: no equivalent
    /**
     * Convert the specified instant to nanoseconds since epoch, or {@link QueryConstants#NULL_LONG null}.
     *
     * @param value the instant to convert
     *
     * @return nanoseconds since epoch or {@link QueryConstants#NULL_LONG null}
     */
    public static long toEpochNano(@Nullable final Instant value) {
        if (value == null) {
            return NULL_LONG;
        }

        return safeComputeNanos(value.getEpochSecond(), value.getNano());
    }

    //TODO: no equivalent
    /**
     * Convert the specified {@link ZonedDateTime} to nanoseconds since epoch, or {@link QueryConstants#NULL_LONG null}.
     *
     * @param value the instant to convert
     *
     * @return nanoseconds since epoch or {@link QueryConstants#NULL_LONG null}
     */
    public static long toEpochNano(@Nullable final ZonedDateTime value) {
        if (value == null) {
            return NULL_LONG;
        }

        return safeComputeNanos(value.toEpochSecond(), value.getNano());
    }

    //TODO: no equivalent
    /**
     * Convert nanos since epoch to an {@link Instant} value.
     *
     * @param nanos nanoseconds since epoch
     * @return a new {@link Instant} or null if nanos was {@link QueryConstants#NULL_LONG}.
     */
    @Nullable
    public static Instant makeInstant(final long nanos) {
        return nanos == NULL_LONG ? null : Instant.ofEpochSecond(nanos / 1_000_000_000L, nanos % 1_000_000_000L);
    }

    //TODO: no equivalent
    /**
     * Converts nanos of epoch to a {@link ZonedDateTime} using the {@link TimeZone#TZ_DEFAULT default} time zone.
     *
     * @param nanos nanoseconds since epoch
     * @return a new {@link ZonedDateTime} or null if nanos was {@link QueryConstants#NULL_LONG}.
     */
    @Nullable
    public static ZonedDateTime makeZonedDateTime(final long nanos) {
        return makeZonedDateTime(nanos, TimeZone.TZ_DEFAULT.getZoneId());
    }

    //TODO: no equivalent
    /**
     * Converts nanos of epoch to a {@link ZonedDateTime}.
     *
     * @param nanos nanoseconds since epoch
     * @param timeZone the {@link TimeZone time zone}
     *
     * @return a new {@link ZonedDateTime} or null if nanos was {@link QueryConstants#NULL_LONG}.
     */
    @Nullable
    public static ZonedDateTime makeZonedDateTime(final long nanos, @NotNull final TimeZone timeZone) {
        return makeZonedDateTime(nanos, timeZone.getZoneId());
    }

    //TODO: no equivalent
    /**
     * Converts nanos of epoch to a {@link ZonedDateTime}.
     *
     * @param nanos nanoseconds since epoch
     * @param zone the {@link ZoneId time zone}
     *
     * @return a new {@link ZonedDateTime} or null if nanos was {@link QueryConstants#NULL_LONG}.
     */
    @Nullable
    public static ZonedDateTime makeZonedDateTime(final long nanos, ZoneId zone) {
        // noinspection ConstantConditions
        return nanos == NULL_LONG ? null : ZonedDateTime.ofInstant(makeInstant(nanos), zone);
    }

    //TODO: no equivalent
    /**
     * Converts a {@link DateTime} to a {@link ZonedDateTime}.
     *
     * @param dateTime The a {@link DateTime} to convert.
     * @return A {@link ZonedDateTime} using the default time zone for the session as indicated by
     *         {@link TimeZone#TZ_DEFAULT}.
     */
    @Nullable
    public static ZonedDateTime getZonedDateTime(final @Nullable DateTime dateTime) {
        return getZonedDateTime(dateTime, TimeZone.TZ_DEFAULT);
    }

    //TODO: no equivalent
    /**
     * Converts a {@link DateTime} to a {@link ZonedDateTime}.
     *
     * @param dateTime The a {@link DateTime} to convert.
     * @param timeZone The {@link TimeZone} to use for the conversion.
     * @return A {@link ZonedDateTime} using the specified time zone. or null if dateTime was null
     */
    @Nullable
    public static ZonedDateTime getZonedDateTime(@Nullable final DateTime dateTime, @NotNull final TimeZone timeZone) {
        if (dateTime == null) {
            return null;
        }

        final ZoneId zone = timeZone.getTimeZone().toTimeZone().toZoneId();
        return dateTime.toZonedDateTime(zone);
    }

    //TODO: no equivalent
    /**
     * Converts a {@link DateTime} to a {@link ZonedDateTime}.
     *
     * @param dateTime The a {@link DateTime} to convert.
     * @param timeZone The {@link ZoneId} to use for the conversion.
     * @return A {@link ZonedDateTime} using the specified time zone. or null if dateTime was null
     */
    @Nullable
    public static ZonedDateTime getZonedDateTime(@Nullable final DateTime dateTime, @NotNull final ZoneId timeZone) {
        if (dateTime == null) {
            return null;
        }

        return dateTime.toZonedDateTime(timeZone);
    }

    //TODO: no equivalent
    /**
     * Converts a {@link ZonedDateTime} to a {@link DateTime}.
     *
     * @param zonedDateTime The a {@link ZonedDateTime} to convert.
     * @throws DateTimeOverflowException if the input is out of the range for a {@link DateTime}, otherwise, a
     *         {@link DateTime} version of the input.
     */
    @Nullable
    public static DateTime toDateTime(@Nullable final ZonedDateTime zonedDateTime) {
        if (zonedDateTime == null) {
            return null;
        }

        int nanos = zonedDateTime.getNano();
        long seconds = zonedDateTime.toEpochSecond();

        long limit = (Long.MAX_VALUE - nanos) / DateTimeUtils.SECOND;
        if (seconds >= limit) {
            throw new DateTimeOverflowException("Overflow: cannot convert " + zonedDateTime + " to new DateTime");
        }

        return new DateTime(nanos + (seconds * DateTimeUtils.SECOND));
    }
    // endregion

    // region Query Helper Methods

    private abstract static class CachedDate {

        final TimeZone timeZone;

        String value;
        long valueExpirationTimeMillis;

        private CachedDate(@NotNull final TimeZone timeZone) {
            this.timeZone = timeZone;
        }

        private TimeZone getTimeZone() {
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

        private CachedCurrentDate(@NotNull final TimeZone timeZone) {
            super(timeZone);
        }

        @Override
        void update(final long currentTimeMillis) {
            value = formatDate(millisToDateTime(currentTimeMillis), timeZone);
            valueExpirationTimeMillis = new org.joda.time.DateTime(currentTimeMillis, timeZone.getTimeZone())
                    .withFieldAdded(DurationFieldType.days(), 1).withTimeAtStartOfDay().getMillis();
        }
    }

    private static class CachedDateKey<CACHED_DATE_TYPE extends CachedDate>
            extends KeyedObjectKey.Basic<TimeZone, CACHED_DATE_TYPE> {

        @Override
        public TimeZone getKey(final CACHED_DATE_TYPE cachedDate) {
            return cachedDate.timeZone;
        }
    }

    private static final KeyedObjectHashMap<TimeZone, CachedCurrentDate> cachedCurrentDates =
            new KeyedObjectHashMap<>(new CachedDateKey<CachedCurrentDate>());

    //TODO: move to clock
    //TODO: make use clock
    //TODO: this cache could be total garbage!
    //TODO: no equivalent
    /**
     * Returns a String of the current date in the specified {@link TimeZone}.
     *
     * @param timeZone The {@link TimeZone} to reference when evaluating the current date for "now".
     * @return A String in format yyyy-MM-dd.
     */
    public static String currentDate(TimeZone timeZone) {
        return cachedCurrentDates.putIfAbsent(timeZone, CachedCurrentDate::new).get();
    }


    //TODO: no equivalent
    /**
     * Converts a long offset from Epoch value to a {@link DateTime}. This method uses expected date ranges to infer
     * whether the passed value is in milliseconds, microseconds, or nanoseconds. Thresholds used are
     * {@link TimeConstants#MICROTIME_THRESHOLD} divided by 1000 for milliseconds, as-is for microseconds, and
     * multiplied by 1000 for nanoseconds. The value is tested to see if its ABS exceeds the threshold. E.g. a value
     * whose ABS is greater than 1000 * {@link TimeConstants#MICROTIME_THRESHOLD} will be treated as nanoseconds.
     *
     * @param epoch The long Epoch offset value to convert.
     * @return null, if the input is equal to {@link QueryConstants#NULL_LONG}, otherwise a {@link DateTime} based on
     *         the inferred conversion.
     */
    @SuppressWarnings("WeakerAccess")
    public static DateTime autoEpochToTime(long epoch) {
        return new DateTime(autoEpochToNanos(epoch));
    }

    //TODO: no equivalent
    /**
     * Converts a long offset from Epoch value to a nanoseconds as a long. This method uses expected date ranges to
     * infer whether the passed value is in milliseconds, microseconds, or nanoseconds. Thresholds used are
     * {@link TimeConstants#MICROTIME_THRESHOLD} divided by 1000 for milliseconds, as-is for microseconds, and
     * multiplied by 1000 for nanoseconds. The value is tested to see if its ABS exceeds the threshold. E.g. a value
     * whose ABS is greater than 1000 * {@link TimeConstants#MICROTIME_THRESHOLD} will be treated as nanoseconds.
     *
     * @param epoch The long Epoch offset value to convert.
     * @return null, if the input is equal to {@link QueryConstants#NULL_LONG}, otherwise a nanoseconds value
     *         corresponding to the passed in epoch value.
     */
    public static long autoEpochToNanos(final long epoch) {
        if (epoch == NULL_LONG) {
            return epoch;
        }
        final long absEpoch = Math.abs(epoch);
        if (absEpoch > 1000 * TimeConstants.MICROTIME_THRESHOLD) { // Nanoseconds
            return epoch;
        }
        if (absEpoch > TimeConstants.MICROTIME_THRESHOLD) { // Microseconds
            return 1000 * epoch;
        }
        if (absEpoch > TimeConstants.MICROTIME_THRESHOLD / 1000) { // Milliseconds
            return 1000 * 1000 * epoch;
        }
        // Seconds
        return 1000 * 1000 * 1000 * epoch;
    }

    //TODO: no equivalent
    /**
     * Returns a {@link DateTime} value based on a starting value and a {@link Period} to add to it, but with a cap max
     * value which is returned in case the starting value plus period exceeds the cap.
     *
     * @param original The starting {@link DateTime} value.
     * @param period The {@link Period} to add to dateTime.
     * @param cap A {@link DateTime} value to use as the maximum return value.
     * @return a null {@link DateTime} if either original or period are null; the starting {@link DateTime} plus the
     *         specified period, if the result is not too large for a DateTime and does not exceed the cap value; the
     *         cap value if this is less than offset plus period. Throws a {@link DateTimeOverflowException
     *         DateTimeOverflowException} if the resultant value is more than max long nanoseconds from Epoch.
     */
    public static DateTime cappedTimeOffset(DateTime original, Period period, DateTime cap) {
        DateTime offset = DateTimeUtils.plus(original, period);
        return (offset.compareTo(cap) > 0) ? cap : offset;
    }

    // endregion

    //TODO: no equivalent
    /**
     * Converts an expression, replacing DateTime and Period literals with references to constant DateTime/Period
     * instances.
     *
     * @param formula The formula to convert.
     * @return A {@link Result} object, which includes the converted formula string, a string of instance variable
     *         declarations, and a map describing the names and types of these instance variables.
     *
     * @throws Exception If any error occurs or a literal value cannot be parsed.
     */
    // TODO: This should probably be handled in LanguageParser.accept(CharLiteralExpr, StringBuilder).
    public static Result convertExpression(String formula) throws Exception { // TODO: Why throw Exception?
        final StringBuilder instanceVariablesString = new StringBuilder();
        final HashMap<String, Class<?>> newVariables = new HashMap<>();

        final StringBuilder convertedFormula = new StringBuilder();

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

            if (parseDateTimeQuiet(s) != null) {
                matcher.appendReplacement(convertedFormula, "_date" + dateTimeIndex);
                instanceVariablesString.append("        private DateTime _date").append(dateTimeIndex)
                        .append("=DateTimeUtils.toDateTime(\"")
                        .append(formula, matcher.start() + 1, matcher.end() - 1).append("\");\n");
                newVariables.put("_date" + dateTimeIndex, DateTime.class);

                dateTimeIndex++;
            } else if (convertDateQuiet(s) != null) {
                matcher.appendReplacement(convertedFormula, "_localDate" + localDateIndex);
                instanceVariablesString.append("        private java.time.LocalDate _localDate").append(localDateIndex)
                        .append("=DateTimeUtils.convertDate(\"").append(formula, matcher.start() + 1, matcher.end() - 1)
                        .append("\");\n");
                newVariables.put("_localDate" + localDateIndex, LocalDate.class);
                localDateIndex++;
            } else if (parseNanosQuiet(s) != NULL_LONG) {
                matcher.appendReplacement(convertedFormula, "_time" + timeIndex);
                instanceVariablesString.append("        private long _time").append(timeIndex)
                        .append("=DateTimeUtils.convertTime(\"").append(formula, matcher.start() + 1, matcher.end() - 1)
                        .append("\");\n");
                newVariables.put("_time" + timeIndex, long.class);

                timeIndex++;
            } else if (parsePeriodQuiet(s) != null) {
                matcher.appendReplacement(convertedFormula, "_period" + periodIndex);
                instanceVariablesString.append("        private Period _period").append(periodIndex)
                        .append("=DateTimeUtils.convertPeriod(\"")
                        .append(formula, matcher.start() + 1, matcher.end() - 1)
                        .append("\");\n");
                newVariables.put("_period" + periodIndex, Period.class);

                periodIndex++;
            } else if (convertLocalTimeQuiet(s) != null) {
                matcher.appendReplacement(convertedFormula, "_localTime" + timeIndex);
                instanceVariablesString.append("        private java.time.LocalTime _localTime").append(timeIndex)
                        .append("=DateTimeUtils.convertLocalTime(\"")
                        .append(formula, matcher.start() + 1, matcher.end() - 1).append("\");\n");
                newVariables.put("_localTime" + timeIndex, LocalTime.class);
                timeIndex++;
            } else {
                throw new Exception("Cannot parse datetime/time/period : " + s);
            }
        }

        matcher.appendTail(convertedFormula);

        return new Result(convertedFormula.toString(), instanceVariablesString.toString(), newVariables);
    }

    //TODO: no equivalent
    /**
     * Converts a String date/time to nanoseconds from Epoch or a nanoseconds period. Three patterns are supported:
     * <p>
     * yyyy-MM-ddThh:mm:ss[.nnnnnnnnn] TZ for date/time values
     * </p>
     * <p>
     * hh:mm:ss[.nnnnnnnnn] for time values
     * </p>
     * <p>
     * Period Strings in the form of numbertype, e.g. 1W for one week, and Tnumbertype for times, e.g. T1M for one
     * minute
     * </p>
     *
     * @param formula The String to be evaluated and converted. Optionally, but preferred, enclosed in straight single
     *        ticks.
     * @return A long value representing an Epoch offset in nanoseconds for a time or date/time, or a duration in
     *         nanoseconds for a period. Throws {@link DateTimeOverflowException} if the resultant value would be longer
     *         than max long, or {@link IllegalArgumentException} if expression cannot be evaluated.
     */
    public static long expressionToNanos(String formula) {
        if (!formula.startsWith("'")) {
            formula = '\'' + formula + '\'';
        }
        Matcher matcher = Pattern.compile("'[^'][^']+'").matcher(formula);

        boolean result = matcher.find();

        String s = formula.substring(matcher.start() + 1, matcher.end() - 1);
        final DateTime dateTime = parseDateTimeQuiet(s);
        if (dateTime != null) {
            return dateTime.getNanos();
        }
        long time = parseNanosQuiet(s);
        if (time != NULL_LONG) {
            return time;
        }
        final Period period = parsePeriodQuiet(s);
        if (period != null) {
            try {
                return StrictMath.multiplyExact(period.getJodaPeriod().toStandardDuration().getMillis(),
                        period.isPositive() ? 1_000_000L : -1_000_000L);
            } catch (ArithmeticException ex) {
                throw new DateTimeOverflowException("Period length in nanoseconds exceeds Long.MAX_VALUE : " + s, ex);
            }
        }
        throw new IllegalArgumentException("Cannot parse datetime/time/period : " + s);
    }

    //TODO: no equivalent
    /**
     * Attempt to convert the given string to a LocalDate. This should <b>not</b> accept dates with times, as we want
     * those to be interpreted as DateTime values. The ideal date format is YYYY-MM-DD since it's the least ambiguous,
     * but this method also parses slash-delimited dates according to the system "date style".
     *
     * @param s the date string to convert
     * @throws RuntimeException if the date cannot be converted, otherwise returns a {@link LocalDate}
     */
    @SuppressWarnings("WeakerAccess")
    public static LocalDate convertDate(String s) {
        final LocalDate ret = convertDateQuiet(s);

        if (ret == null) {
            throw new RuntimeException("Cannot parse date : " + s);
        }

        return ret;
    }


    //TODO: no equivalent
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
                final int hour = Integer.parseInt(matcher.group(1)); // hour is the only required field
                final int minute = matcher.group(2) != null ? Integer.parseInt(matcher.group(2)) : 0;
                final int second = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : 0;
                final int nanos;
                if (matcher.group(4) != null) {
                    final String fractionStr = matcher.group(5); // group 5 excludes the decimal pt
                    nanos = Integer.parseInt(fractionStr) * (int) Math.pow(10, 9 - fractionStr.length());
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

    //TODO: no equivalent
    /**
     * Attempt to convert the given string to a LocalDate. This should <b>not</b> accept dates with times, as we want
     * those to be interpreted as DateTime values. The ideal date format is YYYY-MM-DD since it's the least ambiguous.
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
     * Attempt to convert the given string to a LocalDate. This should <b>not</b> accept dates with times, as we want
     * those to be interpreted as DateTime values. The ideal date format is YYYY-MM-DD since it's the least ambiguous.
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

            // see if we can match one of the slash-delimited styles, the interpretation of which requires knowing the
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
                    year = Year.parse(slashMatcher.group(yearGroup), TWO_DIGIT_YR_FORMAT).getValue();
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




    //TODO: no equivalent
    /**
     * Returns a {@link ChronoField} indicating the level of precision in a String time value.
     *
     * @param timeDef The time String to evaluate.
     * @return null if the time String cannot be parsed, otherwise a {@link ChronoField} for the finest units in the
     *         String (e.g. "10:00:00" would yield SecondOfMinute).
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

    //TODO: does this need to be in this class?
    /**
     * A container object for the result of {@link #convertExpression(String)}, which includes the converted formula
     * String, a String of instance variable declarations, and a map describing the names and types of these instance
     * variables.
     */
    public static class Result {
        private final String convertedFormula;
        private final String instanceVariablesString;
        private final HashMap<String, Class<?>> newVariables;

        public Result(String convertedFormula, String instanceVariablesString, HashMap<String, Class<?>> newVariables) {
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

        public HashMap<String, Class<?>> getNewVariables() {
            return newVariables;
        }
    }


    //TODO: no equivalent
    /**
     * Create a DateTimeFormatter formatter with the specified time zone name using the standard yyyy-MM-dd format.
     *
     * @param timeZoneName the time zone name
     * @return a formatter set for the specified time zone
     */
    public static DateTimeFormatter createFormatter(final String timeZoneName) {
        final ZoneId zoneId = ZoneId.of(timeZoneName);
        return DateTimeFormatter.ofPattern(DATE_COLUMN_PARTITION_FORMAT_STRING).withZone(zoneId);
    }

    //TODO: no equivalent
    /**
     * Given a DateTimeFormatter and a timestamp in millis, return the date as a String in standard column-partition
     * format of yyyy-MM-dd. A timestamp of NULL_LONG means use the system current time.
     *
     * @param dateTimeFormatter the date formatter
     * @param timestampMillis the timestamp in millis
     * @return the formatted date
     */
    public static String getPartitionFromTimestampMillis(@NotNull final DateTimeFormatter dateTimeFormatter,
            final long timestampMillis) {
        if (timestampMillis == NULL_LONG) {
            return dateTimeFormatter.format(Instant.ofEpochMilli(System.currentTimeMillis()));
        }
        return dateTimeFormatter.format(Instant.ofEpochMilli(timestampMillis));
    }

    //TODO: no equivalent
    /**
     * Given a DateTimeFormatter and a timestamp in micros from epoch, return the date as a String in standard
     * column-partition format of yyyy-MM-dd. A timestamp of NULL_LONG means use the system current time.
     *
     * @param dateTimeFormatter the date formatter
     * @param timestampMicros the timestamp in micros
     * @return the formatted date
     */
    public static String getPartitionFromTimestampMicros(@NotNull final DateTimeFormatter dateTimeFormatter,
            final long timestampMicros) {
        if (timestampMicros == NULL_LONG) {
            return dateTimeFormatter.format(Instant.ofEpochMilli(System.currentTimeMillis()));
        }
        return dateTimeFormatter.format(Instant.ofEpochMilli(timestampMicros / 1_000));
    }

    //TODO: no equivalent
    /**
     * Given a DateTimeFormatter and a timestamp in nanos from epoch, return the date as a String in standard
     * column-partition format of yyyy-MM-dd. A timestamp of NULL_LONG means use the system current time.
     *
     * @param dateTimeFormatter the date formatter
     * @param timestampNanos the timestamp in nanos
     * @return the formatted date
     */
    public static String getPartitionFromTimestampNanos(@NotNull final DateTimeFormatter dateTimeFormatter,
            final long timestampNanos) {
        if (timestampNanos == NULL_LONG) {
            return dateTimeFormatter.format(Instant.ofEpochMilli(System.currentTimeMillis()));
        }
        return dateTimeFormatter.format(Instant.ofEpochMilli(timestampNanos / 1_000_000));
    }

    //TODO: no equivalent
    /**
     * Given a DateTimeFormatter and a timestamp in seconds from epoch, return the date as a String in standard
     * column-partition format of yyyy-MM-dd. A timestamp of NULL_LONG means use the system current time.
     *
     * @param dateTimeFormatter the date formatter
     * @param timestampSeconds the timestamp in seconds
     * @return the formatted date
     */
    public static String getPartitionFromTimestampSeconds(@NotNull final DateTimeFormatter dateTimeFormatter,
            final long timestampSeconds) {
        if (timestampSeconds == NULL_LONG) {
            return dateTimeFormatter.format(Instant.ofEpochMilli(System.currentTimeMillis()));
        }
        return dateTimeFormatter.format(Instant.ofEpochMilli(timestampSeconds * 1_000));
    }
}
