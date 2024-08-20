//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.clock.TimeConstants;
import io.deephaven.function.Numeric;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.zone.ZoneRulesException;
import java.util.Date;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.deephaven.util.QueryConstants.*;
import static java.time.format.DateTimeFormatter.*;

/**
 * Functions for working with time.
 */
@SuppressWarnings({"RegExpRedundantEscape"})
public class DateTimeUtils {

    // region Format Patterns

    /**
     * Very permissive formatter / parser for local dates.
     */
    private static final DateTimeFormatter FORMATTER_ISO_LOCAL_DATE = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
            .toFormatter();

    /**
     * Very permissive formatter / parser for local times.
     */
    private static final DateTimeFormatter FORMATTER_ISO_LOCAL_TIME = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.HOUR_OF_DAY, 1, 2, SignStyle.NORMAL)
            .appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 1, 2, SignStyle.NORMAL)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 1, 2, SignStyle.NORMAL)
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter();

    /**
     * Very permissive formatter / parser for local date times.
     */
    private static final DateTimeFormatter FORMATTER_ISO_LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(FORMATTER_ISO_LOCAL_DATE)
            .appendLiteral('T')
            .append(FORMATTER_ISO_LOCAL_TIME)
            .toFormatter();

    /**
     * Matches long values.
     */
    private static final Pattern LONG_PATTERN = Pattern.compile("^-?\\d{1,19}$");

    /**
     * Matches dates with time zones.
     */
    private static final Pattern DATE_TZ_PATTERN = Pattern.compile(
            "(?<date>[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9])(?<t>[tT]?) (?<timezone>[a-zA-Z_/]+)");

    /**
     * Matches dates without time zones.
     */
    private static final Pattern LOCAL_DATE_PATTERN = Pattern.compile(
            "(?<date>[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9])(?<t>[tT]?)");


    /**
     * Matches time durations.
     */
    private static final Pattern TIME_DURATION_PATTERN = Pattern.compile(
            "(?<sign1>[-]?)[pP][tT](?<sign2>[-]?)(?<hour>[0-9]+):(?<minute>[0-9]+)(?<second>:[0-9]+)?(?<nanos>\\.[0-9][0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?)?");

    /**
     * Matches date times.
     */
    private static final Pattern CAPTURING_DATETIME_PATTERN = Pattern.compile(
            "(([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9])T?)?(([0-9][0-9]?)(?::([0-9][0-9])(?::([0-9][0-9]))?(?:\\.([0-9][0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?))?)?)?( [a-zA-Z]+)?");

    // endregion

    // region Constants

    /**
     * A zero length array of {@link Instant instants}.
     */
    public static final Instant[] ZERO_LENGTH_INSTANT_ARRAY = new Instant[0];

    /**
     * One microsecond in nanoseconds.
     */
    public static final long MICRO = 1_000;

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
     * One day in nanoseconds. This is one hour of wall time and does not take into account calendar adjustments.
     */
    public static final long DAY = 24 * HOUR;

    /**
     * One week in nanoseconds. This is 7 days of wall time and does not take into account calendar adjustments.
     */
    public static final long WEEK = 7 * DAY;

    /**
     * One 365 day year in nanoseconds. This is 365 days of wall time and does not take into account calendar
     * adjustments.
     */
    public static final long YEAR_365 = 365 * DAY;

    /**
     * One average year in nanoseconds. This is 365.2425 days of wall time and does not take into account calendar
     * adjustments.
     */
    public static final long YEAR_AVG = 31556952000000000L;

    /**
     * Maximum time in microseconds that can be converted to an instant without overflow.
     */
    private static final long MAX_CONVERTIBLE_MICROS = Long.MAX_VALUE / 1_000L;

    /**
     * Maximum time in milliseconds that can be converted to an instant without overflow.
     */
    private static final long MAX_CONVERTIBLE_MILLIS = Long.MAX_VALUE / 1_000_000L;

    /**
     * Maximum time in seconds that can be converted to an instant without overflow.
     */
    private static final long MAX_CONVERTIBLE_SECONDS = Long.MAX_VALUE / 1_000_000_000L;

    /**
     * Number of seconds per nanosecond.
     */
    public static final double SECONDS_PER_NANO = 1. / (double) SECOND;

    /**
     * Number of minutes per nanosecond.
     */
    public static final double MINUTES_PER_NANO = 1. / (double) MINUTE;

    /**
     * Number of hours per nanosecond.
     */
    public static final double HOURS_PER_NANO = 1. / (double) HOUR;

    /**
     * Number of days per nanosecond.
     */
    public static final double DAYS_PER_NANO = 1. / (double) DAY;

    /**
     * Number of 365 day years per nanosecond.
     */
    public static final double YEARS_PER_NANO_365 = 1. / (double) YEAR_365;

    /**
     * Number of average (365.2425 day) years per nanosecond.
     */
    public static final double YEARS_PER_NANO_AVG = 1. / (double) YEAR_AVG;

    // endregion

    // region Overflow / Underflow

    /**
     * Exception type thrown when operations on date time values would exceed the range available by max or min long
     * nanoseconds.
     */
    public static class DateTimeOverflowException extends RuntimeException {
        /**
         * Creates a new overflow exception.
         */
        @SuppressWarnings("unused")
        private DateTimeOverflowException() {
            super("Operation failed due to overflow");
        }

        /**
         * Creates a new overflow exception.
         *
         * @param cause the cause of the overflow
         */
        private DateTimeOverflowException(@NotNull final Throwable cause) {
            super("Operation failed due to overflow", cause);
        }

        /**
         * Creates a new overflow exception.
         *
         * @param message the error string
         */
        private DateTimeOverflowException(@NotNull final String message) {
            super(message);
        }

        /**
         * Creates a new overflow exception.
         *
         * @param message the error message
         * @param cause the cause of the overflow
         */
        @SuppressWarnings("unused")
        private DateTimeOverflowException(@NotNull final String message, @NotNull final Throwable cause) {
            super(message, cause);
        }
    }

    // + can only result in overflow if both positive or both negative
    @SuppressWarnings("SameParameterValue")
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

    // - can only result in overflow if one is positive and one is negative
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

    private static long safeComputeNanos(final long epochSecond, final long nanoOfSecond) {
        if (epochSecond >= MAX_CONVERTIBLE_SECONDS) {
            throw new DateTimeOverflowException("Numeric overflow detected during conversion of " + epochSecond
                    + " to nanoseconds");
        }

        return epochSecond * 1_000_000_000L + nanoOfSecond;
    }

    // endregion

    // region Clock

    // TODO(deephaven-core#3044): Improve scaffolding around full system replay
    /**
     * Clock used to compute the current time. This allows a custom clock to be used instead of the current system
     * clock. This is mainly used for replay simulations.
     */
    private static Clock clock;

    /**
     * Set the clock used to compute the current time. This allows a custom clock to be used instead of the current
     * system clock. This is mainly used for replay simulations.
     *
     * @param clock the clock used to compute the current time; if {@code null}, use the system clock
     */
    @ScriptApi
    public static void setClock(@Nullable final Clock clock) {
        DateTimeUtils.clock = clock;
    }

    /**
     * Returns the clock used to compute the current time. This may be the current system clock, or it may be an
     * alternative clock used for replay simulations.
     *
     * @return the current clock
     * @see #setClock(Clock)
     */
    @ScriptApi
    @NotNull
    public static Clock currentClock() {
        return Objects.requireNonNullElse(clock, Clock.system());
    }

    /**
     * Provides the current {@link Instant instant} with nanosecond resolution according to the {@link #currentClock()
     * current clock}. Under most circumstances, this method will return the current system time, but during replay
     * simulations, this method can return the replay time.
     *
     * @return the current instant with nanosecond resolution according to the current clock
     * @see #currentClock()
     * @see #setClock(Clock)
     * @see #nowSystem()
     */
    @ScriptApi
    @NotNull
    public static Instant now() {
        return currentClock().instantNanos();
    }

    /**
     * Provides the current {@link Instant instant} with millisecond resolution according to the {@link #currentClock()
     * current clock}. Under most circumstances, this method will return the current system time, but during replay
     * simulations, this method can return the replay time.
     *
     * @return the current instant with millisecond resolution according to the current clock
     * @see #currentClock()
     * @see #setClock(Clock)
     * @see #nowSystemMillisResolution()
     */
    @ScriptApi
    @NotNull
    public static Instant nowMillisResolution() {
        return currentClock().instantMillis();
    }

    /**
     * Provides the current {@link Instant instant} with nanosecond resolution according to the {@link Clock#system()
     * system clock}. Note that the system time may not be desirable during replay simulations.
     *
     * @return the current instant with nanosecond resolution according to the system clock
     * @see #now()
     */
    @ScriptApi
    @NotNull
    public static Instant nowSystem() {
        return Clock.system().instantNanos();
    }

    /**
     * Provides the current {@link Instant instant} with millisecond resolution according to the {@link Clock#system()
     * system clock}. Note that the system time may not be desirable during replay simulations.
     *
     * @return the current instant with millisecond resolution according to the system clock
     * @see #nowMillisResolution()
     */
    @ScriptApi
    @NotNull
    public static Instant nowSystemMillisResolution() {
        return Clock.system().instantMillis();
    }

    /**
     * A cached date in a specific timezone. The cache is invalidated when the current clock indicates the next day has
     * arrived.
     */
    private abstract static class CachedDate {

        final ZoneId timeZone;
        volatile LocalDate date;
        volatile String str;
        volatile long valueExpirationTimeMillis;

        private CachedDate(@NotNull final ZoneId timeZone) {
            this.timeZone = timeZone;
        }

        @SuppressWarnings("unused")
        private ZoneId getTimeZone() {
            return timeZone;
        }

        public LocalDate getLocalDate() {
            return getLocalDate(currentClock().currentTimeMillis());
        }

        public LocalDate getLocalDate(final long currentTimeMillis) {
            if (currentTimeMillis >= valueExpirationTimeMillis) {
                update(currentTimeMillis);
            }
            return date;
        }

        public String getStr() {
            return getStr(currentClock().currentTimeMillis());
        }

        public synchronized String getStr(final long currentTimeMillis) {
            if (currentTimeMillis >= valueExpirationTimeMillis) {
                update(currentTimeMillis);
            }
            return str;
        }

        // Update methods should be synchronized!
        abstract void update(long currentTimeMillis);
    }

    private static class CachedCurrentDate extends CachedDate {

        private CachedCurrentDate(@NotNull final ZoneId timeZone) {
            super(timeZone);
        }

        @Override
        synchronized void update(final long currentTimeMillis) {
            date = toLocalDate(epochMillisToInstant(currentTimeMillis), timeZone);
            str = formatDate(date);
            valueExpirationTimeMillis = epochMillis(date.plusDays(1).atStartOfDay(timeZone));
        }
    }

    private static class CachedDateKey<CACHED_DATE_TYPE extends CachedDate>
            extends KeyedObjectKey.Basic<ZoneId, CACHED_DATE_TYPE> {

        @Override
        public ZoneId getKey(final CACHED_DATE_TYPE cachedDate) {
            return cachedDate.timeZone;
        }
    }

    private static final KeyedObjectHashMap<ZoneId, CachedCurrentDate> cachedCurrentDates =
            new KeyedObjectHashMap<>(new CachedDateKey<>());

    private static CachedCurrentDate getCachedCurrentDate(@NotNull final ZoneId timeZone) {
        return cachedCurrentDates.putIfAbsent(timeZone, CachedCurrentDate::new);
    }

    /**
     * Provides the current date string according to the {@link #currentClock() current clock}. Under most
     * circumstances, this method will return the date according to current system time, but during replay simulations,
     * this method can return the date according to replay time.
     *
     * @param timeZone the time zone
     * @return the current date according to the current clock and time zone formatted as "yyyy-MM-dd". {@code null} if
     *         the input is {@code null}.
     * @see #currentClock()
     * @see #setClock(Clock)
     */
    @ScriptApi
    @Nullable
    public static String today(@Nullable final ZoneId timeZone) {
        if (timeZone == null) {
            return null;
        }

        return getCachedCurrentDate(timeZone).getStr();
    }

    /**
     * Provides the current date string according to the {@link #currentClock() current clock} and the
     * {@link ZoneId#systemDefault() default time zone}. Under most circumstances, this method will return the date
     * according to current system time, but during replay simulations, this method can return the date according to
     * replay time.
     *
     * @return the current date according to the current clock and default time zone formatted as "yyyy-MM-dd"
     * @see #currentClock()
     * @see #setClock(Clock)
     * @see ZoneId#systemDefault()
     */
    @ScriptApi
    @NotNull
    public static String today() {
        // noinspection ConstantConditions
        return today(DateTimeUtils.timeZone());
    }

    /**
     * Provides the current date according to the {@link #currentClock() current clock}. Under most circumstances, this
     * method will return the date according to current system time, but during replay simulations, this method can
     * return the date according to replay time.
     *
     * @param timeZone the time zone
     * @return the current date according to the current clock and time zone formatted as "yyyy-MM-dd". {@code null} if
     *         the input is {@code null}.
     * @see #currentClock()
     * @see #setClock(Clock)
     */
    @ScriptApi
    @Nullable
    public static LocalDate todayLocalDate(@Nullable final ZoneId timeZone) {
        if (timeZone == null) {
            return null;
        }

        return getCachedCurrentDate(timeZone).getLocalDate();
    }

    /**
     * Provides the current date according to the {@link #currentClock() current clock} and the
     * {@link ZoneId#systemDefault() default time zone}. Under most circumstances, this method will return the date
     * according to current system time, but during replay simulations, this method can return the date according to
     * replay time.
     *
     * @return the current date according to the current clock and default time zone formatted as "yyyy-MM-dd"
     * @see #currentClock()
     * @see #setClock(Clock)
     * @see ZoneId#systemDefault()
     */
    @ScriptApi
    @NotNull
    public static LocalDate todayLocalDate() {
        // noinspection ConstantConditions
        return todayLocalDate(DateTimeUtils.timeZone());
    }

    // endregion

    // region Time Zone

    /**
     * Gets the time zone for a time zone name.
     *
     * @param timeZone the time zone name
     * @return the corresponding time zone. {@code null} if the input is {@code null}.
     * @throws DateTimeException if {@code timeZone} has an invalid format
     * @throws ZoneRulesException if {@code timeZone} cannot be found
     */
    @ScriptApi
    @Nullable
    public static ZoneId timeZone(@Nullable String timeZone) {
        if (timeZone == null) {
            return null;
        }

        return TimeZoneAliases.zoneId(timeZone);
    }

    /**
     * Gets the {@link ZoneId#systemDefault() system default time zone}.
     *
     * @return the system default time zone
     * @see ZoneId#systemDefault()
     */
    @ScriptApi
    public static ZoneId timeZone() {
        return ZoneId.systemDefault();
    }

    /**
     * Adds a new time zone alias.
     *
     * @param alias the alias name
     * @param timeZone the time zone id name
     * @throws IllegalArgumentException if the alias already exists or the time zone is invalid
     */
    @ScriptApi
    public static void timeZoneAliasAdd(@NotNull final String alias, @NotNull final String timeZone) {
        TimeZoneAliases.addAlias(alias, timeZone);
    }

    /**
     * Removes a time zone alias.
     *
     * @param alias the alias name
     * @return whether {@code alias} was present
     */
    @ScriptApi
    public static boolean timeZoneAliasRm(@NotNull final String alias) {
        return TimeZoneAliases.rmAlias(alias);
    }

    // endregion

    // region Conversions: Time Units

    /**
     * Converts microseconds to nanoseconds.
     *
     * @param micros the microseconds to convert
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         microseconds converted to nanoseconds
     */
    @ScriptApi
    public static long microsToNanos(final long micros) {
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
     * @param millis the milliseconds to convert
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         milliseconds converted to nanoseconds
     */
    @ScriptApi
    public static long millisToNanos(final long millis) {
        if (millis == NULL_LONG) {
            return NULL_LONG;
        }
        if (Math.abs(millis) > MAX_CONVERTIBLE_MILLIS) {
            throw new DateTimeOverflowException("Converting " + millis + " millis to nanos would overflow");
        }
        return millis * 1000000;
    }

    /**
     * Converts seconds to nanoseconds.
     *
     * @param seconds the seconds to convert
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         seconds converted to nanoseconds
     */
    @ScriptApi
    public static long secondsToNanos(final long seconds) {
        if (seconds == NULL_LONG) {
            return NULL_LONG;
        }
        if (Math.abs(seconds) > MAX_CONVERTIBLE_SECONDS) {
            throw new DateTimeOverflowException("Converting " + seconds + " seconds to nanos would overflow");
        }

        return seconds * 1000000000L;
    }

    /**
     * Converts nanoseconds to microseconds.
     *
     * @param nanos the nanoseconds to convert
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         nanoseconds converted to microseconds, rounded down
     */
    @ScriptApi
    public static long nanosToMicros(final long nanos) {
        if (nanos == NULL_LONG) {
            return NULL_LONG;
        }
        return nanos / MICRO;
    }

    /**
     * Converts milliseconds to microseconds.
     *
     * @param millis the milliseconds to convert
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         milliseconds converted to microseconds, rounded down
     */
    @ScriptApi
    public static long millisToMicros(final long millis) {
        if (millis == NULL_LONG) {
            return NULL_LONG;
        }
        return millis * 1000L;
    }

    /**
     * Converts seconds to microseconds.
     *
     * @param seconds the seconds to convert
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         seconds converted to microseconds, rounded down
     */
    @ScriptApi
    public static long secondsToMicros(final long seconds) {
        if (seconds == NULL_LONG) {
            return NULL_LONG;
        }
        return seconds * 1_000_000;
    }

    /**
     * Converts nanoseconds to milliseconds.
     *
     * @param nanos the nanoseconds to convert
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         nanoseconds converted to milliseconds, rounded down
     */
    @ScriptApi
    public static long nanosToMillis(final long nanos) {
        if (nanos == NULL_LONG) {
            return NULL_LONG;
        }

        return nanos / MILLI;
    }

    /**
     * Converts microseconds to milliseconds.
     *
     * @param micros the microseconds to convert
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         microseconds converted to milliseconds, rounded down
     */
    @ScriptApi
    public static long microsToMillis(final long micros) {
        if (micros == NULL_LONG) {
            return NULL_LONG;
        }

        return micros / 1000L;
    }

    /**
     * Converts seconds to milliseconds.
     *
     * @param seconds the nanoseconds to convert
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         seconds converted to milliseconds, rounded down
     */
    @ScriptApi
    public static long secondsToMillis(final long seconds) {
        if (seconds == NULL_LONG) {
            return NULL_LONG;
        }

        return seconds * 1_000L;
    }

    /**
     * Converts nanoseconds to seconds.
     *
     * @param nanos the nanoseconds to convert
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         nanoseconds converted to seconds, rounded down
     */
    @ScriptApi
    public static long nanosToSeconds(final long nanos) {
        if (nanos == NULL_LONG) {
            return NULL_LONG;
        }
        return nanos / SECOND;
    }

    /**
     * Converts microseconds to seconds.
     *
     * @param micros the microseconds to convert
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         microseconds converted to seconds, rounded down
     */
    @ScriptApi
    public static long microsToSeconds(final long micros) {
        if (micros == NULL_LONG) {
            return NULL_LONG;
        }
        return micros / 1_000_000L;
    }

    /**
     * Converts milliseconds to seconds.
     *
     * @param millis the milliseconds to convert
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         milliseconds converted to seconds, rounded down
     */
    @ScriptApi
    public static long millisToSeconds(final long millis) {
        if (millis == NULL_LONG) {
            return NULL_LONG;
        }
        return millis / 1_000L;
    }

    // endregion

    // region Conversions: Date Time Types


    /**
     * Converts a {@link ZonedDateTime} to an {@link Instant}.
     *
     * @param dateTime the zoned date time to convert
     * @return the {@link Instant}, or {@code null} if {@code dateTime} is {@code null}
     */
    @ScriptApi
    @Nullable
    public static Instant toInstant(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }

        return dateTime.toInstant();
    }

    /**
     * Converts a {@link LocalDateTime} and {@link ZoneId} to an {@link Instant}.
     *
     * @param localDateTime the local date time
     * @param timeZone the time zone
     * @return the {@link Instant}, or {@code null} if any input is {@code null}
     */
    @ScriptApi
    @Nullable
    public static Instant toInstant(
            @Nullable final LocalDateTime localDateTime,
            @Nullable final ZoneId timeZone) {
        if (localDateTime == null || timeZone == null) {
            return null;
        }

        return localDateTime // LocalDateTime
                .atZone(timeZone) // ZonedDateTime
                .toInstant(); // Instant
    }

    /**
     * Converts a {@link LocalDate}, {@link LocalTime}, and {@link ZoneId} to an {@link Instant}.
     *
     * @param date the local date
     * @param time the local time
     * @param timeZone the time zone
     * @return the {@link Instant}, or {@code null} if any input is {@code null}
     */
    @ScriptApi
    @Nullable
    public static Instant toInstant(
            @Nullable final LocalDate date,
            @Nullable final LocalTime time,
            @Nullable final ZoneId timeZone) {
        if (date == null || time == null || timeZone == null) {
            return null;
        }

        return time // LocalTime
                .atDate(date) // LocalDateTime
                .atZone(timeZone) // ZonedDateTime
                .toInstant(); // Instant
    }

    /**
     * Converts a {@link Date} to an {@link Instant}.
     *
     * @param date the date to convert
     * @return the {@link Instant}, or {@code null} if {@code date} is {@code null}
     */
    @Deprecated
    @ScriptApi
    @Nullable
    public static Instant toInstant(@Nullable final Date date) {
        if (date == null) {
            return null;
        }

        return epochMillisToInstant(date.getTime());
    }

    /**
     * Converts an {@link Instant} to a {@link ZonedDateTime}.
     *
     * @param instant the instant to convert
     * @param timeZone the time zone to use
     * @return the {@link ZonedDateTime}, or {@code null} if any input is {@code null}
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime toZonedDateTime(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return null;
        }

        return ZonedDateTime.ofInstant(instant, timeZone);
    }

    /**
     * Converts a {@link LocalDateTime} and {@link ZoneId} to a {@link ZonedDateTime}.
     *
     * @param localDateTime the local date time
     * @param timeZone the time zone
     * @return the {@link ZonedDateTime}, or {@code null} if any input is {@code null}
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime toZonedDateTime(
            @Nullable final LocalDateTime localDateTime,
            @Nullable final ZoneId timeZone) {
        if (localDateTime == null || timeZone == null) {
            return null;
        }

        return localDateTime // LocalDateTime
                .atZone(timeZone); // ZonedDateTime
    }

    /**
     * Converts a {@link LocalDate}, {@link LocalTime}, and {@link ZoneId} to a {@link ZonedDateTime}.
     *
     * @param date the local date
     * @param time the local time
     * @param timeZone the time zone to use
     * @return the {@link ZonedDateTime}, or {@code null} if any input is {@code null}
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime toZonedDateTime(
            @Nullable final LocalDate date,
            @Nullable final LocalTime time,
            @Nullable final ZoneId timeZone) {
        if (date == null || time == null || timeZone == null) {
            return null;
        }

        return time // LocalTime
                .atDate(date) // LocalDateTime
                .atZone(timeZone); // ZonedDateTime
    }

    /**
     * Converts an {@link Instant} to a {@link LocalDateTime} with the specified {@link ZoneId}.
     *
     * @param instant the instant to convert
     * @param timeZone the time zone
     * @return the {@link LocalDateTime}, or {@code null} if any input is {@code null}
     */
    @ScriptApi
    @Nullable
    public static LocalDateTime toLocalDateTime(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return null;
        }
        return toZonedDateTime(instant, timeZone).toLocalDateTime();
    }

    /**
     * Gets the {@link LocalDateTime} portion of a {@link ZonedDateTime}.
     *
     * @param dateTime the zoned date time to convert
     * @return the {@link LocalDateTime}, or {@code null} if {@code dateTime} is {@code null}
     */
    @ScriptApi
    @Nullable
    public static LocalDateTime toLocalDateTime(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        return dateTime.toLocalDateTime();
    }

    /**
     * Converts a {@link LocalDate} and {@link LocalTime} pair to a {@link LocalDateTime}.
     *
     * @param localDate the local date to convert
     * @param localTime the local time to convert
     * @return the {@link LocalDateTime}, or {@code null} if {@code localDateTime} is {@code null}
     */
    @ScriptApi
    @Nullable
    public static LocalDateTime toLocalDateTime(
            @Nullable final LocalDate localDate,
            @Nullable final LocalTime localTime) {
        if (localDate == null || localTime == null) {
            return null;
        }
        return LocalDateTime.of(localDate, localTime);
    }

    /**
     * Converts an {@link Instant} to a {@link LocalDate} with the specified {@link ZoneId}.
     *
     * @param instant the instant to convert
     * @param timeZone the time zone
     * @return the {@link LocalDate}, or {@code null} if any input is {@code null}
     */
    @ScriptApi
    @Nullable
    public static LocalDate toLocalDate(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return null;
        }

        return toZonedDateTime(instant, timeZone).toLocalDate();
    }

    /**
     * Gets the {@link LocalDate} portion of a {@link ZonedDateTime}.
     *
     * @param dateTime the zoned date time to convert
     * @return the {@link LocalDate}, or {@code null} if {@code dateTime} is {@code null}
     */
    @ScriptApi
    @Nullable
    public static LocalDate toLocalDate(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }

        return dateTime.toLocalDate();
    }

    /**
     * Gets the {@link LocalDate} portion of a {@link LocalDateTime}.
     *
     * @param localDateTime the local date time to convert
     * @return the {@link LocalDate}, or {@code null} if {@code localDateTime} is {@code null}
     */
    @ScriptApi
    @Nullable
    public static LocalDate toLocalDate(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return null;
        }

        return localDateTime.toLocalDate();
    }

    /**
     * Converts an {@link Instant} to a {@link LocalTime} with the specified {@link ZoneId}.
     *
     * @param instant the instant to convert
     * @param timeZone the time zone
     * @return the {@link LocalTime}, or {@code null} if any input is {@code null}
     */
    @ScriptApi
    @Nullable
    public static LocalTime toLocalTime(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return null;
        }
        return toZonedDateTime(instant, timeZone).toLocalTime();
    }

    /**
     * Gets the {@link LocalTime} portion of a {@link ZonedDateTime}.
     *
     * @param dateTime the zoned date time to convert
     * @return the {@link LocalTime}, or {@code null} if {@code dateTime} is {@code null}
     */
    @ScriptApi
    @Nullable
    public static LocalTime toLocalTime(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        return dateTime.toLocalTime();
    }

    /**
     * Gets the {@link LocalTime} portion of a {@link LocalDateTime}.
     *
     * @param localDateTime the local date time to convert
     * @return the {@link LocalTime}, or {@code null} if {@code localDateTime} is {@code null}
     */
    @ScriptApi
    @Nullable
    public static LocalTime toLocalTime(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return null;
        }
        return localDateTime.toLocalTime();
    }

    /**
     * Converts the number of milliseconds from midnight to a {@link LocalTime}
     *
     * @param millis milliseconds from midnight
     * @return the {@link LocalTime}, or {@code null} if any input is {@link QueryConstants#NULL_INT NULL_INT}
     */
    @ScriptApi
    public static @Nullable LocalTime millisOfDayToLocalTime(final int millis) {
        if (millis == NULL_INT) {
            return null;
        }
        return LocalTime.ofNanoOfDay(millis * MILLI);
    }

    /**
     * Converts the number of microseconds from midnight to a {@link LocalTime}
     *
     * @param micros microseconds from midnight
     * @return the {@link LocalTime}, or {@code null} if any input is {@link QueryConstants#NULL_LONG NULL_LONG}
     */
    @ScriptApi
    public static @Nullable LocalTime microsOfDayToLocalTime(final long micros) {
        if (micros == NULL_LONG) {
            return null;
        }
        return LocalTime.ofNanoOfDay(micros * MICRO);
    }

    /**
     * Converts the number of nanoseconds from midnight to a {@link LocalTime}
     *
     * @param nanos nanoseconds from midnight
     * @return the {@link LocalTime}, or {@code null} if any input is {@link QueryConstants#NULL_LONG NULL_LONG}
     */
    @ScriptApi
    public static @Nullable LocalTime nanosOfDayToLocalTime(final long nanos) {
        if (nanos == NULL_LONG) {
            return null;
        }
        return LocalTime.ofNanoOfDay(nanos);
    }

    /**
     * Converts an {@link Instant} to a {@link Date}. {@code instant} will be truncated to millisecond resolution.
     *
     * @param instant the instant to convert
     * @return the {@link Date}, or {@code null} if {@code instant} is {@code null}
     * @deprecated
     */
    @Deprecated
    @ScriptApi
    @Nullable
    public static Date toDate(@Nullable final Instant instant) {
        if (instant == null) {
            return null;
        }
        return new Date(epochMillis(instant));
    }

    /**
     * Converts a {@link ZonedDateTime} to a {@link Date}. {@code dateTime} will be truncated to millisecond resolution.
     *
     * @param dateTime the zoned date time to convert
     * @return the {@link Date}, or {@code null} if {@code dateTime} is {@code null}
     * @deprecated
     */
    @Deprecated
    @ScriptApi
    @Nullable
    public static Date toDate(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        return new Date(epochMillis(dateTime));
    }

    // endregion

    // region Conversions: Epoch

    /**
     * Returns nanoseconds from the Epoch for an {@link Instant} value.
     *
     * @param instant instant to compute the Epoch offset for
     * @return nanoseconds since Epoch, or a NULL_LONG value if the instant is null
     */
    @ScriptApi
    public static long epochNanos(@Nullable final Instant instant) {
        if (instant == null) {
            return NULL_LONG;
        }

        return safeComputeNanos(instant.getEpochSecond(), instant.getNano());
    }

    /**
     * Returns nanoseconds from the Epoch for a {@link ZonedDateTime} value.
     *
     * @param dateTime the zoned date time to compute the Epoch offset for
     * @return nanoseconds since Epoch, or a NULL_LONG value if the zoned date time is null
     */
    @ScriptApi
    public static long epochNanos(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return safeComputeNanos(dateTime.toEpochSecond(), dateTime.getNano());
    }

    /**
     * Returns microseconds from the Epoch for an {@link Instant} value.
     *
     * @param instant instant to compute the Epoch offset for
     * @return microseconds since Epoch, or a {@link QueryConstants#NULL_LONG NULL_LONG} value if the instant is
     *         {@code null}
     */
    @ScriptApi
    public static long epochMicros(@Nullable final Instant instant) {
        if (instant == null) {
            return NULL_LONG;
        }

        return nanosToMicros(epochNanos(instant));
    }

    /**
     * Returns microseconds from the Epoch for a {@link ZonedDateTime} value.
     *
     * @param dateTime zoned date time to compute the Epoch offset for
     * @return microseconds since Epoch, or a {@link QueryConstants#NULL_LONG NULL_LONG} value if the zoned date time is
     *         {@code null}
     */
    @ScriptApi
    public static long epochMicros(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return nanosToMicros(epochNanos(dateTime));
    }

    /**
     * Returns milliseconds from the Epoch for an {@link Instant} value.
     *
     * @param instant instant to compute the Epoch offset for
     * @return milliseconds since Epoch, or a {@link QueryConstants#NULL_LONG NULL_LONG} value if the instant is
     *         {@code null}
     */
    @ScriptApi
    public static long epochMillis(@Nullable final Instant instant) {
        if (instant == null) {
            return NULL_LONG;
        }

        return instant.toEpochMilli();
    }

    /**
     * Returns milliseconds from the Epoch for a {@link ZonedDateTime} value.
     *
     * @param dateTime zoned date time to compute the Epoch offset for
     * @return milliseconds since Epoch, or a {@link QueryConstants#NULL_LONG NULL_LONG} value if the zoned date time is
     *         {@code null}
     */
    @ScriptApi
    public static long epochMillis(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return nanosToMillis(epochNanos(dateTime));
    }

    /**
     * Returns seconds since from the Epoch for an {@link Instant} value.
     *
     * @param instant instant to compute the Epoch offset for
     * @return seconds since Epoch, or a {@link QueryConstants#NULL_LONG NULL_LONG} value if the instant is {@code null}
     */
    @ScriptApi
    public static long epochSeconds(@Nullable final Instant instant) {
        if (instant == null) {
            return NULL_LONG;
        }

        return instant.getEpochSecond();
    }

    /**
     * Returns seconds since from the Epoch for a {@link ZonedDateTime} value.
     *
     * @param dateTime zoned date time to compute the Epoch offset for
     * @return seconds since Epoch, or a {@link QueryConstants#NULL_LONG NULL_LONG} value if the zoned date time is
     *         {@code null}
     */
    @ScriptApi
    public static long epochSeconds(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return dateTime.toEpochSecond();
    }

    /**
     * Returns number of days from the Epoch for a {@link LocalDate} value.
     *
     * @param date date to compute the Epoch offset for
     * @return days since Epoch, or a {@link QueryConstants#NULL_LONG NULL_LONG} value if the instant is {@code null}
     */
    @ScriptApi
    public static long epochDays(@Nullable final LocalDate date) {
        if (date == null) {
            return NULL_LONG;
        }
        return date.toEpochDay();
    }

    /**
     * Returns number of days (as an {@code int}) from the Epoch for a {@link LocalDate} value.
     *
     * @param date date to compute the Epoch offset for
     * @return days since Epoch, or a {@link QueryConstants#NULL_INT NULL_INT} value if the instant is {@code null}
     */
    @ScriptApi
    public static int epochDaysAsInt(@Nullable final LocalDate date) {
        if (date == null) {
            return NULL_INT;
        }
        return Math.toIntExact(date.toEpochDay());
    }

    /**
     * Converts nanoseconds from the Epoch to an {@link Instant}.
     *
     * @param nanos nanoseconds since Epoch
     * @return {@code null} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input nanoseconds from the
     *         Epoch converted to an {@link Instant}
     */
    @ScriptApi
    @Nullable
    public static Instant epochNanosToInstant(final long nanos) {
        return nanos == NULL_LONG ? null : Instant.ofEpochSecond(nanos / 1_000_000_000L, nanos % 1_000_000_000L);
    }

    /**
     * Converts microseconds from the Epoch to an {@link Instant}.
     *
     * @param micros microseconds since Epoch
     * @return {@code null} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input microseconds from the
     *         Epoch converted to an {@link Instant}
     */
    @ScriptApi
    @Nullable
    public static Instant epochMicrosToInstant(final long micros) {
        return micros == NULL_LONG ? null : Instant.ofEpochSecond(micros / 1_000_000L, (micros % 1_000_000L) * 1_000L);
    }

    /**
     * Converts milliseconds from the Epoch to an {@link Instant}.
     *
     * @param millis milliseconds since Epoch
     * @return {@code null} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input milliseconds from the
     *         Epoch converted to an {@link Instant}
     */
    @ScriptApi
    @Nullable
    public static Instant epochMillisToInstant(final long millis) {
        return millis == NULL_LONG ? null : Instant.ofEpochSecond(millis / 1_000L, (millis % 1_000L) * 1_000_000L);
    }

    /**
     * Converts seconds from the Epoch to an {@link Instant}.
     *
     * @param seconds seconds since Epoch
     * @return {@code null} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input seconds from the Epoch
     *         converted to an {@link Instant}
     */
    @ScriptApi
    @Nullable
    public static Instant epochSecondsToInstant(final long seconds) {
        return seconds == NULL_LONG ? null : Instant.ofEpochSecond(seconds, 0);
    }

    /**
     * Converts nanoseconds from the Epoch to a {@link ZonedDateTime}.
     *
     * @param nanos nanoseconds since Epoch
     * @param timeZone time zone
     * @return {@code null} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input nanoseconds from the
     *         Epoch converted to a {@link ZonedDateTime}
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime epochNanosToZonedDateTime(final long nanos, final ZoneId timeZone) {
        if (timeZone == null) {
            return null;
        }

        // noinspection ConstantConditions
        return nanos == NULL_LONG ? null : ZonedDateTime.ofInstant(epochNanosToInstant(nanos), timeZone);
    }

    /**
     * Converts microseconds from the Epoch to a {@link ZonedDateTime}.
     *
     * @param micros microseconds since Epoch
     * @param timeZone time zone
     * @return {@code null} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input microseconds from the
     *         Epoch converted to a {@link ZonedDateTime}
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime epochMicrosToZonedDateTime(final long micros, @Nullable ZoneId timeZone) {
        if (timeZone == null) {
            return null;
        }

        // noinspection ConstantConditions
        return micros == NULL_LONG ? null : ZonedDateTime.ofInstant(epochMicrosToInstant(micros), timeZone);
    }

    /**
     * Converts milliseconds from the Epoch to a {@link ZonedDateTime}.
     *
     * @param millis milliseconds since Epoch
     * @param timeZone time zone
     * @return {@code null} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input milliseconds from the
     *         Epoch converted to a {@link ZonedDateTime}
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime epochMillisToZonedDateTime(final long millis, @Nullable final ZoneId timeZone) {
        if (timeZone == null) {
            return null;
        }

        // noinspection ConstantConditions
        return millis == NULL_LONG ? null : ZonedDateTime.ofInstant(epochMillisToInstant(millis), timeZone);
    }

    /**
     * Converts seconds from the Epoch to a {@link ZonedDateTime}.
     *
     * @param seconds seconds since Epoch
     * @param timeZone time zone
     * @return {@code null} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input seconds from the Epoch
     *         converted to a {@link ZonedDateTime}
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime epochSecondsToZonedDateTime(final long seconds, @Nullable final ZoneId timeZone) {
        if (timeZone == null) {
            return null;
        }
        // noinspection ConstantConditions
        return seconds == NULL_LONG ? null : ZonedDateTime.ofInstant(epochSecondsToInstant(seconds), timeZone);
    }

    /**
     * Converts an offset from the Epoch to a nanoseconds from the Epoch. The offset can be in milliseconds,
     * microseconds, or nanoseconds. Expected date ranges are used to infer the units for the offset.
     *
     * @param epochOffset time offset from the Epoch
     * @return {@code null} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input offset from the Epoch
     *         converted to nanoseconds from the Epoch
     */
    @ScriptApi
    public static long epochAutoToEpochNanos(final long epochOffset) {
        if (epochOffset == NULL_LONG) {
            return epochOffset;
        }
        final long absEpoch = Math.abs(epochOffset);
        if (absEpoch > 1000 * TimeConstants.MICROTIME_THRESHOLD) { // Nanoseconds
            return epochOffset;
        }
        if (absEpoch > TimeConstants.MICROTIME_THRESHOLD) { // Microseconds
            return 1000 * epochOffset;
        }
        if (absEpoch > TimeConstants.MICROTIME_THRESHOLD / 1000) { // Milliseconds
            return 1000 * 1000 * epochOffset;
        }
        // Seconds
        return 1000 * 1000 * 1000 * epochOffset;
    }

    /**
     * Converts an offset from the Epoch to an {@link Instant}. The offset can be in milliseconds, microseconds, or
     * nanoseconds. Expected date ranges are used to infer the units for the offset.
     *
     * @param epochOffset time offset from the Epoch
     * @return {@code null} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input offset from the Epoch
     *         converted to an {@link Instant}
     */
    @ScriptApi
    @Nullable
    public static Instant epochAutoToInstant(final long epochOffset) {
        if (epochOffset == NULL_LONG) {
            return null;
        }
        return epochNanosToInstant(epochAutoToEpochNanos(epochOffset));
    }

    /**
     * Converts an offset from the Epoch to a {@link ZonedDateTime}. The offset can be in milliseconds, microseconds, or
     * nanoseconds. Expected date ranges are used to infer the units for the offset.
     *
     * @param epochOffset time offset from the Epoch
     * @param timeZone time zone
     * @return {@code null} if any input is {@code null} or {@link QueryConstants#NULL_LONG}; otherwise the input offset
     *         from the Epoch converted to a {@link ZonedDateTime}
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime epochAutoToZonedDateTime(final long epochOffset, @Nullable ZoneId timeZone) {
        if (epochOffset == NULL_LONG || timeZone == null) {
            return null;
        }
        return epochNanosToZonedDateTime(epochAutoToEpochNanos(epochOffset), timeZone);
    }

    /**
     * Converts days from the Epoch to a {@link LocalDate}.
     *
     * @param days days since Epoch
     * @return {@code null} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input days from the Epoch
     *         converted to a {@link LocalDate}
     */
    @ScriptApi
    public static @Nullable LocalDate epochDaysToLocalDate(final long days) {
        return days == NULL_LONG ? null : LocalDate.ofEpochDay(days);
    }

    /**
     * Converts days from the Epoch (stored as {@code int}) to a {@link LocalDate}.
     *
     * @param days days since Epoch
     * @return {@code null} if the input is {@link QueryConstants#NULL_INT}; otherwise the input days from the Epoch
     *         converted to a {@link LocalDate}
     */
    @ScriptApi
    public static @Nullable LocalDate epochDaysAsIntToLocalDate(final int days) {
        return days == NULL_INT ? null : LocalDate.ofEpochDay(days);
    }

    // endregion

    // region Conversions: Excel

    private static double epochMillisToExcelTime(final long millis, final ZoneId timeZone) {
        return (double) (millis + java.util.TimeZone.getTimeZone(timeZone).getOffset(millis)) / 86400000 + 25569;
    }

    private static long excelTimeToEpochMillis(final double excel, final ZoneId timeZone) {
        final java.util.TimeZone tz = java.util.TimeZone.getTimeZone(timeZone);

        final long mpo = (long) ((excel - 25569) * 86400000);
        final long o = tz.getOffset(mpo);
        final long m = mpo - o;
        final long o2 = tz.getOffset(m);
        return mpo - o2;
    }

    /**
     * Converts an {@link Instant} to an Excel time represented as a double.
     *
     * @param instant instant to convert
     * @param timeZone time zone to use when interpreting the instant
     * @return 0.0 if either input is {@code null}; otherwise, the input instant converted to an Excel time represented
     *         as a double
     */
    @ScriptApi
    public static double toExcelTime(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return 0.0;
        }

        return epochMillisToExcelTime(epochMillis(instant), timeZone);
    }

    /**
     * Converts a {@link ZonedDateTime} to an Excel time represented as a double.
     *
     * @param dateTime zoned date time to convert
     * @return 0.0 if either input is {@code null}; otherwise, the input zoned date time converted to an Excel time
     *         represented as a double
     */
    @ScriptApi
    public static double toExcelTime(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return 0.0;
        }

        return toExcelTime(toInstant(dateTime), dateTime.getZone());
    }

    /**
     * Converts an Excel time represented as a double to an {@link Instant}.
     *
     * @param excel excel time represented as a double
     * @param timeZone time zone to use when interpreting the Excel time
     * @return {@code null} if timeZone is {@code null}; otherwise, the input Excel time converted to an {@link Instant}
     */
    @ScriptApi
    @Nullable
    public static Instant excelToInstant(final double excel, @Nullable final ZoneId timeZone) {
        if (timeZone == null) {
            return null;
        }

        return epochMillisToInstant(excelTimeToEpochMillis(excel, timeZone));
    }

    /**
     * Converts an Excel time represented as a double to a {@link ZonedDateTime}.
     *
     * @param excel excel time represented as a double
     * @param timeZone time zone to use when interpreting the Excel time
     * @return {@code null} if timeZone is {@code null}; otherwise, the input Excel time converted to a
     *         {@link ZonedDateTime}
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime excelToZonedDateTime(final double excel, @Nullable final ZoneId timeZone) {
        if (timeZone == null) {
            return null;
        }

        return epochMillisToZonedDateTime(excelTimeToEpochMillis(excel, timeZone), timeZone);
    }

    // endregion

    // region Arithmetic

    /**
     * Adds days to a {@link LocalDate}.
     *
     * @param localDateTime starting local date time
     * @param days number of days to add
     * @return {@code null} if either input is {@code null} or {@link QueryConstants#NULL_LONG}; otherwise the starting
     *         local date time plus the specified number of days
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static LocalDateTime plusDays(@Nullable final LocalDateTime localDateTime, final long days) {
        if (localDateTime == null || days == NULL_LONG) {
            return null;
        }

        try {
            return localDateTime.plusDays(days);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Adds days to a {@link LocalDate}.
     *
     * @param localDate starting local date
     * @param days number of days to add
     * @return {@code null} if either input is {@code null} or {@link QueryConstants#NULL_LONG}; otherwise the starting
     *         local date plus the specified number of days
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static LocalDate plusDays(@Nullable final LocalDate localDate, final long days) {
        if (localDate == null || days == NULL_LONG) {
            return null;
        }

        try {
            return localDate.plusDays(days);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Adds a time period to a {@link LocalDateTime}.
     *
     * @param localDateTime starting local date time
     * @param period time period
     * @return {@code null} if either input is {@code null}; otherwise the starting local date time plus the specified
     *         time period
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static LocalDateTime plus(@Nullable final LocalDateTime localDateTime, final Period period) {
        if (localDateTime == null || period == null) {
            return null;
        }

        try {
            return localDateTime.plus(period);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Adds a time period to a {@link LocalDate}.
     *
     * @param localDate starting local date
     * @param period time period
     * @return {@code null} if either input is {@code null}; otherwise the starting local date plus the specified time
     *         period
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static LocalDate plus(@Nullable final LocalDate localDate, final Period period) {
        if (localDate == null || period == null) {
            return null;
        }

        try {
            return localDate.plus(period);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Adds nanoseconds to an {@link Instant}.
     *
     * @param instant starting instant value
     * @param nanos number of nanoseconds to add
     * @return {@code null} if either input is {@code null} or {@link QueryConstants#NULL_LONG}; otherwise the starting
     *         instant plus the specified number of nanoseconds
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static Instant plus(@Nullable final Instant instant, final long nanos) {
        if (instant == null || nanos == NULL_LONG) {
            return null;
        }

        try {
            return instant.plusNanos(nanos);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Adds nanoseconds to a {@link ZonedDateTime}.
     *
     * @param dateTime starting zoned date time value
     * @param nanos number of nanoseconds to add
     * @return {@code null} if either input is {@code null} or {@link QueryConstants#NULL_LONG}; otherwise the starting
     *         zoned date time plus the specified number of nanoseconds
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime plus(@Nullable final ZonedDateTime dateTime, final long nanos) {
        if (dateTime == null || nanos == NULL_LONG) {
            return null;
        }

        try {
            return dateTime.plusNanos(nanos);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Adds a time period to an {@link Instant}.
     *
     * @param instant starting instant value
     * @param duration time period
     * @return {@code null} if either input is {@code null} or {@link QueryConstants#NULL_LONG}; otherwise the starting
     *         instant plus the specified time period
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static Instant plus(@Nullable final Instant instant, @Nullable final Duration duration) {
        if (instant == null || duration == null) {
            return null;
        }

        try {
            return instant.plus(duration);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Adds a time period to an {@link Instant}.
     *
     * @param instant starting instant value
     * @param period time period
     * @return {@code null} if either input is {@code null} or {@link QueryConstants#NULL_LONG}; otherwise the starting
     *         instant plus the specified time period
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static Instant plus(@Nullable final Instant instant, @Nullable final Period period) {
        if (instant == null || period == null) {
            return null;
        }

        try {
            return instant.plus(period);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Adds a time period to a {@link ZonedDateTime}.
     *
     * @param dateTime starting zoned date time value
     * @param duration time period
     * @return {@code null} if either input is {@code null} or {@link QueryConstants#NULL_LONG}; otherwise the starting
     *         zoned date time plus the specified time period
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime plus(@Nullable final ZonedDateTime dateTime, @Nullable final Duration duration) {
        if (dateTime == null || duration == null) {
            return null;
        }

        try {
            return dateTime.plus(duration);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Adds a time period to a {@link ZonedDateTime}.
     *
     * @param dateTime starting zoned date time value
     * @param period time period
     * @return {@code null} if either input is {@code null} or {@link QueryConstants#NULL_LONG}; otherwise the starting
     *         zoned date time plus the specified time period
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime plus(@Nullable final ZonedDateTime dateTime, @Nullable final Period period) {
        if (dateTime == null || period == null) {
            return null;
        }

        try {
            return dateTime.plus(period);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Adds two durations.
     *
     * @param duration1 first duration
     * @param duration2 second duration
     * @return {@code null} if either input is {@code null}; otherwise the sum of the two durations
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    public static Duration plus(@Nullable final Duration duration1, @Nullable final Duration duration2) {
        if (duration1 == null || duration2 == null) {
            return null;
        }

        try {
            return duration1.plus(duration2);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Adds two periods.
     *
     * @param period1 first period
     * @param period2 second period
     * @return {@code null} if either input is {@code null}; otherwise the sum of the two periods
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    public static Period plus(@Nullable final Period period1, @Nullable final Period period2) {
        if (period1 == null || period2 == null) {
            return null;
        }

        try {
            return period1.plus(period2);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtracts days from a {@link LocalDate}.
     *
     * @param localDateTime starting date
     * @param days number of days to subtract
     * @return {@code null} if either input is {@code null} or {@link QueryConstants#NULL_LONG}; otherwise the starting
     *         local date time minus the specified number of days
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static LocalDateTime minusDays(@Nullable final LocalDateTime localDateTime, final long days) {
        if (localDateTime == null || days == NULL_LONG) {
            return null;
        }

        try {
            return localDateTime.minusDays(days);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtracts days from a {@link LocalDate}.
     *
     * @param localDate starting local date
     * @param days number of days to subtract
     * @return {@code null} if either input is {@code null} or {@link QueryConstants#NULL_LONG}; otherwise the starting
     *         local date minus the specified number of days
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static LocalDate minusDays(@Nullable final LocalDate localDate, final long days) {
        if (localDate == null || days == NULL_LONG) {
            return null;
        }

        try {
            return localDate.minusDays(days);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtracts a time period from a {@link LocalDateTime}.
     *
     * @param localDateTime starting local date time
     * @param period time period
     * @return {@code null} if either input is {@code null}; otherwise the starting local date time minus the specified
     *         time period
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static LocalDateTime minus(@Nullable final LocalDateTime localDateTime, final Period period) {
        if (localDateTime == null || period == null) {
            return null;
        }

        try {
            return localDateTime.minus(period);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtracts a time period from a {@link LocalDate}.
     *
     * @param localDate starting local date
     * @param period time period
     * @return {@code null} if either input is {@code null}; otherwise the starting local date minus the specified time
     *         period
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static LocalDate minus(@Nullable final LocalDate localDate, final Period period) {
        if (localDate == null || period == null) {
            return null;
        }

        try {
            return localDate.minus(period);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtracts nanoseconds from an {@link Instant}.
     *
     * @param instant starting instant value
     * @param nanos number of nanoseconds to subtract
     * @return {@code null} if either input is {@code null} or {@link QueryConstants#NULL_LONG}; otherwise the starting
     *         instant minus the specified number of nanoseconds
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static Instant minus(@Nullable final Instant instant, final long nanos) {
        if (instant == null || nanos == NULL_LONG) {
            return null;
        }

        try {
            return instant.minusNanos(nanos);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtracts nanoseconds from a {@link ZonedDateTime}.
     *
     * @param dateTime starting zoned date time value
     * @param nanos number of nanoseconds to subtract
     * @return {@code null} if either input is {@code null} or {@link QueryConstants#NULL_LONG}; otherwise the starting
     *         zoned date time minus the specified number of nanoseconds
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime minus(@Nullable final ZonedDateTime dateTime, final long nanos) {
        if (dateTime == null || nanos == NULL_LONG) {
            return null;
        }

        try {
            return dateTime.minusNanos(nanos);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtracts a time period to an {@link Instant}.
     *
     * @param instant starting instant value
     * @param duration time period
     * @return {@code null} if either input is {@code null} or {@link QueryConstants#NULL_LONG}; otherwise the starting
     *         instant minus the specified time period
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static Instant minus(@Nullable final Instant instant, @Nullable final Duration duration) {
        if (instant == null || duration == null) {
            return null;
        }

        try {
            return instant.minus(duration);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtracts a time period to an {@link Instant}.
     *
     * @param instant starting instant value
     * @param period time period
     * @return {@code null} if either input is {@code null} or {@link QueryConstants#NULL_LONG}; otherwise the starting
     *         instant minus the specified time period
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static Instant minus(@Nullable final Instant instant, @Nullable final Period period) {
        if (instant == null || period == null) {
            return null;
        }

        try {
            return instant.minus(period);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtracts a time period to a {@link ZonedDateTime}.
     *
     * @param dateTime starting zoned date time value
     * @param duration time period
     * @return {@code null} if either input is {@code null} or {@link QueryConstants#NULL_LONG}; otherwise the starting
     *         zoned date time minus the specified time period
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime minus(@Nullable final ZonedDateTime dateTime, @Nullable final Duration duration) {
        if (dateTime == null || duration == null) {
            return null;
        }

        try {
            return dateTime.minus(duration);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtracts a time period to a {@link ZonedDateTime}.
     *
     * @param dateTime starting zoned date time value
     * @param period time period
     * @return {@code null} if either input is {@code null} or {@link QueryConstants#NULL_LONG}; otherwise the starting
     *         zoned date time minus the specified time period
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime minus(@Nullable final ZonedDateTime dateTime, @Nullable final Period period) {
        if (dateTime == null || period == null) {
            return null;
        }

        try {
            return dateTime.minus(period);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtract one instant from another and return the difference in nanoseconds.
     *
     * @param instant1 first instant
     * @param instant2 second instant
     * @return {@link QueryConstants#NULL_LONG} if either input is {@code null}; otherwise the difference in instant1
     *         and instant2 in nanoseconds
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static long minus(@Nullable final Instant instant1, @Nullable final Instant instant2) {
        if (instant1 == null || instant2 == null) {
            return NULL_LONG;
        }

        return checkUnderflowMinus(epochNanos(instant1), epochNanos(instant2), true);
    }

    /**
     * Subtract one zoned date time from another and return the difference in nanoseconds.
     *
     * @param dateTime1 first zoned date time
     * @param dateTime2 second zoned date time
     * @return {@link QueryConstants#NULL_LONG} if either input is {@code null}; otherwise the difference in dateTime1
     *         and dateTime2 in nanoseconds
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static long minus(@Nullable final ZonedDateTime dateTime1, @Nullable final ZonedDateTime dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return NULL_LONG;
        }

        return checkUnderflowMinus(epochNanos(dateTime1), epochNanos(dateTime2), true);
    }

    /**
     * Subtracts two durations.
     *
     * @param duration1 first duration
     * @param duration2 second duration
     * @return {@code null} if either input is {@code null}; otherwise the difference of the two durations
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    public static Duration minus(@Nullable final Duration duration1, @Nullable final Duration duration2) {
        if (duration1 == null || duration2 == null) {
            return null;
        }

        try {
            return duration1.minus(duration2);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtracts two periods.
     *
     * @param period1 first period
     * @param period2 second period
     * @return {@code null} if either input is {@code null}; otherwise the difference of the two periods
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    public static Period minus(@Nullable final Period period1, @Nullable final Period period2) {
        if (period1 == null || period2 == null) {
            return null;
        }

        try {
            return period1.minus(period2);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Multiply a duration by a scalar.
     *
     * @param duration the duration to multiply
     * @param scalar the scalar to multiply by
     * @return {@code null} if either input is {@code null}; otherwise the duration multiplied by the scalar
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    public static Duration multiply(final Duration duration, final long scalar) {
        if (duration == null || scalar == NULL_LONG) {
            return null;
        }

        try {
            return duration.multipliedBy(scalar);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Multiply a duration by a scalar.
     *
     * @param duration the duration to multiply
     * @param scalar the scalar to multiply by
     * @return {@code null} if either input is {@code null}; otherwise the duration multiplied by the scalar
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    public static Duration multiply(final long scalar, final Duration duration) {
        return multiply(duration, scalar);
    }

    /**
     * Multiply a period by a scalar.
     *
     * @param period the period to multiply
     * @param scalar the scalar to multiply by
     * @return {@code null} if either input is {@code null}; otherwise the period multiplied by the scalar
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    public static Period multiply(final Period period, final int scalar) {
        if (period == null || scalar == NULL_INT) {
            return null;
        }

        try {
            return period.multipliedBy(scalar);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Multiply a period by a scalar.
     *
     * @param period the period to multiply
     * @param scalar the scalar to multiply by
     * @return {@code null} if either input is {@code null}; otherwise the period multiplied by the scalar
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    public static Period multiply(final int scalar, final Period period) {
        return multiply(period, scalar);
    }

    /**
     * Divide a duration by a scalar.
     *
     * @param duration the duration to divide
     * @param scalar the scalar to divide by
     * @return {@code null} if either input is {@code null}; otherwise the duration divide by the scalar
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    public static Duration divide(final Duration duration, final long scalar) {
        if (duration == null || scalar == NULL_LONG) {
            return null;
        }

        try {
            return duration.dividedBy(scalar);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Returns the difference in nanoseconds between two instant values.
     *
     * @param start start time
     * @param end end time
     * @return {@link QueryConstants#NULL_LONG} if either input is {@code null}; otherwise the difference in start and
     *         end in nanoseconds
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static long diffNanos(@Nullable final Instant start, @Nullable final Instant end) {
        return minus(end, start);
    }

    /**
     * Returns the difference in nanoseconds between two zoned date time values.
     *
     * @param start start time
     * @param end end time
     * @return {@link QueryConstants#NULL_LONG} if either input is {@code null}; otherwise the difference in start and
     *         end in nanoseconds
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static long diffNanos(@Nullable final ZonedDateTime start, @Nullable final ZonedDateTime end) {
        return minus(end, start);
    }

    /**
     * Returns the difference in microseconds between two instant values.
     *
     * @param start start time
     * @param end end time
     * @return {@link QueryConstants#NULL_LONG} if either input is {@code null}; otherwise the difference in start and
     *         end in microseconds
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static long diffMicros(@Nullable final Instant start, @Nullable final Instant end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }

        return nanosToMicros(diffNanos(start, end));
    }

    /**
     * Returns the difference in microseconds between two zoned date time values.
     *
     * @param start start time
     * @param end end time
     * @return {@link QueryConstants#NULL_LONG} if either input is {@code null}; otherwise the difference in start and
     *         end in microseconds
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static long diffMicros(@Nullable final ZonedDateTime start, @Nullable final ZonedDateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }

        return nanosToMicros(diffNanos(start, end));
    }

    /**
     * Returns the difference in milliseconds between two instant values.
     *
     * @param start start time
     * @param end end time
     * @return {@link QueryConstants#NULL_LONG} if either input is {@code null}; otherwise the difference in start and
     *         end in milliseconds
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static long diffMillis(@Nullable final Instant start, @Nullable final Instant end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }

        return nanosToMillis(diffNanos(start, end));
    }

    /**
     * Returns the difference in milliseconds between two zoned date time values.
     *
     * @param start start time
     * @param end end time
     * @return {@link QueryConstants#NULL_LONG} if either input is {@code null}; otherwise the difference in start and
     *         end in milliseconds
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static long diffMillis(@Nullable final ZonedDateTime start, @Nullable final ZonedDateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }

        return nanosToMillis(diffNanos(start, end));
    }

    /**
     * Returns the difference in seconds between two instant values.
     *
     * @param start start time
     * @param end end time
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is {@code null}; otherwise the difference in start and
     *         end in seconds
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static double diffSeconds(@Nullable final Instant start, @Nullable final Instant end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / SECOND;
    }

    /**
     * Returns the difference in seconds between two zoned date time values.
     *
     * @param start start time
     * @param end end time
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is {@code null}; otherwise the difference in start and
     *         end in seconds
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static double diffSeconds(@Nullable final ZonedDateTime start, @Nullable final ZonedDateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / SECOND;
    }

    /**
     * Returns the difference in minutes between two instant values.
     *
     * @param start start time
     * @param end end time
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is {@code null}; otherwise the difference in start and
     *         end in minutes
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static double diffMinutes(@Nullable final Instant start, @Nullable final Instant end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / MINUTE;
    }

    /**
     * Returns the difference in minutes between two zoned date time values.
     *
     * @param start start time
     * @param end end time
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is {@code null}; otherwise the difference in start and
     *         end in minutes
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static double diffMinutes(@Nullable final ZonedDateTime start, @Nullable final ZonedDateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / MINUTE;
    }

    /**
     * Returns the difference in days between two instant values.
     *
     * @param start start time
     * @param end end time
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is {@code null}; otherwise the difference in start and
     *         end in days
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static double diffDays(@Nullable final Instant start, @Nullable final Instant end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / DAY;
    }

    /**
     * Returns the difference in days between two zoned date time values.
     *
     * @param start start time
     * @param end end time
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is {@code null}; otherwise the difference in start and
     *         end in days
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static double diffDays(@Nullable final ZonedDateTime start, @Nullable final ZonedDateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / DAY;
    }

    /**
     * Returns the difference in years between two instant values.
     * <p>
     * Years are defined in terms of 365 day years.
     *
     * @param start start time
     * @param end end time
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is {@code null}; otherwise the difference in start and
     *         end in years
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static double diffYears365(@Nullable final Instant start, @Nullable final Instant end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) * YEARS_PER_NANO_365;
    }

    /**
     * Returns the difference in years between two zoned date time values.
     * <p>
     * Years are defined in terms of 365 day years.
     *
     * @param start start time
     * @param end end time
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is {@code null}; otherwise the difference in start and
     *         end in years
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static double diffYears365(@Nullable final ZonedDateTime start, @Nullable final ZonedDateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) * YEARS_PER_NANO_365;
    }

    /**
     * Returns the difference in years between two instant values.
     * <p>
     * Years are defined in terms of 365.2425 day years.
     *
     * @param start start time
     * @param end end time
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is {@code null}; otherwise the difference in start and
     *         end in years
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static double diffYearsAvg(@Nullable final Instant start, @Nullable final Instant end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) * YEARS_PER_NANO_AVG;
    }

    /**
     * Returns the difference in years between two zoned date time values.
     * <p>
     * Years are defined in terms of 365.2425 day years.
     *
     * @param start start time
     * @param end end time
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is {@code null}; otherwise the difference in start and
     *         end in years
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows
     */
    @ScriptApi
    public static double diffYearsAvg(@Nullable final ZonedDateTime start, @Nullable final ZonedDateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) * YEARS_PER_NANO_AVG;
    }

    // endregion

    // region Comparisons

    /**
     * Evaluates whether one instant value is before a second instant value.
     *
     * @param instant1 first instant
     * @param instant2 second instant
     * @return {@code true} if instant1 is before instant2; otherwise, {@code false} if either value is {@code null} or
     *         if instant2 is equal to or before instant1
     */
    @ScriptApi
    public static boolean isBefore(@Nullable final Instant instant1, @Nullable final Instant instant2) {
        if (instant1 == null || instant2 == null) {
            return false;
        }

        return instant1.isBefore(instant2);
    }

    /**
     * Evaluates whether one zoned date time value is before a second zoned date time value.
     *
     * @param dateTime1 first zoned date time
     * @param dateTime2 second zoned date time
     * @return {@code true} if dateTime1 is before dateTime2; otherwise, {@code false} if either value is {@code null}
     *         or if dateTime2 is equal to or before dateTime1
     */
    @ScriptApi
    public static boolean isBefore(@Nullable final ZonedDateTime dateTime1, @Nullable final ZonedDateTime dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return false;
        }

        return dateTime1.isBefore(dateTime2);
    }

    /**
     * Evaluates whether one instant value is before or equal to a second instant value.
     *
     * @param instant1 first instant
     * @param instant2 second instant
     * @return {@code true} if instant1 is before or equal to instant2; otherwise, {@code false} if either value is
     *         {@code null} or if instant2 is before instant1
     */
    @ScriptApi
    public static boolean isBeforeOrEqual(@Nullable final Instant instant1, @Nullable final Instant instant2) {
        if (instant1 == null || instant2 == null) {
            return false;
        }

        return instant1.isBefore(instant2) || instant1.equals(instant2);
    }

    /**
     * Evaluates whether one zoned date time value is before or equal to a second zoned date time value.
     *
     * @param dateTime1 first zoned date time
     * @param dateTime2 second zoned date time
     * @return {@code true} if dateTime1 is before or equal to dateTime2; otherwise, {@code false} if either value is
     *         {@code null} or if dateTime2 is before dateTime1
     */
    @ScriptApi
    public static boolean isBeforeOrEqual(
            @Nullable final ZonedDateTime dateTime1,
            @Nullable final ZonedDateTime dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return false;
        }

        return dateTime1.isBefore(dateTime2) || dateTime1.equals(dateTime2);
    }

    /**
     * Evaluates whether one instant value is after a second instant value.
     *
     * @param instant1 first instant
     * @param instant2 second instant
     * @return {@code true} if instant1 is after instant2; otherwise, {@code false} if either value is {@code null} or
     *         if instant2 is equal to or after instant1
     */
    @ScriptApi
    public static boolean isAfter(@Nullable final Instant instant1, @Nullable final Instant instant2) {
        if (instant1 == null || instant2 == null) {
            return false;
        }

        return instant1.isAfter(instant2);
    }

    /**
     * Evaluates whether one zoned date time value is after a second zoned date time value.
     *
     * @param dateTime1 first zoned date time
     * @param dateTime2 second zoned date time
     * @return {@code true} if dateTime1 is after dateTime2; otherwise, {@code false} if either value is {@code null} or
     *         if dateTime2 is equal to or after dateTime1
     */
    @ScriptApi
    public static boolean isAfter(@Nullable final ZonedDateTime dateTime1, @Nullable final ZonedDateTime dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return false;
        }

        return dateTime1.isAfter(dateTime2);
    }

    /**
     * Evaluates whether one instant value is after or equal to a second instant value.
     *
     * @param instant1 first instant
     * @param instant2 second instant
     * @return {@code true} if instant1 is after or equal to instant2; otherwise, {@code false} if either value is
     *         {@code null} or if instant2 is after instant1
     */
    @ScriptApi
    public static boolean isAfterOrEqual(@Nullable final Instant instant1, @Nullable final Instant instant2) {
        if (instant1 == null || instant2 == null) {
            return false;
        }

        return instant1.isAfter(instant2) || instant1.equals(instant2);
    }

    /**
     * Evaluates whether one zoned date time value is after or equal to a second zoned date time value.
     *
     * @param dateTime1 first zoned date time
     * @param dateTime2 second zoned date time
     * @return {@code true} if dateTime1 is after or equal to dateTime2; otherwise, {@code false} if either value is
     *         {@code null} or if dateTime2 is after dateTime1
     */
    @ScriptApi
    public static boolean isAfterOrEqual(@Nullable final ZonedDateTime dateTime1, @Nullable ZonedDateTime dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return false;
        }

        return dateTime1.isAfter(dateTime2) || dateTime1.equals(dateTime2);
    }

    // endregion

    // region Chronology

    /**
     * Returns the number of nanoseconds that have elapsed since the top of the millisecond.
     *
     * @param instant time
     * @return {@link QueryConstants#NULL_INT} if the input is {@code null}; otherwise, number of nanoseconds that have
     *         elapsed since the top of the millisecond
     */
    @ScriptApi
    public static int nanosOfMilli(@Nullable final Instant instant) {
        if (instant == null) {
            return NULL_INT;
        }

        return (int) (epochNanos(instant) % 1000000);
    }

    /**
     * Returns the number of nanoseconds that have elapsed since the top of the millisecond.
     *
     * @param dateTime time
     * @return {@link QueryConstants#NULL_INT} if the input is {@code null}; otherwise, number of nanoseconds that have
     *         elapsed since the top of the millisecond
     */
    @ScriptApi
    public static int nanosOfMilli(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_INT;
        }

        return nanosOfMilli(toInstant(dateTime));
    }

    /**
     * Returns the number of microseconds that have elapsed since the top of the millisecond. Nanoseconds are rounded,
     * not dropped -- '20:41:39.123456700' has 457 micros, not 456.
     *
     * @param instant time
     * @return {@link QueryConstants#NULL_INT} if the input is {@code null}; otherwise, number of microseconds that have
     *         elapsed since the top of the millisecond
     */
    @ScriptApi
    public static int microsOfMilli(@Nullable final Instant instant) {
        if (instant == null) {
            return NULL_INT;
        }

        return (int) Math.round((epochNanos(instant) % 1000000) / 1000d);
    }

    /**
     * Returns the number of microseconds that have elapsed since the top of the millisecond. Nanoseconds are rounded,
     * not dropped -- '20:41:39.123456700' has 457 micros, not 456.
     *
     * @param dateTime time
     * @return {@link QueryConstants#NULL_INT} if the input is {@code null}; otherwise, number of microseconds that have
     *         elapsed since the top of the millisecond
     */
    @ScriptApi
    public static int microsOfMilli(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_INT;
        }

        return microsOfMilli(toInstant(dateTime));
    }

    /**
     * Returns the number of nanoseconds that have elapsed since the top of the second.
     *
     * @param instant time
     * @param timeZone time zone
     * @return {@link QueryConstants#NULL_LONG} if either input is {@code null}; otherwise, number of nanoseconds that
     *         have elapsed since the top of the second
     */
    @ScriptApi
    public static long nanosOfSecond(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return NULL_LONG;
        }

        return nanosOfSecond(toZonedDateTime(instant, timeZone));
    }

    /**
     * Returns the number of nanoseconds that have elapsed since the top of the second.
     *
     * @param dateTime time
     * @return {@link QueryConstants#NULL_LONG} if either input is {@code null}; otherwise, number of nanoseconds that
     *         have elapsed since the top of the second
     */
    @ScriptApi
    public static long nanosOfSecond(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return dateTime.getNano();
    }

    /**
     * Returns the number of microseconds that have elapsed since the top of the second.
     *
     * @param instant time
     * @param timeZone time zone
     * @return {@link QueryConstants#NULL_LONG} if either input is {@code null}; otherwise, number of microseconds that
     *         have elapsed since the top of the second
     */
    @ScriptApi
    public static long microsOfSecond(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return NULL_LONG;
        }

        return nanosToMicros(nanosOfSecond(instant, timeZone));
    }

    /**
     * Returns the number of microseconds that have elapsed since the top of the second.
     *
     * @param dateTime time
     * @return {@link QueryConstants#NULL_LONG} if either input is {@code null}; otherwise, number of microseconds that
     *         have elapsed since the top of the second
     */
    @ScriptApi
    public static long microsOfSecond(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return nanosToMicros(nanosOfSecond(dateTime));
    }

    /**
     * Returns the number of milliseconds that have elapsed since the top of the second.
     *
     * @param instant time
     * @param timeZone time zone
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, number of milliseconds that
     *         have elapsed since the top of the second
     */
    @ScriptApi
    public static int millisOfSecond(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return NULL_INT;
        }

        return (int) nanosToMillis(nanosOfSecond(instant, timeZone));
    }

    /**
     * Returns the number of milliseconds that have elapsed since the top of the second.
     *
     * @param dateTime time
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, number of milliseconds that
     *         have elapsed since the top of the second
     */
    @ScriptApi
    public static int millisOfSecond(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_INT;
        }

        return (int) nanosToMillis(nanosOfSecond(dateTime));
    }

    /**
     * Returns the number of seconds that have elapsed since the top of the minute.
     *
     * @param instant time
     * @param timeZone time zone
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, number of seconds that have
     *         elapsed since the top of the minute
     */
    @ScriptApi
    public static int secondOfMinute(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return NULL_INT;
        }

        return toZonedDateTime(instant, timeZone).getSecond();
    }

    /**
     * Returns the number of seconds that have elapsed since the top of the minute.
     *
     * @param dateTime time
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, number of seconds that have
     *         elapsed since the top of the minute
     */
    @ScriptApi
    public static int secondOfMinute(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_INT;
        }

        return dateTime.getSecond();
    }

    /**
     * Returns the number of minutes that have elapsed since the top of the hour.
     *
     * @param instant time
     * @param timeZone time zone
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, number of minutes that have
     *         elapsed since the top of the hour
     */
    @ScriptApi
    public static int minuteOfHour(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return NULL_INT;
        }

        return toZonedDateTime(instant, timeZone).getMinute();
    }

    /**
     * Returns the number of minutes that have elapsed since the top of the hour.
     *
     * @param dateTime time
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, number of minutes that have
     *         elapsed since the top of the hour
     */
    @ScriptApi
    public static int minuteOfHour(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_INT;
        }

        return dateTime.getMinute();
    }

    /**
     * Returns the number of nanoseconds that have elapsed since the start of the day.
     *
     * @param instant time
     * @param timeZone time zone
     * @param asLocalTime If {@code true}, returns the number of nanos from the start of the day according to the local
     *        time. In this case, 9:30AM always returns the same value. If {@code false}, returns the number of nanos
     *        from the start of the day. On days when daylight savings time events occur, results may be different from
     *        what is expected based upon the local time. For example, on daylight savings time change days, 9:30AM may
     *        be earlier or later in the day based upon if the daylight savings time adjustment is forwards or
     *        backwards. On non DST days, the result is the same regardless of the value of {@code asLocalTime}.
     * @return {@link QueryConstants#NULL_LONG} if either input is {@code null}; otherwise, number of nanoseconds that
     *         have elapsed since the start of the day
     */
    @ScriptApi
    public static long nanosOfDay(@Nullable final Instant instant, @Nullable final ZoneId timeZone,
            final boolean asLocalTime) {
        if (instant == null || timeZone == null) {
            return NULL_LONG;
        }

        return nanosOfDay(toZonedDateTime(instant, timeZone), asLocalTime);
    }

    /**
     * Returns the number of nanoseconds that have elapsed since the start of the day.
     *
     * @param dateTime time
     * @param asLocalTime If {@code true}, returns the number of nanos from the start of the day according to the local
     *        time. In this case, 9:30AM always returns the same value. If {@code false}, returns the number of nanos
     *        from the start of the day. On days when daylight savings time events occur, results may be different from
     *        what is expected based upon the local time. For example, on daylight savings time change days, 9:30AM may
     *        be earlier or later in the day based upon if the daylight savings time adjustment is forwards or
     *        backwards. On non DST days, the result is the same regardless of the value of {@code asLocalTime}.
     * @return {@link QueryConstants#NULL_LONG} if either input is {@code null}; otherwise, number of nanoseconds that
     *         have elapsed since the start of the day
     */
    @ScriptApi
    public static long nanosOfDay(@Nullable final ZonedDateTime dateTime, final boolean asLocalTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        if (asLocalTime) {
            return dateTime.toLocalTime().toNanoOfDay();
        } else {
            return epochNanos(dateTime) - epochNanos(atMidnight(dateTime));
        }
    }

    /**
     * Returns the number of nanoseconds that have elapsed since the start of the day.
     *
     * @param localDateTime local date time
     * @return {@link QueryConstants#NULL_LONG} if input is {@code null}; otherwise, number of nanoseconds that have
     *         elapsed since the start of the day
     */
    @ScriptApi
    public static long nanosOfDay(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return NULL_LONG;
        }

        return localDateTime.toLocalTime().toNanoOfDay();
    }

    /**
     * Returns the number of nanoseconds that have elapsed since the start of the day.
     *
     * @param localTime local time
     * @return {@link QueryConstants#NULL_LONG} if input is {@code null}; otherwise, number of nanoseconds that have
     *         elapsed since the start of the day
     */
    @ScriptApi
    public static long nanosOfDay(@Nullable final LocalTime localTime) {
        if (localTime == null) {
            return NULL_LONG;
        }

        return localTime.toNanoOfDay();
    }

    /**
     * Returns the number of milliseconds that have elapsed since the start of the day.
     *
     * @param instant time
     * @param timeZone time zone
     * @param asLocalTime If {@code true}, returns the number of milliseconds from the start of the day according to the
     *        local time. In this case, 9:30AM always returns the same value. If {@code false}, returns the number of
     *        milliseconds from the start of the day. On days when daylight savings time events occur, results may be
     *        different from what is expected based upon the local time. For example, on daylight savings time change
     *        days, 9:30AM may be earlier or later in the day based upon if the daylight savings time adjustment is
     *        forwards or backwards. On non DST days, the result is the same regardless of the value of
     *        {@code asLocalTime}.
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, number of milliseconds that
     *         have elapsed since the start of the day
     */
    @ScriptApi
    public static int millisOfDay(@Nullable final Instant instant, @Nullable final ZoneId timeZone,
            final boolean asLocalTime) {
        if (instant == null || timeZone == null) {
            return NULL_INT;
        }

        return (int) nanosToMillis(nanosOfDay(instant, timeZone, asLocalTime));
    }

    /**
     * Returns the number of milliseconds that have elapsed since the start of the day.
     *
     * @param dateTime time
     * @param asLocalTime If {@code true}, returns the number of milliseconds from the start of the day according to the
     *        local time. In this case, 9:30AM always returns the same value. If {@code false}, returns the number of
     *        milliseconds from the start of the day. On days when daylight savings time events occur, results may be
     *        different from what is expected based upon the local time. For example, on daylight savings time change
     *        days, 9:30AM may be earlier or later in the day based upon if the daylight savings time adjustment is
     *        forwards or backwards. On non DST days, the result is the same regardless of the value of
     *        {@code asLocalTime}.
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, number of milliseconds that
     *         have elapsed since the start of the day
     */
    @ScriptApi
    public static int millisOfDay(@Nullable final ZonedDateTime dateTime, final boolean asLocalTime) {
        if (dateTime == null) {
            return NULL_INT;
        }

        return (int) nanosToMillis(nanosOfDay(dateTime, asLocalTime));
    }

    /**
     * Returns the number of milliseconds that have elapsed since the start of the day.
     *
     * @param localDateTime local date time
     * @return {@link QueryConstants#NULL_INT} if input is {@code null}; otherwise, number of milliseconds that have
     *         elapsed since the start of the day
     */
    @ScriptApi
    public static int millisOfDay(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return NULL_INT;
        }

        return (int) nanosToMillis(localDateTime.toLocalTime().toNanoOfDay());
    }

    /**
     * Returns the number of milliseconds that have elapsed since the start of the day.
     *
     * @param localTime local time
     * @return {@link QueryConstants#NULL_INT} if input is {@code null}; otherwise, number of milliseconds that have
     *         elapsed since the start of the day
     */
    @ScriptApi
    public static int millisOfDay(@Nullable final LocalTime localTime) {
        if (localTime == null) {
            return NULL_INT;
        }

        return (int) nanosToMillis(localTime.toNanoOfDay());
    }

    /**
     * Returns the number of seconds that have elapsed since the start of the day.
     *
     * @param instant time
     * @param timeZone time zone
     * @param asLocalTime If {@code true}, returns the number of nanos from the start of the day according to the local
     *        time. In this case, 9:30AM always returns the same value. If {@code false}, returns the number of nanos
     *        from the start of the day. On days when daylight savings time events occur, results may be different from
     *        what is expected based upon the local time. For example, on daylight savings time change days, 9:30AM may
     *        be earlier or later in the day based upon if the daylight savings time adjustment is forwards or
     *        backwards. On non DST days, the result is the same regardless of the value of {@code asLocalTime}.
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, number of seconds that have
     *         elapsed since the start of the day
     */
    @ScriptApi
    public static int secondOfDay(@Nullable final Instant instant, @Nullable final ZoneId timeZone,
            final boolean asLocalTime) {
        if (instant == null || timeZone == null) {
            return NULL_INT;
        }

        return (int) nanosToSeconds(nanosOfDay(instant, timeZone, asLocalTime));
    }

    /**
     * Returns the number of seconds that have elapsed since the start of the day.
     *
     * @param dateTime time
     * @param asLocalTime If {@code true}, returns the number of seconds from the start of the day according to the
     *        local time. In this case, 9:30AM always returns the same value. If {@code false}, returns the number of
     *        seconds from the start of the day. On days when daylight savings time events occur, results may be
     *        different from what is expected based upon the local time. For example, on daylight savings time change
     *        days, 9:30AM may be earlier or later in the day based upon if the daylight savings time adjustment is
     *        forwards or backwards. On non DST days, the result is the same regardless of the value of
     *        {@code asLocalTime}.
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, number of seconds that have
     *         elapsed since the start of the day
     */
    @ScriptApi
    public static int secondOfDay(@Nullable final ZonedDateTime dateTime, final boolean asLocalTime) {
        if (dateTime == null) {
            return NULL_INT;
        }

        return (int) nanosToSeconds(nanosOfDay(dateTime, asLocalTime));
    }

    /**
     * Returns the number of seconds that have elapsed since the start of the day.
     *
     * @param localDateTime local date time
     * @return {@link QueryConstants#NULL_INT} if input is {@code null}; otherwise, number of seconds that have elapsed
     *         since the start of the day
     */
    @ScriptApi
    public static int secondOfDay(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return NULL_INT;
        }

        return localDateTime.toLocalTime().toSecondOfDay();
    }

    /**
     * Returns the number of seconds that have elapsed since the start of the day.
     *
     * @param localTime local time
     * @return {@link QueryConstants#NULL_INT} if input is {@code null}; otherwise, number of seconds that have elapsed
     *         since the start of the day
     */
    @ScriptApi
    public static int secondOfDay(@Nullable final LocalTime localTime) {
        if (localTime == null) {
            return NULL_INT;
        }

        return localTime.toSecondOfDay();
    }

    /**
     * Returns the number of minutes that have elapsed since the start of the day.
     *
     * @param instant time
     * @param timeZone time zone
     * @param asLocalTime If {@code true}, returns the number of minutes from the start of the day according to the
     *        local time. In this case, 9:30AM always returns the same value. If {@code false}, returns the number of
     *        minutes from the start of the day. On days when daylight savings time events occur, results may be
     *        different from what is expected based upon the local time. For example, on daylight savings time change
     *        days, 9:30AM may be earlier or later in the day based upon if the daylight savings time adjustment is
     *        forwards or backwards. On non DST days, the result is the same as if asLocalTime is false.
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, number of minutes that have
     *         elapsed since the start of the day
     */
    @ScriptApi
    public static int minuteOfDay(@Nullable final Instant instant, @Nullable final ZoneId timeZone,
            final boolean asLocalTime) {
        if (instant == null || timeZone == null) {
            return NULL_INT;
        }

        return secondOfDay(instant, timeZone, asLocalTime) / 60;
    }

    /**
     * Returns the number of minutes that have elapsed since the start of the day.
     *
     * @param dateTime time
     * @param asLocalTime If {@code true}, returns the number of minutes from the start of the day according to the
     *        local time. In this case, 9:30AM always returns the same value. If {@code false}, returns the number of
     *        minutes from the start of the day. On days when daylight savings time events occur, results may be
     *        different from what is expected based upon the local time. For example, on daylight savings time change
     *        days, 9:30AM may be earlier or later in the day based upon if the daylight savings time adjustment is
     *        forwards or backwards. On non DST days, the result is the same regardless of the value of
     *        {@code asLocalTime}.
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, number of minutes that have
     *         elapsed since the start of the day
     */
    @ScriptApi
    public static int minuteOfDay(@Nullable final ZonedDateTime dateTime, final boolean asLocalTime) {
        if (dateTime == null) {
            return NULL_INT;
        }

        return secondOfDay(dateTime, asLocalTime) / 60;
    }

    /**
     * Returns the number of minutes that have elapsed since the start of the day.
     *
     * @param localDateTime local date time
     * @return {@link QueryConstants#NULL_INT} if input is {@code null}; otherwise, number of minutes that have elapsed
     *         since the start of the day
     */
    @ScriptApi
    public static int minuteOfDay(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return NULL_INT;
        }

        return secondOfDay(localDateTime) / 60;
    }

    /**
     * Returns the number of minutes that have elapsed since the start of the day.
     *
     * @param localTime local time
     * @return {@link QueryConstants#NULL_INT} if input is {@code null}; otherwise, number of minutes that have elapsed
     *         since the start of the day
     */
    @ScriptApi
    public static int minuteOfDay(@Nullable final LocalTime localTime) {
        if (localTime == null) {
            return NULL_INT;
        }

        return secondOfDay(localTime) / 60;
    }

    /**
     * Returns the number of hours that have elapsed since the start of the day.
     *
     * @param instant time
     * @param timeZone time zone
     * @param asLocalTime If {@code true}, returns the number of hours from the start of the day according to the local
     *        time. In this case, 9:30AM always returns the same value. If {@code false}, returns the number of hours
     *        from the start of the day. On days when daylight savings time events occur, results may be different from
     *        what is expected based upon the local time. For example, on daylight savings time change days, 9:30AM may
     *        be earlier or later in the day based upon if the daylight savings time adjustment is forwards or
     *        backwards. On non DST days, the result is the same regardless of the value of {@code asLocalTime}.
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, number of hours that have
     *         elapsed since the start of the day
     */
    @ScriptApi
    public static int hourOfDay(@Nullable final Instant instant, @Nullable final ZoneId timeZone,
            final boolean asLocalTime) {
        if (instant == null || timeZone == null) {
            return NULL_INT;
        }

        return hourOfDay(toZonedDateTime(instant, timeZone), asLocalTime);
    }

    /**
     * Returns the number of hours that have elapsed since the start of the day.
     *
     * @param dateTime time
     * @param asLocalTime If {@code true}, returns the number of hours from the start of the day according to the local
     *        time. In this case, 9:30AM always returns the same value. If {@code false}, returns the number of hours
     *        from the start of the day. On days when daylight savings time events occur, results may be different from
     *        what is expected based upon the local time. For example, on daylight savings time change days, 9:30AM may
     *        be earlier or later in the day based upon if the daylight savings time adjustment is forwards or
     *        backwards. On non DST days, the result is the same regardless of the value of {@code asLocalTime}.
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, number of hours that have
     *         elapsed since the start of the day
     */
    @ScriptApi
    public static int hourOfDay(@Nullable final ZonedDateTime dateTime, final boolean asLocalTime) {
        if (dateTime == null) {
            return NULL_INT;
        }

        return minuteOfDay(dateTime, asLocalTime) / 60;
    }

    /**
     * Returns the number of hours that have elapsed since the start of the day.
     *
     * @param localDateTime local date time
     * @return {@link QueryConstants#NULL_INT} if input is {@code null}; otherwise, number of hours that have elapsed
     *         since the start of the day
     */
    @ScriptApi
    public static int hourOfDay(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return NULL_INT;
        }

        return localDateTime.getHour();
    }

    /**
     * Returns the number of hours that have elapsed since the start of the day.
     *
     * @param localTime local time
     * @return {@link QueryConstants#NULL_INT} if input is {@code null}; otherwise, number of hours that have elapsed
     *         since the start of the day
     */
    @ScriptApi
    public static int hourOfDay(@Nullable final LocalTime localTime) {
        if (localTime == null) {
            return NULL_INT;
        }

        return localTime.getHour();
    }

    /**
     * Returns athe day of the week for a {@link ZonedDateTime}.
     *
     * @param localDateTime local date time to find the day of the week of
     * @return {@code null} if {@code localDateTime} is {@code null}; otherwise, the day of the week
     */
    @ScriptApi
    public static DayOfWeek dayOfWeek(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return null;
        }

        return localDateTime.getDayOfWeek();
    }

    /**
     * Returns the day of the week for a {@link LocalDate}.
     *
     * @param localDate local date to find the day of the week of
     * @return {@code null} if {@code localDate} is {@code null}; otherwise, the day of the week
     */
    @ScriptApi
    public static DayOfWeek dayOfWeek(@Nullable final LocalDate localDate) {
        if (localDate == null) {
            return null;
        }

        return localDate.getDayOfWeek();
    }

    /**
     * Returns the day of the week for an {@link Instant} in the specified time zone.
     *
     * @param instant time to find the day of the week of
     * @param timeZone time zone
     * @return {@code null} if either input is {@code null}; otherwise, the day of the week
     */
    @ScriptApi
    public static DayOfWeek dayOfWeek(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return null;
        }

        return dayOfWeek(toZonedDateTime(instant, timeZone));
    }

    /**
     * Returns the day of the week for a {@link ZonedDateTime} in the specified time zone.
     *
     * @param dateTime time to find the day of the week of
     * @return {@code null} if either input is {@code null}; otherwise, the day of the week
     */
    @ScriptApi
    public static DayOfWeek dayOfWeek(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }

        return dateTime.getDayOfWeek();
    }

    /**
     * Returns a 1-based int value of the day of the week for a {@link LocalDateTime}, with 1 being Monday and 7 being
     * Sunday.
     *
     * @param localDateTime local date time to find the day of the week of
     * @return {@link QueryConstants#NULL_INT} if {@code localDateTime} is {@code null}; otherwise, the day of the week
     */
    @ScriptApi
    public static int dayOfWeekValue(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return localDateTime.getDayOfWeek().getValue();
    }

    /**
     * Returns a 1-based int value of the day of the week for a {@link LocalDate}, with 1 being Monday and 7 being
     * Sunday.
     *
     * @param localDate local date to find the day of the week of
     * @return {@link QueryConstants#NULL_INT} if {@code localDate} is {@code null}; otherwise, the day of the week
     */
    @ScriptApi
    public static int dayOfWeekValue(@Nullable final LocalDate localDate) {
        if (localDate == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return localDate.getDayOfWeek().getValue();
    }

    /**
     * Returns a 1-based int value of the day of the week for an {@link Instant} in the specified time zone, with 1
     * being Monday and 7 being Sunday.
     *
     * @param instant time to find the day of the week of
     * @param timeZone time zone
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, the day of the week
     */
    @ScriptApi
    public static int dayOfWeekValue(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return NULL_INT;
        }

        return dayOfWeekValue(toZonedDateTime(instant, timeZone));
    }

    /**
     * Returns a 1-based int value of the day of the week for a {@link ZonedDateTime} in the specified time zone, with 1
     * being Monday and 7 being Sunday.
     *
     * @param dateTime time to find the day of the week of
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, the day of the week
     */
    @ScriptApi
    public static int dayOfWeekValue(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_INT;
        }

        return dateTime.getDayOfWeek().getValue();
    }

    /**
     * Returns a 1-based int value of the day of the month for a {@link LocalDateTime}. The first day of the month
     * returns 1, the second day returns 2, etc.
     *
     * @param localDateTime local date time to find the day of the month of
     * @return A {@link QueryConstants#NULL_INT} if {@code localDateTime} is {@code null}; otherwise, the day of the
     *         month
     */
    @ScriptApi
    public static int dayOfMonth(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return localDateTime.getDayOfMonth();
    }

    /**
     * Returns a 1-based int value of the day of the month for a {@link LocalDate}. The first day of the month returns
     * 1, the second day returns 2, etc.
     *
     * @param localDate local date to find the day of the month of
     * @return A {@link QueryConstants#NULL_INT} if {@code localDate} is {@code null}; otherwise, the day of the month
     */
    @ScriptApi
    public static int dayOfMonth(@Nullable final LocalDate localDate) {
        if (localDate == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return localDate.getDayOfMonth();
    }

    /**
     * Returns a 1-based int value of the day of the month for an {@link Instant} and specified time zone. The first day
     * of the month returns 1, the second day returns 2, etc.
     *
     * @param instant time to find the day of the month of
     * @param timeZone time zone
     * @return A {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, the day of the month
     */
    @ScriptApi
    public static int dayOfMonth(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return NULL_INT;
        }

        return dayOfMonth(toZonedDateTime(instant, timeZone));
    }

    /**
     * Returns a 1-based int value of the day of the month for a {@link ZonedDateTime} and specified time zone. The
     * first day of the month returns 1, the second day returns 2, etc.
     *
     * @param dateTime time to find the day of the month of
     * @return A {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, the day of the month
     */
    @ScriptApi
    public static int dayOfMonth(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_INT;
        }

        return dateTime.getDayOfMonth();
    }

    /**
     * Returns a 1-based int value of the day of the year (Julian date) for a {@link LocalDateTime}. The first day of
     * the year returns 1, the second day returns 2, etc.
     *
     * @param localDateTime local date time to find the day of the year of
     * @return {@link QueryConstants#NULL_INT} if {@code localDateTime} is {@code null}; otherwise, the day of the year
     */
    @ScriptApi
    public static int dayOfYear(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return localDateTime.getDayOfYear();
    }

    /**
     * Returns a 1-based int value of the day of the year (Julian date) for a {@link LocalDate}. The first day of the
     * year returns 1, the second day returns 2, etc.
     *
     * @param localDate local date to find the day of the year of
     * @return {@link QueryConstants#NULL_INT} if {@code localDate} is {@code null}; otherwise, the day of the year
     */
    @ScriptApi
    public static int dayOfYear(@Nullable final LocalDate localDate) {
        if (localDate == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return localDate.getDayOfYear();
    }

    /**
     * Returns a 1-based int value of the day of the year (Julian date) for an {@link Instant} in the specified time
     * zone. The first day of the year returns 1, the second day returns 2, etc.
     *
     * @param instant time to find the day of the year of
     * @param timeZone time zone
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, the day of the year
     */
    @ScriptApi
    public static int dayOfYear(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return NULL_INT;
        }

        return dayOfYear(toZonedDateTime(instant, timeZone));
    }

    /**
     * Returns a 1-based int value of the day of the year (Julian date) for a {@link ZonedDateTime} in the specified
     * time zone. The first day of the year returns 1, the second day returns 2, etc.
     *
     * @param dateTime time to find the day of the year of
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, the day of the year
     */
    @ScriptApi
    public static int dayOfYear(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_INT;
        }

        return dateTime.getDayOfYear();
    }

    /**
     * Returns a 1-based int value of the month of the year (Julian date) for a {@link LocalDateTime}. January is 1,
     * February is 2, etc.
     *
     * @param localDateTime local date time to find the month of the year of
     * @return {@link QueryConstants#NULL_INT} if {@code localDateTime} is {@code null}; otherwise, the month of the
     *         year
     */
    @ScriptApi
    public static int monthOfYear(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return localDateTime.getMonthValue();
    }

    /**
     * Returns a 1-based int value of the month of the year (Julian date) for a {@link LocalDate}. January is 1,
     * February is 2, etc.
     *
     * @param localDate local date to find the month of the year of
     * @return {@link QueryConstants#NULL_INT} if {@code localDate} is {@code null}; otherwise, the month of the year
     */
    @ScriptApi
    public static int monthOfYear(@Nullable final LocalDate localDate) {
        if (localDate == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return localDate.getMonthValue();
    }

    /**
     * Returns a 1-based int value of the month of the year (Julian date) for an {@link Instant} in the specified time
     * zone. January is 1, February is 2, etc.
     *
     * @param instant time to find the month of the year of
     * @param timeZone time zone
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, the month of the year
     */
    @ScriptApi
    public static int monthOfYear(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return NULL_INT;
        }

        return monthOfYear(toZonedDateTime(instant, timeZone));
    }

    /**
     * Returns a 1-based int value of the month of the year (Julian date) for a {@link ZonedDateTime} in the specified
     * time zone. January is 1, February is 2, etc.
     *
     * @param dateTime time to find the month of the year of
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, the month of the year
     */
    @ScriptApi
    public static int monthOfYear(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_INT;
        }

        return dateTime.getMonthValue();
    }

    /**
     * Returns the year for a {@link LocalDateTime}.
     *
     * @param localDateTime local date to find the year of
     * @return {@link QueryConstants#NULL_INT} if {@code localDateTime} is {@code null}; otherwise, the year
     */
    @ScriptApi
    public static int year(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return localDateTime.getYear();
    }

    /**
     * Returns the year for a {@link LocalDate}.
     *
     * @param localDate local date to find the year of
     * @return {@link QueryConstants#NULL_INT} if {@code localDate} is {@code null}; otherwise, the year
     */
    @ScriptApi
    public static int year(@Nullable final LocalDate localDate) {
        if (localDate == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return localDate.getYear();
    }

    /**
     * Returns the year for an {@link Instant} in the specified time zone.
     *
     * @param instant time to find the year of
     * @param timeZone time zone
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, the year
     */
    @ScriptApi
    public static int year(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return NULL_INT;
        }

        return year(toZonedDateTime(instant, timeZone));
    }

    /**
     * Returns the year for a {@link ZonedDateTime} in the specified time zone.
     *
     * @param dateTime time to find the year of
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, the year
     */
    @ScriptApi
    public static int year(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_INT;
        }

        return dateTime.getYear();
    }

    /**
     * Returns the year of the century (two-digit year) for a {@link LocalDateTime}.
     *
     * @param localDateTime local date time to find the year of
     * @return {@link QueryConstants#NULL_INT} if {@code localDateTime} is {@code null}; otherwise, the year of the
     *         century (two-digit year)
     */
    @ScriptApi
    public static int yearOfCentury(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return year(localDateTime) % 100;
    }

    /**
     * Returns the year of the century (two-digit year) for a {@link LocalDate}.
     *
     * @param localDate local date to find the year of
     * @return {@link QueryConstants#NULL_INT} if {@code localDate} is {@code null}; otherwise, the year of the century
     *         (two-digit year)
     */
    @ScriptApi
    public static int yearOfCentury(@Nullable final LocalDate localDate) {
        if (localDate == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return year(localDate) % 100;
    }

    /**
     * Returns the year of the century (two-digit year) for an {@link Instant} in the specified time zone.
     *
     * @param instant time to find the year of
     * @param timeZone time zone
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, the year of the century
     *         (two-digit year)
     */
    @ScriptApi
    public static int yearOfCentury(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return NULL_INT;
        }

        return year(instant, timeZone) % 100;
    }

    /**
     * Returns the year of the century (two-digit year) for a {@link ZonedDateTime} in the specified time zone.
     *
     * @param dateTime time to find the day of the month of
     * @return {@link QueryConstants#NULL_INT} if either input is {@code null}; otherwise, the year of the century
     *         (two-digit year)
     */
    @ScriptApi
    public static int yearOfCentury(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_INT;
        }

        return year(dateTime) % 100;
    }

    /**
     * Returns a {@link ZonedDateTime} for the prior midnight in the specified time zone.
     *
     * @param localDateTime local date time to compute the prior midnight for
     * @param timeZone time zone
     * @return {@code null} if either input is {@code null}; otherwise a {@link ZonedDateTime} representing the prior
     *         midnight in the specified time zone
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime atMidnight(@Nullable final LocalDateTime localDateTime, @Nullable ZoneId timeZone) {
        if (localDateTime == null || timeZone == null) {
            return null;
        }

        return localDateTime.toLocalDate().atStartOfDay(timeZone);
    }

    /**
     * Returns a {@link ZonedDateTime} for the prior midnight in the specified time zone.
     *
     * @param localDate local date to compute the prior midnight for
     * @param timeZone time zone
     * @return {@code null} if either input is {@code null}; otherwise a {@link ZonedDateTime} representing the prior
     *         midnight in the specified time zone
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime atMidnight(@Nullable final LocalDate localDate, @Nullable ZoneId timeZone) {
        if (localDate == null || timeZone == null) {
            return null;
        }

        return localDate.atStartOfDay(timeZone);
    }

    /**
     * Returns an {@link Instant} for the prior midnight in the specified time zone.
     *
     * @param instant time to compute the prior midnight for
     * @param timeZone time zone
     * @return {@code null} if either input is {@code null}; otherwise an {@link Instant} representing the prior
     *         midnight in the specified time zone
     */
    @ScriptApi
    @Nullable
    public static Instant atMidnight(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return null;
        }

        return toInstant(atMidnight(toZonedDateTime(instant, timeZone)));
    }

    /**
     * Returns a {@link ZonedDateTime} for the prior midnight in the specified time zone.
     *
     * @param dateTime time to compute the prior midnight for
     * @return {@code null} if either input is {@code null}; otherwise a {@link ZonedDateTime} representing the prior
     *         midnight in the specified time zone
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime atMidnight(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }

        return dateTime.toLocalDate().atStartOfDay(dateTime.getZone());
    }

    // endregion

    // region Binning

    /**
     * Returns an {@link Instant} value, which is at the starting (lower) end of a time range defined by the interval
     * nanoseconds. For example, a five-minute {@code intervalNanos} value would return the instant value for the start
     * of the five-minute window that contains the input instant.
     *
     * @param instant instant for which to evaluate the start of the containing window
     * @param intervalNanos size of the window in nanoseconds
     * @return {@code null} if either input is {@code null}; otherwise, an {@link Instant} representing the start of the
     *         window
     */
    @ScriptApi
    @Nullable
    public static Instant lowerBin(@Nullable final Instant instant, long intervalNanos) {
        if (instant == null || intervalNanos == NULL_LONG) {
            return null;
        }

        return epochNanosToInstant(Numeric.lowerBin(epochNanos(instant), intervalNanos));
    }

    /**
     * Returns an {@link Instant} value, which is at the starting (lower) end of a time range defined by the interval
     * nanoseconds. For example, a five-minute {@code interval} value would return the instant value for the start of
     * the five-minute window that contains the input instant.
     *
     * @param instant instant for which to evaluate the start of the containing window
     * @param interval size of the window
     * @return {@code null} if either input is {@code null}; otherwise, an {@link Instant} representing the start of the
     *         window
     */
    @ScriptApi
    @Nullable
    public static Instant lowerBin(@Nullable final Instant instant, Duration interval) {
        if (instant == null || interval == null) {
            return null;
        }

        return lowerBin(instant, interval.toNanos());
    }

    /**
     * Returns a {@link ZonedDateTime} value, which is at the starting (lower) end of a time range defined by the
     * interval nanoseconds. For example, a five-minute {@code intervalNanos} value would return the zoned date time
     * value for the start of the five-minute window that contains the input zoned date time.
     *
     * @param dateTime zoned date time for which to evaluate the start of the containing window
     * @param intervalNanos size of the window in nanoseconds
     * @return {@code null} if either input is {@code null}; otherwise, a {@link ZonedDateTime} representing the start
     *         of the window
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime lowerBin(@Nullable final ZonedDateTime dateTime, long intervalNanos) {
        if (dateTime == null || intervalNanos == NULL_LONG) {
            return null;
        }

        return epochNanosToZonedDateTime(Numeric.lowerBin(epochNanos(dateTime), intervalNanos), dateTime.getZone());
    }

    /**
     * Returns a {@link ZonedDateTime} value, which is at the starting (lower) end of a time range defined by the
     * interval nanoseconds. For example, a five-minute {@code interval} value would return the zoned date time value
     * for the start of the five-minute window that contains the input zoned date time.
     *
     * @param dateTime zoned date time for which to evaluate the start of the containing window
     * @param interval size of the window
     * @return {@code null} if either input is {@code null}; otherwise, a {@link ZonedDateTime} representing the start
     *         of the window
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime lowerBin(@Nullable final ZonedDateTime dateTime, Duration interval) {
        if (dateTime == null || interval == null) {
            return null;
        }

        return lowerBin(dateTime, interval.toNanos());
    }

    /**
     * Returns an {@link Instant} value, which is at the starting (lower) end of a time range defined by the interval
     * nanoseconds. For example, a five-minute {@code intervalNanos} value would return the instant value for the start
     * of the five-minute window that contains the input instant.
     *
     * @param instant instant for which to evaluate the start of the containing window
     * @param intervalNanos size of the window in nanoseconds
     * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
     *        one minute.
     * @return {@code null} if any input is {@code null}; otherwise, an {@link Instant} representing the start of the
     *         window
     */
    @ScriptApi
    @Nullable
    public static Instant lowerBin(@Nullable final Instant instant, long intervalNanos, long offset) {
        if (instant == null || intervalNanos == NULL_LONG || offset == NULL_LONG) {
            return null;
        }

        return epochNanosToInstant(Numeric.lowerBin(epochNanos(instant) - offset, intervalNanos) + offset);
    }

    /**
     * Returns an {@link Instant} value, which is at the starting (lower) end of a time range defined by the interval
     * nanoseconds. For example, a five-minute {@code interval} value would return the instant value for the start of
     * the five-minute window that contains the input instant.
     *
     * @param instant instant for which to evaluate the start of the containing window
     * @param interval size of the window
     * @param offset The window start offset. For example, a value of 'PT1m' would offset all windows by one minute.
     * @return {@code null} if any input is {@code null}; otherwise, an {@link Instant} representing the start of the
     *         window
     */
    @ScriptApi
    @Nullable
    public static Instant lowerBin(@Nullable final Instant instant, Duration interval, Duration offset) {
        if (instant == null || interval == null || offset == null) {
            return null;
        }

        return lowerBin(instant, interval.toNanos(), offset.toNanos());
    }

    /**
     * Returns a {@link ZonedDateTime} value, which is at the starting (lower) end of a time range defined by the
     * interval nanoseconds. For example, a five-minute {@code intervalNanos} value would return the zoned date time
     * value for the start of the five-minute window that contains the input zoned date time.
     *
     * @param dateTime zoned date time for which to evaluate the start of the containing window
     * @param intervalNanos size of the window in nanoseconds
     * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
     *        one minute.
     * @return {@code null} if any input is {@code null}; otherwise, a {@link ZonedDateTime} representing the start of
     *         the window
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime lowerBin(@Nullable final ZonedDateTime dateTime, long intervalNanos, long offset) {
        if (dateTime == null || intervalNanos == NULL_LONG || offset == NULL_LONG) {
            return null;
        }

        return epochNanosToZonedDateTime(Numeric.lowerBin(epochNanos(dateTime) - offset, intervalNanos) + offset,
                dateTime.getZone());
    }

    /**
     * Returns a {@link ZonedDateTime} value, which is at the starting (lower) end of a time range defined by the
     * interval nanoseconds. For example, a five-minute {@code interval} intervalNanos value would return the zoned date
     * time value for the start of the five-minute window that contains the input zoned date time.
     *
     * @param dateTime zoned date time for which to evaluate the start of the containing window
     * @param interval size of the window
     * @param offset The window start offset. For example, a value of MINUTE would offset all windows by one minute.
     * @return {@code null} if any input is {@code null}; otherwise, a {@link ZonedDateTime} representing the start of
     *         the window
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime lowerBin(@Nullable final ZonedDateTime dateTime, Duration interval, Duration offset) {
        if (dateTime == null || interval == null || offset == null) {
            return null;
        }

        return lowerBin(dateTime, interval.toNanos(), offset.toNanos());
    }

    /**
     * Returns an {@link Instant} value, which is at the ending (upper) end of a time range defined by the interval
     * nanoseconds. For example, a five-minute {@code intervalNanos} value would return the instant value for the end of
     * the five-minute window that contains the input instant.
     *
     * @param instant instant for which to evaluate the start of the containing window
     * @param intervalNanos size of the window in nanoseconds
     * @return {@code null} if either input is {@code null}; otherwise, an {@link Instant} representing the end of the
     *         window
     */
    @ScriptApi
    @Nullable
    public static Instant upperBin(@Nullable final Instant instant, long intervalNanos) {
        if (instant == null || intervalNanos == NULL_LONG) {
            return null;
        }

        return epochNanosToInstant(Numeric.upperBin(epochNanos(instant), intervalNanos));
    }

    /**
     * Returns an {@link Instant} value, which is at the ending (upper) end of a time range defined by the interval
     * nanoseconds. For example, a five-minute {@code interval} value would return the instant value for the end of the
     * five-minute window that contains the input instant.
     *
     * @param instant instant for which to evaluate the start of the containing window
     * @param interval size of the window
     * @return {@code null} if either input is {@code null}; otherwise, an {@link Instant} representing the end of the
     *         window
     */
    @ScriptApi
    @Nullable
    public static Instant upperBin(@Nullable final Instant instant, Duration interval) {
        if (instant == null || interval == null) {
            return null;
        }

        return upperBin(instant, interval.toNanos());
    }

    /**
     * Returns a {@link ZonedDateTime} value, which is at the ending (upper) end of a time range defined by the interval
     * nanoseconds. For example, a five-minute {@code intervalNanos} value would return the zoned date time value for
     * the end of the five-minute window that contains the input zoned date time.
     *
     * @param dateTime zoned date time for which to evaluate the start of the containing window
     * @param intervalNanos size of the window in nanoseconds
     * @return {@code null} if either input is {@code null}; otherwise, a {@link ZonedDateTime} representing the end of
     *         the window
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime upperBin(@Nullable final ZonedDateTime dateTime, long intervalNanos) {
        if (dateTime == null || intervalNanos == NULL_LONG) {
            return null;
        }

        return epochNanosToZonedDateTime(Numeric.upperBin(epochNanos(dateTime), intervalNanos), dateTime.getZone());
    }

    /**
     * Returns a {@link ZonedDateTime} value, which is at the ending (upper) end of a time range defined by the interval
     * nanoseconds. For example, a five-minute {@code interval} value would return the zoned date time value for the end
     * of the five-minute window that contains the input zoned date time.
     *
     * @param dateTime zoned date time for which to evaluate the start of the containing window
     * @param interval size of the window
     * @return {@code null} if either input is {@code null}; otherwise, a {@link ZonedDateTime} representing the end of
     *         the window
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime upperBin(@Nullable final ZonedDateTime dateTime, Duration interval) {
        if (dateTime == null || interval == null) {
            return null;
        }

        return upperBin(dateTime, interval.toNanos());
    }

    /**
     * Returns an {@link Instant} value, which is at the ending (upper) end of a time range defined by the interval
     * nanoseconds. For example, a five-minute {@code intervalNanos} value would return the instant value for the end of
     * the five-minute window that contains the input instant.
     *
     * @param instant instant for which to evaluate the start of the containing window
     * @param intervalNanos size of the window in nanoseconds
     * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
     *        one minute.
     * @return {@code null} if any input is {@code null}; otherwise, an {@link Instant} representing the end of the
     *         window
     */
    @ScriptApi
    @Nullable
    public static Instant upperBin(@Nullable final Instant instant, long intervalNanos, long offset) {
        if (instant == null || intervalNanos == NULL_LONG
                || offset == NULL_LONG) {
            return null;
        }

        return epochNanosToInstant(Numeric.upperBin(epochNanos(instant) - offset, intervalNanos) + offset);
    }

    /**
     * Returns an {@link Instant} value, which is at the ending (upper) end of a time range defined by the interval
     * nanoseconds. For example, a five-minute {@code interval} value would return the instant value for the end of the
     * five-minute window that contains the input instant.
     *
     * @param instant instant for which to evaluate the start of the containing window
     * @param interval size of the window
     * @param offset The window start offset. For example, a value of 'PT1m' would offset all windows by one minute.
     * @return {@code null} if any input is {@code null}; otherwise, an {@link Instant} representing the end of the
     *         window
     */
    @ScriptApi
    @Nullable
    public static Instant upperBin(@Nullable final Instant instant, Duration interval, Duration offset) {
        if (instant == null || interval == null || offset == null) {
            return null;
        }

        return upperBin(instant, interval.toNanos(), offset.toNanos());
    }

    /**
     * Returns a {@link ZonedDateTime} value, which is at the ending (upper) end of a time range defined by the interval
     * nanoseconds. For example, a five-minute {@code intervalNanos} value would return the zoned date time value for
     * the end of the five-minute window that contains the input zoned date time.
     *
     * @param dateTime zoned date time for which to evaluate the start of the containing window
     * @param intervalNanos size of the window in nanoseconds
     * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
     *        one minute.
     * @return {@code null} if any input is {@code null}; otherwise, a {@link ZonedDateTime} representing the end of the
     *         window
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime upperBin(@Nullable final ZonedDateTime dateTime, long intervalNanos, long offset) {
        if (dateTime == null || intervalNanos == NULL_LONG
                || offset == NULL_LONG) {
            return null;
        }

        return epochNanosToZonedDateTime(Numeric.upperBin(epochNanos(dateTime) - offset, intervalNanos) + offset,
                dateTime.getZone());
    }

    /**
     * Returns a {@link ZonedDateTime} value, which is at the ending (upper) end of a time range defined by the interval
     * nanoseconds. For example, a five-minute {@code interval} value would return the zoned date time value for the end
     * of the five-minute window that contains the input zoned date time.
     *
     * @param dateTime zoned date time for which to evaluate the start of the containing window
     * @param interval size of the window
     * @param offset The window start offset. For example, a value of 'PT1m' would offset all windows by one minute.
     * @return {@code null} if any input is {@code null}; otherwise, a {@link ZonedDateTime} representing the end of the
     *         window
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime upperBin(@Nullable final ZonedDateTime dateTime, Duration interval, Duration offset) {
        if (dateTime == null || interval == null || offset == null) {
            return null;
        }

        return upperBin(dateTime, interval.toNanos(), offset.toNanos());
    }

    // endregion

    // region Format

    /**
     * Pads a string with zeros.
     *
     * @param str string
     * @param length desired time string length
     * @return input string padded with zeros to the desired length. If the input string is longer than the desired
     *         length, the input string is returned.
     */
    @NotNull
    static String padZeros(@NotNull final String str, final int length) {
        if (length <= str.length()) {
            return str;
        }
        return "0".repeat(length - str.length()) + str;
    }

    /**
     * Returns a nanosecond duration formatted as a "[-]PThhh:mm:ss.nnnnnnnnn" string.
     *
     * @param nanos nanoseconds, or {@code null} if the input is {@link QueryConstants#NULL_LONG}
     * @return the nanosecond duration formatted as a "[-]PThhh:mm:ss.nnnnnnnnn" string
     */
    @ScriptApi
    @Nullable
    public static String formatDurationNanos(long nanos) {
        if (nanos == NULL_LONG) {
            return null;
        }

        StringBuilder buf = new StringBuilder(25);

        if (nanos < 0) {
            buf.append('-');
            nanos = -nanos;
        }

        buf.append("PT");

        int hours = (int) (nanos / 3600000000000L);

        nanos %= 3600000000000L;

        int minutes = (int) (nanos / 60000000000L);

        nanos %= 60000000000L;

        int seconds = (int) (nanos / 1000000000L);

        nanos %= 1000000000L;

        buf.append(hours).append(':').append(padZeros(String.valueOf(minutes), 2)).append(':')
                .append(padZeros(String.valueOf(seconds), 2));

        if (nanos != 0) {
            buf.append('.').append(padZeros(String.valueOf(nanos), 9));
        }

        return buf.toString();
    }

    /**
     * Returns an {@link Instant} formatted as a "yyyy-MM-ddThh:mm:ss.SSSSSSSSS TZ" string.
     *
     * @param instant time to format as a string
     * @param timeZone time zone to use when formatting the string.
     * @return {@code null} if either input is {@code null}; otherwise, the time formatted as a
     *         "yyyy-MM-ddThh:mm:ss.nnnnnnnnn TZ" string
     */
    @ScriptApi
    @Nullable
    public static String formatDateTime(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return null;
        }

        return formatDateTime(toZonedDateTime(instant, timeZone));
    }

    /**
     * Returns a {@link ZonedDateTime} formatted as a "yyyy-MM-ddThh:mm:ss.SSSSSSSSS TZ" string.
     *
     * @param dateTime time to format as a string
     * @return {@code null} if either input is {@code null}; otherwise, the time formatted as a
     *         "yyyy-MM-ddThh:mm:ss.nnnnnnnnn TZ" string
     */
    @ScriptApi
    @Nullable
    public static String formatDateTime(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }

        final String timeZone = TimeZoneAliases.zoneName(dateTime.getZone());
        final String ldt = ISO_LOCAL_DATE_TIME.format(dateTime);
        final StringBuilder sb = new StringBuilder();

        sb.append(ldt);

        int pad = 29 - ldt.length();

        if (ldt.length() == 19) {
            sb.append(".");
            pad--;
        }

        return sb.append("0".repeat(Math.max(0, pad))).append(" ").append(timeZone).toString();
    }

    /**
     * Returns an {@link Instant} formatted as a "yyyy-MM-dd" string.
     *
     * @param instant time to format as a string
     * @param timeZone time zone to use when formatting the string.
     * @return {@code null} if either input is {@code null}; otherwise, the time formatted as a "yyyy-MM-dd" string
     */
    @ScriptApi
    @Nullable
    public static String formatDate(@Nullable final Instant instant, @Nullable final ZoneId timeZone) {
        if (instant == null || timeZone == null) {
            return null;
        }

        return formatDate(toZonedDateTime(instant, timeZone));
    }

    /**
     * Returns a {@link ZonedDateTime} formatted as a "yyyy-MM-dd" string.
     *
     * @param dateTime time to format as a string
     * @return {@code null} if either input is {@code null}; otherwise, the time formatted as a "yyyy-MM-dd" string
     */
    @ScriptApi
    @Nullable
    public static String formatDate(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }

        return ISO_LOCAL_DATE.format(dateTime);
    }

    /**
     * Returns a {@link LocalDateTime} formatted as a "yyyy-MM-dd" string.
     *
     * @param localDateTime local date time to format as a string
     * @return {@code null} if either input is {@code null}; otherwise, the time formatted as a "yyyy-MM-dd" string
     */
    @ScriptApi
    @Nullable
    public static String formatDate(@Nullable final LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return null;
        }

        return ISO_LOCAL_DATE.format(localDateTime);
    }

    /**
     * Returns a {@link LocalDate} formatted as a "yyyy-MM-dd" string.
     *
     * @param localDate local date to format as a string
     * @return {@code null} if either input is {@code null}; otherwise, the time formatted as a "yyyy-MM-dd" string
     */
    @ScriptApi
    @Nullable
    public static String formatDate(@Nullable final LocalDate localDate) {
        if (localDate == null) {
            return null;
        }

        return ISO_LOCAL_DATE.format(localDate);
    }

    // endregion

    // region Parse

    /**
     * Exception type thrown when date time string representations can not be parsed.
     */
    public static class DateTimeParseException extends RuntimeException {
        private DateTimeParseException(String msg) {
            super(msg);
        }

        private DateTimeParseException(String msg, Exception ex) {
            super(msg, ex);
        }
    }

    /**
     * Parses the string argument as a time zone.
     *
     * @param s string to be converted
     * @return a {@link ZoneId} represented by the input string
     * @throws DateTimeParseException if the string cannot be converted
     * @see ZoneId
     * @see TimeZoneAliases
     */
    @ScriptApi
    @NotNull
    public static ZoneId parseTimeZone(@NotNull final String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new DateTimeParseException("Cannot parse time zone (null): " + s);
        }

        try {
            return TimeZoneAliases.zoneId(s);
        } catch (Exception ex) {
            throw new DateTimeParseException("Cannot parse time zone: " + s);
        }
    }

    /**
     * Parses the string argument as a time zone.
     *
     * @param s string to be converted
     * @return a {@link ZoneId} represented by the input string, or {@code null} if the string can not be parsed
     * @see ZoneId
     * @see TimeZoneAliases
     */
    @ScriptApi
    @Nullable
    public static ZoneId parseTimeZoneQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return null;
        }

        try {
            return parseTimeZone(s);
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * Parses the string argument as a time duration in nanoseconds.
     * <p>
     * Time duration strings can be formatted as {@code [-]PT[-]hh:mm:[ss.nnnnnnnnn]} or as a duration string formatted
     * as {@code [-]PnDTnHnMn.nS}.
     *
     * @param s string to be converted
     * @return the number of nanoseconds represented by the string
     * @throws DateTimeParseException if the string cannot be parsed
     * @see #parseDuration(String)
     * @see #parseDurationQuiet(String)
     */
    @ScriptApi
    public static long parseDurationNanos(@NotNull String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new DateTimeParseException("Cannot parse time: " + s);
        }

        try {
            return parseDuration(s).toNanos();
        } catch (Exception e) {
            throw new DateTimeParseException("Cannot parse time: " + s, e);
        }
    }

    /**
     * Parses the string argument as a time duration in nanoseconds.
     * <p>
     * Time duration strings can be formatted as {@code [-]PT[-]hh:mm:[ss.nnnnnnnnn]} or as a duration string formatted
     * as {@code [-]PnDTnHnMn.nS}.
     *
     * @param s string to be converted
     * @return the number of nanoseconds represented by the string, or {@link QueryConstants#NULL_LONG} if the string
     *         cannot be parsed
     * @see #parseDuration(String)
     * @see #parseDurationQuiet(String)
     */
    @ScriptApi
    public static long parseDurationNanosQuiet(@Nullable String s) {
        if (s == null || s.length() <= 1) {
            return NULL_LONG;
        }

        try {
            return parseDurationNanos(s);
        } catch (Exception e) {
            return NULL_LONG;
        }
    }

    /**
     * Parses the string argument as a period, which is a unit of time in terms of calendar time (days, weeks, months,
     * years, etc.).
     * <p>
     * Period strings are formatted according to the ISO-8601 duration format as {@code PnYnMnD} and {@code PnW}, where
     * the coefficients can be positive or negative. Zero coefficients can be omitted. Optionally, the string can begin
     * with a negative sign.
     * <p>
     * Examples:
     *
     * <pre>
     *   "P2Y"             -- Period.ofYears(2)
     *   "P3M"             -- Period.ofMonths(3)
     *   "P4W"             -- Period.ofWeeks(4)
     *   "P5D"             -- Period.ofDays(5)
     *   "P1Y2M3D"         -- Period.of(1, 2, 3)
     *   "P1Y2M3W4D"       -- Period.of(1, 2, 25)
     *   "P-1Y2M"          -- Period.of(-1, 2, 0)
     *   "-P1Y2M"          -- Period.of(-1, -2, 0)
     * </pre>
     *
     * @param s period string
     * @return the period
     * @throws DateTimeParseException if the string cannot be parsed
     * @see Period#parse(CharSequence)
     */
    @ScriptApi
    @NotNull
    public static Period parsePeriod(@NotNull final String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new DateTimeParseException("Cannot parse period (null): " + s);
        }

        try {
            return Period.parse(s);
        } catch (Exception ex) {
            throw new DateTimeParseException("Cannot parse period: " + s, ex);
        }
    }

    /**
     * Parses the string argument as a period, which is a unit of time in terms of calendar time (days, weeks, months,
     * years, etc.).
     * <p>
     * Period strings are formatted according to the ISO-8601 duration format as {@code PnYnMnD} and {@code PnW}, where
     * the coefficients can be positive or negative. Zero coefficients can be omitted. Optionally, the string can begin
     * with a negative sign.
     * <p>
     * Examples:
     *
     * <pre>
     *   "P2Y"             -- Period.ofYears(2)
     *   "P3M"             -- Period.ofMonths(3)
     *   "P4W"             -- Period.ofWeeks(4)
     *   "P5D"             -- Period.ofDays(5)
     *   "P1Y2M3D"         -- Period.of(1, 2, 3)
     *   "P1Y2M3W4D"       -- Period.of(1, 2, 25)
     *   "P-1Y2M"          -- Period.of(-1, 2, 0)
     *   "-P1Y2M"          -- Period.of(-1, -2, 0)
     * </pre>
     *
     * @param s period string
     * @return the period, or {@code null} if the string can not be parsed
     * @see Period#parse(CharSequence)
     */
    @ScriptApi
    @Nullable
    public static Period parsePeriodQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return null;
        }

        try {
            return parsePeriod(s);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Parses the string argument as a duration, which is a unit of time in terms of clock time (24-hour days, hours,
     * minutes, seconds, and nanoseconds).
     * <p>
     * Duration strings are formatted according to the ISO-8601 duration format as {@code [-]PnDTnHnMn.nS}, where the
     * coefficients can be positive or negative. Zero coefficients can be omitted. Optionally, the string can begin with
     * a negative sign.
     * <p>
     * Examples:
     *
     * <pre>
     *    "PT20.345S" -- parses as "20.345 seconds"
     *    "PT15M"     -- parses as "15 minutes" (where a minute is 60 seconds)
     *    "PT10H"     -- parses as "10 hours" (where an hour is 3600 seconds)
     *    "P2D"       -- parses as "2 days" (where a day is 24 hours or 86400 seconds)
     *    "P2DT3H4M"  -- parses as "2 days, 3 hours and 4 minutes"
     *    "PT-6H3M"    -- parses as "-6 hours and +3 minutes"
     *    "-PT6H3M"    -- parses as "-6 hours and -3 minutes"
     *    "-PT-6H+3M"  -- parses as "+6 hours and -3 minutes"
     * </pre>
     *
     * @param s duration string
     * @return the duration
     * @throws DateTimeParseException if the string cannot be parsed
     * @see Duration#parse(CharSequence)
     */
    @ScriptApi
    @NotNull
    public static Duration parseDuration(@NotNull final String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new DateTimeParseException("Cannot parse duration (null): " + s);
        }

        try {

            final Matcher tdMatcher = TIME_DURATION_PATTERN.matcher(s);
            if (tdMatcher.matches()) {

                final String sign1Str = tdMatcher.group("sign1");
                final String sign2Str = tdMatcher.group("sign2");
                final String hourStr = tdMatcher.group("hour");
                final String minuteStr = tdMatcher.group("minute");
                final String secondStr = tdMatcher.group("second");
                final String nanosStr = tdMatcher.group("nanos");

                long sign1 = 0;

                if (sign1Str == null || sign1Str.isEmpty() || sign1Str.equals("+")) {
                    sign1 = 1;
                } else if (sign1Str.equals("-")) {
                    sign1 = -1;
                } else {
                    throw new RuntimeException("Unsupported sign: '" + sign1 + "'");
                }

                long sign2 = 0;

                if (sign2Str == null || sign2Str.isEmpty() || sign2Str.equals("+")) {
                    sign2 = 1;
                } else if (sign2Str.equals("-")) {
                    sign2 = -1;
                } else {
                    throw new RuntimeException("Unsupported sign: '" + sign2 + "'");
                }

                if (hourStr == null) {
                    throw new RuntimeException("Missing hour value");
                }

                long rst = Long.parseLong(hourStr) * HOUR;

                if (minuteStr == null) {
                    throw new RuntimeException("Missing minute value");
                }

                rst += Long.parseLong(minuteStr) * MINUTE;

                if (secondStr != null) {
                    rst += Long.parseLong(secondStr.substring(1)) * SECOND;
                }

                if (nanosStr != null) {
                    final String sn = nanosStr.substring(1) + "0".repeat(10 - nanosStr.length());
                    rst += Long.parseLong(sn);
                }

                return Duration.ofNanos(sign1 * sign2 * rst);
            }

            return Duration.parse(s);
        } catch (Exception ex) {
            throw new DateTimeParseException("Cannot parse duration: " + s, ex);
        }
    }

    /**
     * Parses the string argument as a duration, which is a unit of time in terms of clock time (24-hour days, hours,
     * minutes, seconds, and nanoseconds).
     * <p>
     * Duration strings are formatted according to the ISO-8601 duration format as {@code [-]PnDTnHnMn.nS}, where the
     * coefficients can be positive or negative. Zero coefficients can be omitted. Optionally, the string can begin with
     * a negative sign.
     * <p>
     * Examples:
     *
     * <pre>
     *    "PT20.345S" -- parses as "20.345 seconds"
     *    "PT15M"     -- parses as "15 minutes" (where a minute is 60 seconds)
     *    "PT10H"     -- parses as "10 hours" (where an hour is 3600 seconds)
     *    "P2D"       -- parses as "2 days" (where a day is 24 hours or 86400 seconds)
     *    "P2DT3H4M"  -- parses as "2 days, 3 hours and 4 minutes"
     *    "PT-6H3M"    -- parses as "-6 hours and +3 minutes"
     *    "-PT6H3M"    -- parses as "-6 hours and -3 minutes"
     *    "-PT-6H+3M"  -- parses as "+6 hours and -3 minutes"
     * </pre>
     *
     * @param s duration string
     * @return the duration, or {@code null} if the string can not be parsed
     * @see Duration#parse(CharSequence)
     */
    @ScriptApi
    @Nullable
    public static Duration parseDurationQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return null;
        }

        try {
            return parseDuration(s);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Parses the string argument as nanoseconds since the Epoch.
     * <p>
     * Date time strings are formatted according to the ISO 8601 date time format
     * {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others. Additionally, date time strings can be integer values that
     * are nanoseconds, milliseconds, or seconds from the Epoch. Expected date ranges are used to infer the units.
     *
     * @param s date time string
     * @return a long number of nanoseconds since the Epoch, matching the instant represented by the input string
     * @throws DateTimeParseException if the string cannot be parsed
     * @see #epochAutoToEpochNanos
     * @see #parseInstant(String)
     * @see DateTimeFormatter#ISO_INSTANT
     */
    @ScriptApi
    public static long parseEpochNanos(@NotNull final String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new DateTimeParseException("Cannot parse epoch nanos (null): " + s);
        }

        try {
            if (LONG_PATTERN.matcher(s).matches()) {
                return epochAutoToEpochNanos(Long.parseLong(s));
            }

            return epochNanos(parseZonedDateTime(s));
        } catch (Exception e) {
            throw new DateTimeParseException("Cannot parse epoch nanos: " + s, e);
        }
    }

    /**
     * Parses the string argument as a nanoseconds since the Epoch.
     * <p>
     * Date time strings are formatted according to the ISO 8601 date time format
     * {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others. Additionally, date time strings can be integer values that
     * are nanoseconds, milliseconds, or seconds from the Epoch. Expected date ranges are used to infer the units.
     *
     * @param s date time string
     * @return a long number of nanoseconds since the Epoch, matching the instant represented by the input string, or
     *         {@code null} if the string can not be parsed
     * @see DateTimeFormatter#ISO_INSTANT
     */
    @ScriptApi
    public static long parseEpochNanosQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return NULL_LONG;
        }

        try {
            return parseEpochNanos(s);
        } catch (Exception e) {
            return NULL_LONG;
        }
    }

    /**
     * Parses the string argument as an {@link Instant}.
     * <p>
     * Date time strings are formatted according to the ISO 8601 date time format
     * {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others. Additionally, date time strings can be integer values that
     * are nanoseconds, milliseconds, or seconds from the Epoch. Expected date ranges are used to infer the units.
     *
     * @param s date time string
     * @return an {@link Instant} represented by the input string
     * @throws DateTimeParseException if the string cannot be parsed
     * @see DateTimeFormatter#ISO_INSTANT
     */
    @ScriptApi
    @NotNull
    public static Instant parseInstant(@NotNull final String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new DateTimeParseException("Cannot parse instant (null): " + s);
        }

        try {
            if (LONG_PATTERN.matcher(s).matches()) {
                final long nanos = epochAutoToEpochNanos(Long.parseLong(s));
                // noinspection ConstantConditions
                return epochNanosToInstant(nanos);
            }

            return parseZonedDateTime(s).toInstant();
        } catch (Exception e) {
            throw new DateTimeParseException("Cannot parse instant: " + s, e);
        }
    }

    /**
     * Parses the string argument as an {@link Instant}.
     * <p>
     * Date time strings are formatted according to the ISO 8601 date time format
     * {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others. Additionally, date time strings can be integer values that
     * are nanoseconds, milliseconds, or seconds from the Epoch. Expected date ranges are used to infer the units.
     *
     * @param s date time string
     * @return an {@link Instant} represented by the input string, or {@code null} if the string can not be parsed
     * @see DateTimeFormatter#ISO_INSTANT
     */
    @ScriptApi
    @Nullable
    public static Instant parseInstantQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return null;
        }

        try {
            return parseInstant(s);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Parses the string argument as a {@link LocalDateTime}.
     * <p>
     * Date time strings are formatted according to the ISO 8601 date time format
     * {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS]} and others.
     *
     * @param s date time string
     * @return a {@link LocalDateTime} represented by the input string
     * @throws DateTimeParseException if the string cannot be parsed
     */
    @ScriptApi
    @NotNull
    public static LocalDateTime parseLocalDateTime(@NotNull final String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new DateTimeParseException("Cannot parse local date time (null): " + s);
        }

        try {
            return LocalDateTime.parse(s);
        } catch (java.time.format.DateTimeParseException e) {
            // ignore
        }

        try {
            final Matcher dtMatcher = LOCAL_DATE_PATTERN.matcher(s);
            if (dtMatcher.matches()) {
                final String dateString = dtMatcher.group("date");
                return LocalDate.parse(dateString, FORMATTER_ISO_LOCAL_DATE).atTime(LocalTime.of(0, 0));
            }
            return LocalDateTime.parse(s, FORMATTER_ISO_LOCAL_DATE_TIME);
        } catch (Exception ex) {
            throw new DateTimeParseException("Cannot parse local date time: " + s, ex);
        }
    }

    /**
     * Parses the string argument as a {@link LocalDateTime}.
     * <p>
     * Date time strings are formatted according to the ISO 8601 date time format
     * {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS]} and others.
     *
     * @param s date time string
     * @return a {@link LocalDateTime} represented by the input string, or {@code null} if the string can not be parsed
     */
    @ScriptApi
    @Nullable
    public static LocalDateTime parseLocalDateTimeQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return null;
        }

        try {
            return parseLocalDateTime(s);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Parses the string argument as a {@link ZonedDateTime}.
     * <p>
     * Date time strings are formatted according to the ISO 8601 date time format
     * {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others.
     *
     * @param s date time string
     * @return a {@link ZonedDateTime} represented by the input string
     * @throws DateTimeParseException if the string cannot be parsed
     * @see DateTimeFormatter#ISO_INSTANT
     */
    @ScriptApi
    @NotNull
    public static ZonedDateTime parseZonedDateTime(@NotNull final String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new DateTimeParseException("Cannot parse datetime (null): " + s);
        }

        try {
            return ZonedDateTime.parse(s);
        } catch (java.time.format.DateTimeParseException e) {
            // ignore
        }

        try {
            final Matcher dtzMatcher = DATE_TZ_PATTERN.matcher(s);
            if (dtzMatcher.matches()) {
                final String dateString = dtzMatcher.group("date");
                final String timeZoneString = dtzMatcher.group("timezone");
                final ZoneId timeZone = parseTimeZoneQuiet(timeZoneString);

                if (timeZone == null) {
                    throw new RuntimeException("No matching time zone: '" + timeZoneString + "'");
                }

                return LocalDate.parse(dateString, FORMATTER_ISO_LOCAL_DATE).atTime(LocalTime.of(0, 0))
                        .atZone(timeZone);
            }

            int spaceIndex = s.indexOf(' ');
            if (spaceIndex == -1) {
                throw new RuntimeException("No time zone provided");
            }

            final String dateTimeString = s.substring(0, spaceIndex);
            final String timeZoneString = s.substring(spaceIndex + 1);
            final ZoneId timeZone = parseTimeZoneQuiet(timeZoneString);

            if (timeZone == null) {
                throw new RuntimeException("No matching time zone: " + timeZoneString);
            }

            return LocalDateTime.parse(dateTimeString, FORMATTER_ISO_LOCAL_DATE_TIME).atZone(timeZone);
        } catch (Exception ex) {
            throw new DateTimeParseException("Cannot parse zoned date time: " + s, ex);
        }
    }

    /**
     * Parses the string argument as a {@link ZonedDateTime}.
     * <p>
     * Date time strings are formatted according to the ISO 8601 date time format
     * {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others.
     *
     * @param s date time string
     * @return a {@link ZonedDateTime} represented by the input string, or {@code null} if the string can not be parsed
     * @see DateTimeFormatter#ISO_INSTANT
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime parseZonedDateTimeQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return null;
        }

        try {
            return parseZonedDateTime(s);
        } catch (Exception e) {
            return null;
        }
    }

    private enum DateGroupId {
        // Date(1),
        Year(2, ChronoField.YEAR), Month(3, ChronoField.MONTH_OF_YEAR), Day(4, ChronoField.DAY_OF_MONTH),
        // Tod(5),
        Hours(6, ChronoField.HOUR_OF_DAY), Minutes(7, ChronoField.MINUTE_OF_HOUR), Seconds(8,
                ChronoField.SECOND_OF_MINUTE), Fraction(9, ChronoField.MILLI_OF_SECOND);
        // TODO MICRO and NANOs are not supported! -- fix and unit test!

        final int id;
        final ChronoField field;

        DateGroupId(int id, @NotNull ChronoField field) {
            this.id = id;
            this.field = field;
        }
    }

    /**
     * Returns a {@link ChronoField} indicating the level of precision in a time, datetime, or period nanos string.
     *
     * @param s time string
     * @return {@link ChronoField} for the finest units in the string (e.g. "10:00:00" would yield SecondOfMinute)
     * @throws RuntimeException if the string cannot be parsed
     */
    @ScriptApi
    @NotNull
    public static ChronoField parseTimePrecision(@NotNull final String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new DateTimeParseException("Cannot parse time precision (null): " + s);
        }

        try {
            Matcher dtMatcher = CAPTURING_DATETIME_PATTERN.matcher(s);
            if (dtMatcher.matches()) {
                DateGroupId[] parts = DateGroupId.values();
                for (int i = parts.length - 1; i >= 0; i--) {
                    String part = dtMatcher.group(parts[i].id);
                    if (part != null && !part.isEmpty()) {
                        return parts[i].field;
                    }
                }
            }

            if (TIME_DURATION_PATTERN.matcher(s).matches()) {
                return parseTimePrecision(s.replace("PT", ""));
            }

            throw new RuntimeException("Time precision does not match expected pattern");
        } catch (Exception ex) {
            throw new DateTimeParseException("Cannot parse time precision: " + s, ex);
        }
    }

    /**
     * Returns a {@link ChronoField} indicating the level of precision in a time or datetime string.
     *
     * @param s time string
     * @return {@code null} if the time string cannot be parsed; otherwise, a {@link ChronoField} for the finest units
     *         in the string (e.g. "10:00:00" would yield SecondOfMinute)
     * @throws RuntimeException if the string cannot be parsed
     */
    @ScriptApi
    @Nullable
    public static ChronoField parseTimePrecisionQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return null;
        }

        try {
            return parseTimePrecision(s);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Parses the string argument as a local date, which is a date without a time or time zone.
     * <p>
     * Date strings are formatted according to the ISO 8601 date time format as {@code YYYY-MM-DD}.
     *
     * @param s date string
     * @return local date parsed according to the default date style
     * @throws DateTimeParseException if the string cannot be parsed
     * @see DateTimeFormatter#ISO_LOCAL_DATE
     */
    @ScriptApi
    @NotNull
    public static LocalDate parseLocalDate(@NotNull final String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new DateTimeParseException("Cannot parse datetime (null): " + s);
        }

        try {
            return LocalDate.parse(s, FORMATTER_ISO_LOCAL_DATE);
        } catch (java.time.format.DateTimeParseException e) {
            throw new DateTimeParseException("Cannot parse local date: " + s, e);
        }
    }

    /**
     * Parses the string argument as a local date, which is a date without a time or time zone.
     * <p>
     * Date strings are formatted according to the ISO 8601 date time format as {@code YYYY-MM-DD}.
     *
     * @param s date string
     * @return local date parsed according to the default date style, or {@code null} if the string can not be parsed
     * @see DateTimeFormatter#ISO_LOCAL_DATE
     */
    @ScriptApi
    @Nullable
    public static LocalDate parseLocalDateQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return null;
        }

        try {
            return parseLocalDate(s);
        } catch (Exception e) {
            return null;
        }
    }


    /**
     * Parses the string argument as a local time, which is the time that would be read from a clock and does not have a
     * date or timezone.
     * <p>
     * Local time strings can be formatted as {@code hh:mm:ss[.nnnnnnnnn]}.
     *
     * @param s string to be converted
     * @return a {@link LocalTime} represented by the input string.
     * @throws DateTimeParseException if the string cannot be converted, otherwise a {@link LocalTime} from the parsed
     *         string
     */
    @ScriptApi
    @NotNull
    public static LocalTime parseLocalTime(@NotNull final String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new DateTimeParseException("Cannot parse local time (null): " + s);
        }

        try {
            return LocalTime.parse(s, FORMATTER_ISO_LOCAL_TIME);
        } catch (java.time.format.DateTimeParseException e) {
            throw new DateTimeParseException("Cannot parse local time: " + s, e);
        }
    }

    /**
     * Parses the string argument as a local time, which is the time that would be read from a clock and does not have a
     * date or timezone.
     * <p>
     * Local time strings can be formatted as {@code hh:mm:ss[.nnnnnnnnn]}.
     *
     * @param s string to be converted
     * @return a {@link LocalTime} represented by the input string, or {@code null} if the string can not be parsed
     */
    @ScriptApi
    @Nullable
    public static LocalTime parseLocalTimeQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return null;
        }

        try {
            return parseLocalTime(s);
        } catch (Exception e) {
            return null;
        }
    }

    // endregion

}
