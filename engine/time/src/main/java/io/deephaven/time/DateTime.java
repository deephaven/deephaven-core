/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time;

import io.deephaven.base.clock.Clock;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.ReflexiveUse;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.time.*;
import java.util.Date;

//import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

/**
 * A point on the timeline.
 *
 * The time represents a zone-less, precise timepoint without respect to timezones.
 * It is stored as a signed 64-bit long, representing nanoseconds since the epoch (January 1, 1970, 00:00:00 GMT).
 * This provides a range from 1677-09-21T00:12:43.146-775807 UTC to 2262-04-11T23:47:16.854775807 UTC.
 * The minimum long value is reserved for representing null values and therefore is not permitted as a valid time.
 */
@TypeUtils.IsDateTime
@ReflexiveUse(referrers = "io.deephaven.gui.table.filters.StringFilterData")
public final class DateTime implements Comparable<DateTime>, Externalizable {
    //TODO: remove deprecated
    //TODO: document
    //TODO: remove Joda exposure
    //TODO: curate API
    //TODO: review public vs private
    //TODO: test coverage

    private static final long serialVersionUID = -9077991715632523353L;

    private long nanos;

    //TODO: group constructors
    
    //TODO: Remove -- redundant?
    /**
     * Create a new date time from an {@link Instant}.
     *
     * @param instant instant in time.
     * @return the date time.
     */
    public static DateTime of(Instant instant) {
        return new DateTime(DateTimeUtils.epochNanos(instant));
    }

    //TODO: Remove -- redundant?
    /**
     * Create a new date time via {@link Clock#currentTimeNanos()}.
     *
     * <p>
     * Equivalent to {@code new DateTime(clock.currentTimeNanos())}.
     *
     * <p>
     * If nanosecond resolution is not necessary, consider using {@link #ofMillis(Clock)}.
     *
     * @param clock the clock
     * @return the date time
     */
    public static DateTime of(Clock clock) {
        return new DateTime(clock.currentTimeNanos());
    }

    //TODO: Remove -- redundant?
    /**
     * Create a new date time via {@link Clock#currentTimeMillis()}.
     *
     * <p>
     * Equivalent to {@code new DateTime(Math.multiplyExact(clock.currentTimeMillis(), 1_000_000))}.
     *
     * @param clock the clock
     * @return the date time
     */
    public static DateTime ofMillis(Clock clock) {
        return new DateTime(Math.multiplyExact(clock.currentTimeMillis(), 1_000_000));
    }

    //TODO: ofMicros
    //TODO: ofNanos

    //TODO: Remove -- redundant?
    /**
     * Create a new DateTime initialized to the current system time. Based on {@link Clock#system()}. Equivalent to
     * {@code of(Clock.system())}.
     *
     * <p>
     * The precision of DateTime is nanoseconds, but the resolution of the this method depends on the JVM.
     *
     * <p>
     * If you don't need nanosecond resolution, it may be preferable to use {@link #nowMillis()}.
     *
     * <p>
     * Note: overflow checking is not performed - this method will overflow in the year 2262.
     *
     * @return a new DateTime initialized to the current time.
     */
    public static DateTime now() {
        return of(Clock.system());
    }

    //TODO: Remove -- redundant?
    /**
     * Create a new DateTime initialized to the current system time. Based on {@link Clock#system()}. Equivalent to
     * {@code ofMillis(Clock.system())}.
     *
     * <p>
     * The resolution will be in milliseconds.
     *
     * @return a new DateTime initialized to the current time.
     */
    public static DateTime nowMillis() {
        return ofMillis(Clock.system());
    }

    /**
     * Create a new DateTime initialized to the epoch.
     */
    public DateTime() {
        // for Externalizable
    }

    /**
     * Create a new DateTime initialized to the provided nanoseconds since the epoch.
     *
     * @param nanos the number of nanoseconds since the epoch
     */
    public DateTime(long nanos) {
        this.nanos = nanos;
    }

    // region Object hashing / comparison

    @Override
    public boolean equals(final Object that) {
        if (this == that) {
            return true;
        }
        if (that == null) {
            return false;
        }
        if (!(that instanceof DateTime)) {
            return false;
        }
        return nanos == ((DateTime) that).nanos;
    }

    public int hashCode() {
        return (int) (nanos ^ (nanos >>> 32));
    }

    public int compareTo(DateTime dateTime) {
        return Long.compare(nanos, dateTime.nanos);
    }

    // endregion

    // region Epoch accessors

    /**
     * Get this time represented as nanoseconds since the epoch
     *
     * @return the number of nanoseconds since the epoch
     */
    public long getNanos() {
        return nanos;
    }

    /**
     * Get this time represented as microseconds since the epoch
     *
     * @return the number of microseconds since the epoch
     */
    public long getMicros() {
        return nanos / 1000;
    }

    /**
     * Get this time represented as milliseconds since the epoch
     *
     * @return the number of milliseconds since the epoch
     */
    public long getMillis() {
        return nanos / 1000000;
    }

    /**
     * Get the number of nanoseconds since the top of the millisecond.
     * This number will always be between 0 and 999,999
     *
     * @return the number of nanoseconds since the top of the millisecond.
     */
    public long getNanosPartial() {
        return nanos % 1000000;
    }

    // endregion

    //TODO: fix regions

    // region Mutations to other DateTime types

    // region Joda DateTime flavors
    /**
     * Convert this DateTime to a Joda DateTime.
     *
     * This DateTime will be truncated to milliseconds.
     *
     * @return a Joda DateTime representing this DateTime
     * @deprecated use {@link #toZonedDateTime(ZoneId)} instead
     */
    @Deprecated
    public org.joda.time.DateTime getJodaDateTime() {
        return new org.joda.time.DateTime(getMillis());
    }

    /**
     * Convert this DateTime to a Joda DateTime.
     *
     * This DateTime will be truncated to milliseconds.
     *
     * @param timeZone the timezone for the created Joda DateTime
     *
     * @return a Joda DateTime representing this DateTime
     * @deprecated use {@link #toZonedDateTime(ZoneId)} instead
     */
    @Deprecated
    public org.joda.time.DateTime getJodaDateTime(TimeZone timeZone) {
        return new org.joda.time.DateTime(getMillis(), timeZone.getTimeZone());
    }

    // endregion

    // region Java DateTime flavors

    /**
     * Get a {@link ZonedDateTime} version of this {@link DateTime} at the {@link ZoneId#systemDefault() system default}
     * time zone.
     *
     * @return a {@link ZonedDateTime}
     */
    @NotNull
    public ZonedDateTime toZonedDateTime() {
        return toZonedDateTime(ZoneId.systemDefault());
    }

    /**
     * Get a {@link ZonedDateTime} version of this {@link DateTime} at the specified time zone.
     *
     * @return a {@link ZonedDateTime}
     */
    @NotNull
    public ZonedDateTime toZonedDateTime(@NotNull final String zone) {
        return toZonedDateTime(ZoneId.of(zone));
    }

    /**
     * Get a {@link ZonedDateTime} version of this {@link DateTime} at the specified time zone.
     *
     * @return a {@link ZonedDateTime}
     */
    @NotNull
    public ZonedDateTime toZonedDateTime(@NotNull final TimeZone zone) {
        return toZonedDateTime(zone.getZoneId());
    }

    /**
     * Get a {@link ZonedDateTime} version of this {@link DateTime} at the specified time zone.
     *
     * @return a {@link ZonedDateTime}
     */
    @NotNull
    public ZonedDateTime toZonedDateTime(@NotNull final ZoneId zone) {
        return ZonedDateTime.ofInstant(getInstant(), zone);
    }

    /**
     * Get a {@link LocalDate} representing the date of this {@link DateTime} at the {@link ZoneId#systemDefault()
     * system default} time zone.
     *
     * @return the {@link LocalDate}
     */
    @NotNull
    public LocalDate toLocalDate() {
        return toLocalDate(ZoneId.systemDefault());
    }

    /**
     * Get a {@link LocalDate} representing the date of this {@link DateTime} at the specified time zone.
     *
     * @return the {@link LocalDate}
     */
    @NotNull
    public LocalDate toLocalDate(@NotNull final String zone) {
        return toLocalDate(ZoneId.of(zone));
    }

    /**
     * Get a {@link LocalDate} representing the date of this {@link DateTime} at the specified time zone.
     *
     * @return the {@link LocalDate}
     */
    @NotNull
    public LocalDate toLocalDate(@NotNull final TimeZone zone) {
        return toLocalDate(zone.getZoneId());
    }

    /**
     * Get a {@link LocalDate} representing the date of this {@link DateTime} at the specified time zone.
     *
     * @return the {@link LocalDate}
     */
    @NotNull
    public LocalDate toLocalDate(@NotNull final ZoneId zone) {
        return toZonedDateTime(zone).toLocalDate();
    }

    /**
     * Get a {@link LocalTime} representing the time of day of this {@link DateTime} at the
     * {@link ZoneId#systemDefault() system default} time zone.
     *
     * @return the {@link LocalTime}
     */
    @NotNull
    public LocalTime toLocalTime() {
        return toLocalTime(ZoneId.systemDefault());
    }

    /**
     * Get a {@link LocalTime} representing the time of day of this {@link DateTime} at the specified time zone.
     *
     * @return the {@link LocalTime}
     */
    @NotNull
    public LocalTime toLocalTime(@NotNull final String zone) {
        return toLocalTime(ZoneId.of(zone));
    }

    /**
     * Get a {@link LocalTime} representing the time of day of this {@link DateTime} at the specified time zone.
     *
     * @return the {@link LocalTime}
     */
    @NotNull
    public LocalTime toLocalTime(@NotNull final TimeZone zone) {
        return toLocalTime(zone.getZoneId());
    }

    /**
     * Get a {@link LocalTime} representing the time of day of this {@link DateTime} at the specified time zone.
     *
     * @return the {@link LocalTime}
     */
    @NotNull
    public LocalTime toLocalTime(@NotNull final ZoneId zone) {
        return toZonedDateTime(zone).toLocalTime();
    }

    /**
     * Convert this DateTime to a Java Date.
     *
     * This DateTime will be truncated to milliseconds.
     *
     * @return a Java Date representing this DateTime
     * @deprecated use {@link #toZonedDateTime()} instead.
     */
    @Deprecated
    @NotNull
    public Date getDate() {
        return new Date(getMillis());
    }

    /**
     * Convert this DateTime to a Java Instant.
     *
     * @return a Java Instant representing this DateTime
     */
    @NotNull
    public Instant getInstant() {
        return Instant.ofEpochSecond(0, nanos);
    }

    // endregion

    // endregion


    // region String formatting

    @Override
    public String toString() {
        return toString(TimeZone.TZ_DEFAULT);
    }

    /**
     * Converts this DateTime into a String using the provided {@link TimeZone}.
     *
     * The date time will be formatted as {@code yyyy-MM-DDThh:mm:ss.SSSSSSSSS TZ}, for example
     * {@code 2020-05-27T13:37:57.780853000 NY} or {@code 2020-05-27T17:37:42.763641000 UTC}.
     *
     * @param timeZone the timezone for formatting the string
     * @return a string representation of this DateTime
     * @see DateTimeUtils#formatDateTime(DateTime, TimeZone)
     */
    @NotNull
    public String toString(@NotNull final TimeZone timeZone) {
        // noinspection ConstantConditions
        if (timeZone == null) {
            throw new IllegalArgumentException("timeZone cannot be null");
        }

        return DateTimeUtils.formatDateTime(this, timeZone);
    }

    /**
     * Converts this DateTime into a date String using the default {@link TimeZone}.
     *
     * The date will be formatted as {@code yyyy-MM-DD}, for example {@code 2020-05-27} or {@code 2020-05-27}.
     *
     * @return a date string representation of this DateTime in the default {@link TimeZone}.
     * @see DateTimeUtils#formatDate(DateTime, TimeZone)
     */
    @NotNull
    public String toDateString() {
        return toDateString(TimeZone.TZ_DEFAULT);
    }

    /**
     * Converts this DateTime into a date String using the provided {@link TimeZone}.
     *
     * The date will be formatted as {@code yyyy-MM-DD}, for example {@code 2020-05-27} or {@code 2020-05-27}.
     *
     * @param timeZone the timezone for formatting the string
     * @return a date string representation of this DateTime in the provided {@link TimeZone}.
     * @see DateTimeUtils#formatDate(DateTime, TimeZone)
     */
    @NotNull
    public String toDateString(@NotNull final TimeZone timeZone) {
        // noinspection ConstantConditions
        if (timeZone == null) {
            throw new IllegalArgumentException("timeZone cannot be null");
        }
        //noinspection ConstantConditions
        return DateTimeUtils.formatDate(this, timeZone);
    }

    /**
     * Converts this DateTime into a date String using the provided {@link ZoneId}.
     *
     * The date will be formatted as {@code yyyy-MM-DD}, for example {@code 2020-05-27} or {@code 2020-05-27}.
     *
     * @param timeZone the timezone for formatting the string.  The timezone string must be a valid value specified in {@link ZoneId}.
     * @return a date string representation of this DateTime in the provided {@link ZoneId}.
     * @see DateTimeUtils#formatDate(DateTime, TimeZone)
     * @see ZoneId
     */
    @NotNull
    public String toDateString(@NotNull final String timeZone) {
        return toDateString(ZoneId.of(timeZone));
    }

    /**
     * Converts this DateTime into a date String using the provided {@link ZoneId}.
     *
     * The date will be formatted as {@code yyyy-MM-DD}, for example {@code 2020-05-27} or {@code 2020-05-27}.
     *
     * @param timeZone the timezone for formatting the string
     * @return a date string representation of this DateTime in the provided {@link ZoneId}.
     * @see DateTimeUtils#formatDate(DateTime, TimeZone)
     */
    @NotNull
    public String toDateString(@NotNull final ZoneId timeZone) {
        // noinspection ConstantConditions
        if (timeZone == null) {
            throw new IllegalArgumentException("timeZone cannot be null");
        }
        return ISO_LOCAL_DATE.format(ZonedDateTime.ofInstant(getInstant(), timeZone));
    }

    // endregion

    // region Externalizable

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(nanos);
    }

    public void readExternal(ObjectInput in) throws IOException {
        nanos = in.readLong();
    }

    // endregion
}
