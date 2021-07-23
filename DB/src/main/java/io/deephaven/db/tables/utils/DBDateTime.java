/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.base.StringUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.clock.MicroTimer;
import io.deephaven.util.annotations.ReflexiveUse;
import io.deephaven.util.type.TypeUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

/**
 * An object representing a timepoint in Deephaven.
 *
 * <p>The DBDateTime object is a timepoint, that is a precise instance in time without respect to timezones.  The
 * timepoint is stored as a signed 64-bit long nanoseconds since the epoch (January 1, 1970, 00:00:00 GMT).  This
 * provides a range from 1677-09-21T00:12:43.146-775807 UTC to 2262-04-11T23:47:16.854775807 UTC.  The minimum long
 * value is reserved for {@link QueryConstants#NULL_LONG} and therefore is not permitted as a valid DBDateTime.</p>
 */
@TypeUtils.IsDateTime
@ReflexiveUse(referrers = "io.deephaven.gui.table.filters.StringFilterData")
public final class DBDateTime implements Comparable<DBDateTime>, Externalizable {

    private static final long serialVersionUID = -9077991715632523353L;

    private long nanos;

    private static final DateTimeFormatter dateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private static final DateTimeFormatter dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd");

    public static DBDateTime of(Instant instant) {
        return new DBDateTime(DBTimeUtils.nanos(instant));
    }

    /**
     * Create a new DBDatetime initialized to the epoch.
     */
    public DBDateTime(){
        //for Externalizable
    }

    /**
     * Create a new DBDatetime initialized to the current time.
     *
     * <p>The precision of DBDateTime is nanoseconds, but the resolution of the now method is currently microseconds.</p>
     *
     * @return a new DBDateTime initialized to the current time.
     */
    public static DBDateTime now() {
        return new DBDateTime(MicroTimer.currentTimeMicros() * 1000);
    }

    /**
     * Create a new DBDateTime initialized to the provided nanoseconds since the epoch.
     *
     * @param nanos the number of nanoseconds since the epoch
     */
    public DBDateTime(long nanos) {
        this.nanos = nanos;
    }

    /**
     * Get this time represented as nanoseconds since the epoch
     *
     * @return the number of nanoseconds since the epoch
     */
    public long getNanos(){
        return nanos;
    }

    /**
     * Get this time represented as microseconds since the epoch

     * @return the number of microseconds since the epoch
     */
    public long getMicros(){
        return nanos/1000;
    }

    /**
     * Get this time represented as milliseconds since the epoch

     * @return the number of milliseconds since the epoch
     */
    public long getMillis(){
        return nanos/1000000;
    }

    /**
     * Get nanoseconds-of-milliseconds; this number will always be between 0 and 999,999
     *
     * @return the number of nanoseconds after the nearest millisecond.
     */
    public long getNanosPartial(){
        return nanos%1000000;
    }

    /**
     * Convert this DBDateTime to a Java Date.
     *
     * This DBDateTime will be truncated to milliseconds.
     *
     * @return a Java Date representing this DBDateTime
     */
    public Date getDate(){
        return new Date(getMillis());
    }

    /**
     * Convert this DBDateTime to a Joda DateTime.
     *
     * This DBDateTime will be truncated to milliseconds.
     *
     * @return a Joda DateTime representing this DBDateTime
     */
    public DateTime getJodaDateTime(){
        return new DateTime(getMillis());
    }

    /**
     * Convert this DBDateTime to a Joda DateTime.
     *
     * This DBDateTime will be truncated to milliseconds.
     *
     * @param timeZone the timezone for the created Joda DateTime
     *
     * @return a Joda DateTime representing this DBDateTime
     */
    public DateTime getJodaDateTime(DBTimeZone timeZone){
        return new DateTime(getMillis(), timeZone.getTimeZone());
    }

    /**
     * Convert this DBDateTime to a Java Instant.
     *
     * @return a Java Instant representing this DBDateTime
     */
    public Instant getInstant() {
        long epochSecond = nanos/1000000000;
        long nanoAdjustment = nanos%1000000000;
        return Instant.ofEpochSecond(epochSecond, nanoAdjustment);
    }

    @Override
    public boolean equals(final Object that) {
        if (this == that) {
            return true;
        }
        if (that == null) {
            return false;
        }
        if (!(that instanceof DBDateTime)) {
            return false;
        }
        return nanos == ((DBDateTime)that).nanos;
    }

    public int hashCode() {
        return (int)(nanos ^ (nanos >>> 32));
    }

    public int compareTo(DBDateTime dateTime) {
        return (nanos<dateTime.nanos ? -1 : (nanos==dateTime.nanos ? 0 : 1));
    }

    @Override
    public String toString() {
        return toString(DBTimeZone.TZ_DEFAULT);
    }

    /**
     * Convert this DBDateTime into a String using the provided {@link DBTimeZone}.
     *
     * <p>The date will be formatted as {@code yyyy-MM-DDThh:mm:ss.SSSSSSSSS TZ}, for example
     * {@code 2020-05-27T13:37:57.780853000 NY} or {@code 2020-05-27T17:37:42.763641000 UTC}.</p>
     *
     * @param timeZone the timezone for formatting the string
     * @return a String representation of this DBDateTime
     */
    public String toString(DBTimeZone timeZone) {
        return dateTimeFormat.withZone(timeZone.getTimeZone()).print(getMillis()) + StringUtils.pad(String.valueOf(getNanosPartial()), 6, '0') + " " + timeZone.toString().substring(3);
    }

    /**
     * Get the date represented by this DBDateTime in the default {@link DBTimeZone}.
     *
     * @return The date (yyyy-MM-dd) represented by this {@code DBDateTime} in the default {@link DBTimeZone}.
     */
    public String toDateString() {
        return toDateString(DBTimeZone.TZ_DEFAULT);
    }

    /**
     * Get the date represented by this DBDateTime in the given {@link DBTimeZone}.
     *
     * @param timeZone a DBTimeZone
     * @return The date (yyyy-MM-dd) represented by this {@code DBDateTime} in the given timeZone.
     */
    public String toDateString(DBTimeZone timeZone) {
        return dateFormat.withZone(timeZone.getTimeZone()).print(getMillis());
    }

    /**
     * Get the date represented by this DBDateTime in the given joda {@code DateTimeZone}.
     *
     * @param timeZone A joda DateTimeZone
     * @return The date (yyyy-MM-dd) represented by this {@code DBDateTime} in the given {@code timeZone}
     */
    public String toDateString(DateTimeZone timeZone) {
        if(timeZone == null) {
            throw new IllegalArgumentException("timeZone cannot be null");
        }
        return dateFormat.withZone(timeZone).print(getMillis());
    }

    /**
     * Get the date represented by this DBDateTime in the time zone specified by {@code zoneId}
     *
     * @param zoneId A java time zone ID string
     * @return The date (yyyy-MM-dd) represented by this {@code DBDateTime} in time zone represented by the given {@code zoneId}
     */
    public String toDateString(String zoneId) {
        return toDateString(ZoneId.of(zoneId));
    }

    /**
     * Get the date represented by this DBDateTime in the given java {@code ZoneId}.
     *
     * @param timeZone A java {@link ZoneId time zone ID}.
     * @return The date (yyyy-MM-dd) represented by this {@code DBDateTime} in the given {@code timeZone}
     */
    public String toDateString(ZoneId timeZone) {
        if(timeZone == null) {
            throw new IllegalArgumentException("timeZone cannot be null");
        }
        return ISO_LOCAL_DATE.format(ZonedDateTime.ofInstant(getInstant(), timeZone));
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(nanos);
    }

    public void readExternal(ObjectInput in) throws IOException {
        nanos=in.readLong();
    }
}
