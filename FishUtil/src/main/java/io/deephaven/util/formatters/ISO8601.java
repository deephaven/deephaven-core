/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.formatters;

import io.deephaven.configuration.Configuration;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class ISO8601 {
    private static final ThreadLocal<DateFormat> toISO8601Cache = new ThreadLocal<DateFormat>();
    private static final ThreadLocal<DateFormat> timeISO8601Cache = new ThreadLocal<DateFormat>();
    private static final ThreadLocal<DateFormat> dateISO8601Cache = new ThreadLocal<DateFormat>();
    private static TimeZone TZ_SERVER = null;

    public static synchronized TimeZone serverTimeZone() {
        if (TZ_SERVER == null) {
            TZ_SERVER = Configuration.getInstance().getServerTimezone();
        }
        return TZ_SERVER;
    }

    public static DateFormat ISO8601DateFormat(TimeZone tz) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        df.setTimeZone(tz);
        return df;
    }

    public static DateFormat ISE8601TimeFormat() {
        return ISO8601TimeFormat(serverTimeZone());
    }

    public static DateFormat ISO8601TimeFormat(TimeZone tz) {
        SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss.SSSZ");
        df.setTimeZone(tz);
        return df;
    }

    public static DateFormat ISE8601DateTimeFormat() {
        return ISO8601DateTimeFormat(serverTimeZone());
    }

    public static DateFormat ISO8601DateTimeFormat(TimeZone tz) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        df.setTimeZone(tz);
        return df;
    }

    public static String toISO8601(long millis) {
        return toISO8601(new Date(millis), serverTimeZone());
    }

    public static String toISO8601(Date d) {
        return toISO8601(d, serverTimeZone());
    }

    public static String toISO8601(long millis, TimeZone tz) {
        return toISO8601(new Date(millis), tz);
    }

    public static String toISO8601(Date d, TimeZone tz) {
        DateFormat df = toISO8601Cache.get();
        if (df == null) {
            df = ISO8601DateTimeFormat(tz);
            toISO8601Cache.set(df);
        } else {
            df.setTimeZone(tz);
        }
        return df.format(d);
    }

    public static String timeISO8601(long millis) {
        return timeISO8601(new Date(millis), serverTimeZone());
    }

    public static String timeISO8601(Date d) {
        return timeISO8601(d, serverTimeZone());
    }

    public static String timeISO8601(long millis, TimeZone tz) {
        return timeISO8601(new Date(millis), tz);
    }

    public static String timeISO8601(Date d, TimeZone tz) {
        DateFormat df = timeISO8601Cache.get();
        if (df == null) {
            df = ISO8601TimeFormat(tz);
            timeISO8601Cache.set(df);
        } else {
            df.setTimeZone(tz);
        }
        return df.format(d);
    }

    public static String dateISO8601(long millis) {
        return dateISO8601(new Date(millis), serverTimeZone());
    }

    public static String dateISO8601(Date d) {
        return dateISO8601(d, serverTimeZone());
    }

    public static String dateISO8601(long millis, TimeZone tz) {
        return dateISO8601(new Date(millis), tz);
    }

    public static String dateISO8601(Date d, TimeZone tz) {
        DateFormat df = dateISO8601Cache.get();
        if (df == null) {
            df = ISO8601DateFormat(tz);
            dateISO8601Cache.set(df);
        } else {
            df.setTimeZone(tz);
        }
        return df.format(d);
    }
}
