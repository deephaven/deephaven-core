/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.clock;

public class TimeConstants {
    // this constant is a stop gap that lets us detect if a timestamp is in millis or micros by looking at the
    // magnitude,
    // anything greater than this is assumed to be micros. This is the year 2265 (http://en.memory-alpha.org/wiki/2265)
    public static final long MICROTIME_THRESHOLD = 9309341000000L;
    public final static long SECOND = 1000;
    public final static long MINUTE = 60 * SECOND;
    public final static long HOUR = 60 * MINUTE;
    public final static long DAY = 24 * HOUR;
    public final static long YEAR = 365 * DAY;
    public final static long WEEK = 7 * DAY;

    public final static long SECOND_IN_NANOS = 1_000_000_000;
}
