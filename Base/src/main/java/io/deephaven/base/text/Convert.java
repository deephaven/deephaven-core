/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.text;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class Convert {

    private static final byte[] MIN_SHORT_BYTES = Integer.toString(Short.MIN_VALUE).getBytes();
    private static final byte[] MIN_INT_BYTES = Integer.toString(Integer.MIN_VALUE).getBytes();
    private static final byte[] MIN_LONG_BYTES = Long.toString(Long.MIN_VALUE).getBytes();

    /** the maximum number of bytes in the ASCII decimal representation of an integer */
    public static int MAX_SHORT_BYTES = 6;

    /** the maximum number of bytes in the ASCII decimal representation of an integer */
    public static int MAX_INT_BYTES = 11;

    /** the maximum number of bytes in the ASCII decimal representation of a long */
    public static int MAX_LONG_BYTES = 20;

    /**
     * the maximum number of bytes in the ASCII decimal representation of a double: 17 digits, decimal point, sign, 'E',
     * exponent
     */
    public static int MAX_DOUBLE_BYTES = 24;

    /** the exact number of bytes in an ISO8601 millis timestamp: YYYY-MM-DDTHH:MM:SS.MMM<suffix> */
    public static final int MAX_ISO8601_BYTES = 23 + 5;

    /** the exact number of bytes in an ISO8601 micros timestamp: YYYY-MM-DDTHH:MM:SS.MMM<suffix> */
    public static final int MAX_ISO8601_MICROS_BYTES = 26 + 5;

    public static final int ISO8601_SECOND_OFFSET = 17;
    public static final int ISO8601_MILLIS_OFFSET = 20;
    public static final int ISO8601_MICROS_OFFSET = 23;

    /**
     * Append a decimal representation of a short to a byte buffer.
     * 
     * @param n the integer to be converted
     * @param b the byte buffer
     * @return the byte buffer
     * @throws java.nio.BufferOverflowException if there is not enough space in the buffer
     */
    public static ByteBuffer appendShort(short n, ByteBuffer b) {
        if (n < 0) {
            if (n == Short.MIN_VALUE) {
                b.put(MIN_SHORT_BYTES);
                return b;
            }
            b.put((byte) '-');
            n = (short) -n;
        }
        int q = b.position();
        do {
            b.put((byte) (n % 10 + '0'));
        } while ((n /= 10) > 0);
        int p = b.position() - 1;
        while (q < p) {
            byte tmp = b.get(p);
            b.put(p--, b.get(q));
            b.put(q++, tmp);
        }
        return b;
    }

    /**
     * Append a decimal representation of an integer to a byte buffer.
     * 
     * @param n the integer to be converted
     * @param b the byte buffer
     * @return the byte buffer
     * @throws java.nio.BufferOverflowException if there is not enough space in the buffer
     */
    public static ByteBuffer appendInt(int n, ByteBuffer b) {
        if (n < 0) {
            if (n == Integer.MIN_VALUE) {
                b.put(MIN_INT_BYTES);
                return b;
            }
            b.put((byte) '-');
            n = -n;
        }
        int q = b.position();
        do {
            b.put((byte) (n % 10 + '0'));
        } while ((n /= 10) > 0);
        int p = b.position() - 1;
        while (q < p) {
            byte tmp = b.get(p);
            b.put(p--, b.get(q));
            b.put(q++, tmp);
        }
        return b;
    }

    /**
     * Append a decimal representation of a long to a byte buffer.
     * 
     * @param n the long to be converted
     * @param b the byte buffer
     * @return the byte buffer
     * @throws java.nio.BufferOverflowException if there is not enough space in the buffer
     */
    public static ByteBuffer appendLong(long n, ByteBuffer b) {
        if (n < 0) {
            if (n == Long.MIN_VALUE) {
                b.put(MIN_LONG_BYTES);
                return b;
            }
            b.put((byte) '-');
            n = -n;
        }
        int q = b.position();
        do {
            b.put((byte) (n % 10 + '0'));
        } while ((n /= 10) > 0);
        int p = b.position() - 1;
        while (q < p) {
            byte tmp = b.get(p);
            b.put(p--, b.get(q));
            b.put(q++, tmp);
        }
        return b;
    }

    // ------------------------------------------------------------------------------------------
    // Floating-point conversion
    // ------------------------------------------------------------------------------------------

    private static final ThreadLocal<StringBuilder> STRING_BUILDER_THREAD_LOCAL =
            ThreadLocal.withInitial(() -> new StringBuilder(MAX_DOUBLE_BYTES));

    /**
     * Append a decimal representation of a {@code double} to a {@link ByteBuffer}. Works as if
     * {@link Double#toString(double)} were used.
     *
     * @param input The {@code double} to be converted
     * @param dest The {@link ByteBuffer}
     * @return {@code dest}
     */
    public static ByteBuffer appendDouble(final double input, @NotNull final ByteBuffer dest) {
        final StringBuilder sb = STRING_BUILDER_THREAD_LOCAL.get();
        sb.setLength(0);
        sb.append(input);
        final int length = sb.length();
        for (int ci = 0; ci < length; ++ci) {
            dest.put((byte) sb.charAt(ci));
        }
        return dest;
    }

    // ------------------------------------------------------------------------------------------
    // Timestamp conversion
    // ------------------------------------------------------------------------------------------

    /**
     * The number of days preceding the first day of the given month, ignoring leap days, with Jan == 1.
     */
    private static final int[] DAYS_SO_FAR = {
            0, 0, 31, 59, 90, 120, // xxx Jan Feb Mar Apr May
            151, 181, 212, 243, // Jun Jul Aug Sep
            273, 304, 334 // Oct Nov Dec
    };

    /**
     * Returns the number of days preceding the first day of the given year.
     */
    private static int yearToDays(int year) {
        int lastYear = year - 1;
        return year * 365 + (0 == year ? 0 : lastYear / 4 - lastYear / 100 + lastYear / 400);
    }

    /** Returns true if the given year is a leap year. */
    private static boolean isLeapYear(int year) {
        return 0 == year % 4 && (0 != year % 100 || 0 == year % 400) && 0 != year;
    }

    /**
     * Returns the number of leap days that have happened so far this year.
     */
    private static int countLeapDay(int year, int month) {
        return isLeapYear(year) && month > 2 ? 1 : 0;
    }

    /** Returns the number of leaps days in the given year. */
    private static int countLeapYear(int year) {
        return isLeapYear(year) ? 1 : 0;
    }

    /**
     * Append an ISO 8601 representation of millis-since-the-epoch timestamp to a byte buffer. The output length is
     * always 23 bytes plus the length of the GMT offset suffix: YYYY-MM-DDTHH:MM:SS.MMM&lt;suffix&gt;.
     *
     * @param t the timestamp to be converted, millis since 1970-01-01T00:00:00 GMT
     * @param gmtOffsetSuffix the time zone suffix, or null for no suffix
     * @param b the byte buffer
     * @return the byte buffer
     * @throws java.nio.BufferOverflowException if there is not enough space in the buffer
     */
    public static ByteBuffer appendISO8601Millis(long t, byte[] gmtOffsetSuffix, ByteBuffer b) {
        // logic copied from DateOnly

        // convert into whole days
        long days = t / 86400000L;
        int millis = (int) (t % 86400000L);
        if (millis < 0) {
            days--;
            millis += 86400000L;
        }
        // put day 0 at 0000-01-01 (ignoring calendar reform)
        days += 719527L;
        if (days < 0) {
            return appendISO8601(0, 0, 0, 0, 0, 0, 0, gmtOffsetSuffix, b);
        }
        if (days > 3652423L) {
            return appendISO8601(9999, 99, 99, 99, 99, 99, 999, gmtOffsetSuffix, b);
        }

        int year = (int) (days * 10000 / 3652425);
        int daysLeft = (int) (days - yearToDays(year));
        if (daysLeft > 364 + countLeapYear(year)) {
            daysLeft -= 365 + countLeapYear(year++);
        }

        int month = daysLeft / 31 + 1;
        if (month < 12 && DAYS_SO_FAR[month + 1] + countLeapDay(year, month + 1) < daysLeft + 1) {
            month++;
        }

        int day = daysLeft - DAYS_SO_FAR[month] - countLeapDay(year, month) + 1;

        int second = millis / 1000;
        millis %= 1000;
        int minute = second / 60;
        second %= 60;
        int hour = minute / 60;
        minute %= 60;

        /* put it into the byte buffer */
        return appendISO8601(year, month, day, hour, minute, second, millis, gmtOffsetSuffix, b);
    }

    /**
     * Append an ISO 8601 representation of a broken-down time to a byte buffer. The output length is always 23 bytes
     * plus the length of the GMT offset suffix: YYYY-MM-DDTHH:MM:SS.MMM&lt;suffix&gt;.
     *
     * @param year the year
     * @param month the month
     * @param day the day of the month
     * @param hour the hour
     * @param minute the minute
     * @param second the second
     * @param millis the millis
     * @param gmtOffsetSuffix the time zone suffix, or null for no suffix
     * @param b the byte buffer
     * @return the byte buffer
     * @throws java.nio.BufferOverflowException if there is not enough space in the buffer
     */
    public static ByteBuffer appendISO8601(int year, int month, int day,
            int hour, int minute, int second, int millis,
            byte[] gmtOffsetSuffix, ByteBuffer b) {
        b.put((byte) ('0' + year / 1000));
        b.put((byte) ('0' + (year % 1000) / 100));
        b.put((byte) ('0' + (year % 100) / 10));
        b.put((byte) ('0' + (year % 10)));
        b.put((byte) '-');
        b.put((byte) ('0' + (month / 10)));
        b.put((byte) ('0' + (month % 10)));
        b.put((byte) '-');
        b.put((byte) ('0' + (day / 10)));
        b.put((byte) ('0' + (day % 10)));
        b.put((byte) 'T');
        b.put((byte) ('0' + (hour / 10)));
        b.put((byte) ('0' + (hour % 10)));
        b.put((byte) ':');
        b.put((byte) ('0' + (minute / 10)));
        b.put((byte) ('0' + (minute % 10)));
        b.put((byte) ':');
        b.put((byte) ('0' + (second / 10)));
        b.put((byte) ('0' + (second % 10)));
        b.put((byte) '.');
        b.put((byte) ('0' + (millis / 100)));
        b.put((byte) ('0' + (millis % 100) / 10));
        b.put((byte) ('0' + (millis % 10)));
        if (gmtOffsetSuffix != null) {
            b.put(gmtOffsetSuffix);
        }
        return b;
    }

    /**
     * Append an ISO 8601 representation of micros-since-the-epoch timestamp to a byte buffer. The output length is
     * always 26 bytes plus the length of the GMT offset suffix: YYYY-MM-DDTHH:MM:SS.MMMMMM&lt;suffix&gt;.
     *
     * @param t the timestamp to be converted, micros since 1970-01-01T00:00:00 GMT
     * @param gmtOffsetSuffix the time zone suffix, or null for no suffix
     * @param b the byte buffer
     * @return the byte buffer
     * @throws java.nio.BufferOverflowException if there is not enough space in the buffer
     */
    public static ByteBuffer appendISO8601Micros(long t, byte[] gmtOffsetSuffix, ByteBuffer b) {
        // logic copied from DateOnly

        // convert into whole days
        long days = t / 86400000000L;
        long micros = t % 86400000000L;
        if (micros < 0) {
            days--;
            micros += 86400000000L;
        }
        // put day 0 at 0000-01-01 (ignoring calendar reform)
        days += 719527L;
        if (days < 0) {
            return appendISO8601Micros(0, 0, 0, 0, 0, 0, 0, 0, gmtOffsetSuffix, b);
        }
        if (days > 3652423L) {
            return appendISO8601Micros(9999, 99, 99, 99, 99, 99, 999, 999, gmtOffsetSuffix, b);
        }

        int year = (int) (days * 10000 / 3652425);
        int daysLeft = (int) (days - yearToDays(year));
        if (daysLeft > 364 + countLeapYear(year)) {
            daysLeft -= 365 + countLeapYear(year++);
        }

        int month = daysLeft / 31 + 1;
        if (month < 12 && DAYS_SO_FAR[month + 1] + countLeapDay(year, month + 1) < daysLeft + 1) {
            month++;
        }

        int day = daysLeft - DAYS_SO_FAR[month] - countLeapDay(year, month) + 1;

        int second = (int) (micros / 1000000);
        micros %= 1000000;
        int millis = (int) (micros / 1000);
        micros %= 1000;
        int minute = second / 60;
        second %= 60;
        int hour = minute / 60;
        minute %= 60;

        /* put it into the byte buffer */
        return appendISO8601Micros(year, month, day, hour, minute, second, millis, (int) micros, gmtOffsetSuffix, b);
    }

    /**
     * Append an ISO 8601 representation of a broken-down time to a byte buffer. The output length is always 23 bytes
     * plus the length of the GMT offset suffix: YYYY-MM-DDTHH:MM:SS.MMM&lt;suffix&gt;.
     *
     * @param year the year
     * @param month the month
     * @param day the day of the month
     * @param hour the hour
     * @param minute the minute
     * @param second the second
     * @param millis the millis
     * @param micros the micros
     * @param gmtOffsetSuffix the time zone suffix, or null for no suffix
     * @param b the byte buffer
     * @return the byte buffer
     * @throws java.nio.BufferOverflowException if there is not enough space in the buffer
     */
    public static ByteBuffer appendISO8601Micros(int year, int month, int day,
            int hour, int minute, int second, int millis, int micros,
            byte[] gmtOffsetSuffix, ByteBuffer b) {
        b.put((byte) ('0' + year / 1000));
        b.put((byte) ('0' + (year % 1000) / 100));
        b.put((byte) ('0' + (year % 100) / 10));
        b.put((byte) ('0' + (year % 10)));
        b.put((byte) '-');
        b.put((byte) ('0' + (month / 10)));
        b.put((byte) ('0' + (month % 10)));
        b.put((byte) '-');
        b.put((byte) ('0' + (day / 10)));
        b.put((byte) ('0' + (day % 10)));
        b.put((byte) 'T');
        b.put((byte) ('0' + (hour / 10)));
        b.put((byte) ('0' + (hour % 10)));
        b.put((byte) ':');
        b.put((byte) ('0' + (minute / 10)));
        b.put((byte) ('0' + (minute % 10)));
        b.put((byte) ':');
        b.put((byte) ('0' + (second / 10)));
        b.put((byte) ('0' + (second % 10)));
        b.put((byte) '.');
        b.put((byte) ('0' + (millis / 100)));
        b.put((byte) ('0' + (millis % 100) / 10));
        b.put((byte) ('0' + (millis % 10)));
        b.put((byte) ('0' + (micros / 100)));
        b.put((byte) ('0' + (micros % 100) / 10));
        b.put((byte) ('0' + (micros % 10)));
        if (gmtOffsetSuffix != null) {
            b.put(gmtOffsetSuffix);
        }
        return b;
    }
}
