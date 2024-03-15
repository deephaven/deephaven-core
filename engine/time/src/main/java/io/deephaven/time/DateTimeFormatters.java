//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time;

/**
 * Common date time formatters.
 *
 * @see DateTimeFormatter
 */
public enum DateTimeFormatters {
    // @formatter:off
    /** ISO date plus time format with a 'T" separating the date and time, 9 sub-second digits and, the time zone. */
    ISO9TZ(true, true, true, 9, true),
    /** ISO date plus time format with a 'T" separating the date and time, 6 sub-second digits and, the time zone. */
    ISO6TZ(true, true, true, 6, true),
    /** ISO date plus time format with a 'T" separating the date and time, 3 sub-second digits and, the time zone. */
    ISO3TZ(true, true, true, 3, true),
    /** ISO date plus time format with a 'T" separating the date and time, 0 sub-second digits and, the time zone. */
    ISO0TZ(true, true, true, 0, true),
    /** ISO date plus time format with a 'T" separating the date and time, 9 sub-second digits and, no time zone. */
    ISO9(true, true, true, 9, false),
    /** ISO date plus time format with a 'T" separating the date and time, 6 sub-second digits and, no time zone. */
    ISO6(true, true, true, 6, false),
    /** ISO date plus time format with a 'T" separating the date and time, 3 sub-second digits and, no time zone. */
    ISO3(true, true, true, 3, false),
    /** ISO date plus time format with a 'T" separating the date and time, 0 sub-second digits and, no time zone. */
    ISO0(true, true, true, 0, false),
    /** Date plus time format with a space separating the date and time, 9 sub-second digits, and the time zone. */
    NONISO9TZ(false, true, true, 9, true),
    /** Date plus time format with a space separating the date and time, 6 sub-second digits, and the time zone. */
    NONISO6TZ(false, true, true, 6, true),
    /** Date plus time format with a space separating the date and time, 3 sub-second digits, and the time zone. */
    NONISO3TZ(false, true, true, 3, true),
    /** Date plus time format with a space separating the date and time, 0 sub-second digits, and the time zone. */
    NONISO0TZ(false, true, true, 0, true),
    /** Date plus time format with a space separating the date and time, 9 sub-second digits, and no time zone. */
    NONISO9(false, true, true, 9, false),
    /** Date plus time format with a space separating the date and time, 6 sub-second digits, and no time zone. */
    NONISO6(false, true, true, 6, false),
    /** Date plus time format with a space separating the date and time, 3 sub-second digits, and no time zone. */
    NONISO3(false, true, true, 3, false),
    /** Date plus time format with a space separating the date and time, 0 sub-second digits, and no time zone. */
    NONISO0(false, true, true, 0, false),
    /** Time only format with 9 sub-second digits and the time zone. */
    NODATE9TZ(true, false, true, 9, true),
    /** Time only format with 6 sub-second digits and the time zone. */
    NODATE6TZ(true, false, true, 6, true),
    /** Time only format with 3 sub-second digits and the time zone. */
    NODATE3TZ(true, false, true, 3, true),
    /** Time only format with 0 sub-second digits and the time zone. */
    NODATE0TZ(true, false, true, 0, true),
    /** Time only format with 9 sub-second digits and no time zone. */
    NODATE9(true, false, true, 9, false),
    /** Time only format with 6 sub-second digits and no time zone. */
    NODATE6(true, false, true, 6, false),
    /** Time only format with 3 sub-second digits and no time zone. */
    NODATE3(true, false, true, 3, false),
    /** Time only format with 0 sub-second digits and no time zone. */
    NODATE0(true, false, true, 0, false),
    /** Date only format with the time zone. */
    DATEONLYTZ(true, true, false, 0, true),
    /** Date only format. */
    DATEONLY(true, true, false, 0, false),
    // @formatter:on
    ;

    private final DateTimeFormatter formatter;

    DateTimeFormatters(final boolean isISO, final boolean hasDate, final boolean hasTime, final int subsecondDigits,
            final boolean hasTZ) {
        this.formatter = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);
    }

    /**
     * Gets the formatter.
     *
     * @return formatter.
     */
    public DateTimeFormatter getFormatter() {
        return formatter;
    }

    @Override
    public String toString() {
        return formatter.toString();
    }
}
