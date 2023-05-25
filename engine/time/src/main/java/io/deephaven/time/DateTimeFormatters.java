/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time;

/**
 * Common DateTimeFormatters.
 */
public enum DateTimeFormatters {
    // @formatter:off
    ISO9TZ(true, true, true, 9, true),
    ISO6TZ(true, true, true, 6, true),
    ISO3TZ(true, true, true, 3, true),
    ISO0TZ(true, true, true, 0, true),
    ISO9(true, true, true, 9, false),
    ISO6(true, true, true, 6, false),
    ISO3(true, true, true, 3, false),
    ISO0(true, true, true, 0, false),
    NONISO9TZ(false, true, true, 9, true),
    NONISO6TZ(false, true, true, 6, true),
    NONISO3TZ(false, true, true, 3, true),
    NONISO0TZ(false, true, true, 0, true),
    NONISO9(false, true, true, 9, false),
    NONISO6(false, true, true, 6, false),
    NONISO3(false, true, true, 3, false),
    NONISO0(false, true, true, 0, false),
    NODATE9TZ(true, false, true, 9, true),
    NODATE6TZ(true, false, true, 6, true),
    NODATE3TZ(true, false, true, 3, true),
    NODATE0TZ(true, false, true, 0, true),
    NODATE9(true, false, true, 9, false),
    NODATE6(true, false, true, 6, false),
    NODATE3(true, false, true, 3, false),
    NODATE0(true, false, true, 0, false),
    DATEONLYTZ(true, true, false, 0, true),
    DATEONLY(true, true, false, 0, false),
    // @formatter:on
    ;

    private final DateTimeFormatter formatter;

    DateTimeFormatters(final boolean isISO, final boolean hasDate, final boolean hasTime, final int subsecondDigits,
            final boolean hasTZ) {
        this.formatter = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);
    }

    public DateTimeFormatter getFormatter() {
        return formatter;
    }

    @Override
    public String toString() {
        return formatter.toString();
    }
}
