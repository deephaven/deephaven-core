/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.time;

/**
 * Common DateTimeFormatters.
 */
public enum DateTimeFormatters {
    ISO9TZ(new DateTimeFormatter(true, true, true, 9, true)), ISO6TZ(
            new DateTimeFormatter(true, true, true, 6, true)), ISO3TZ(
                    new DateTimeFormatter(true, true, true, 3, true)), ISO0TZ(
                            new DateTimeFormatter(true, true, true, 0, true)), ISO9(
                                    new DateTimeFormatter(true, true, true, 9, false)), ISO6(
                                            new DateTimeFormatter(true, true, true, 6, false)), ISO3(
                                                    new DateTimeFormatter(true, true, true, 3, false)), ISO0(
                                                            new DateTimeFormatter(true, true, true, 0, false)),

    NONISO9TZ(new DateTimeFormatter(false, true, true, 9, true)), NONISO6TZ(
            new DateTimeFormatter(false, true, true, 6, true)), NONISO3TZ(
                    new DateTimeFormatter(false, true, true, 3, true)), NONISO0TZ(
                            new DateTimeFormatter(false, true, true, 0, true)), NONISO9(
                                    new DateTimeFormatter(false, true, true, 9, false)), NONISO6(
                                            new DateTimeFormatter(false, true, true, 6, false)), NONISO3(
                                                    new DateTimeFormatter(false, true, true, 3, false)), NONISO0(
                                                            new DateTimeFormatter(false, true, true, 0, false)),

    NODATE9TZ(new DateTimeFormatter(true, false, true, 9, true)), NODATE6TZ(
            new DateTimeFormatter(true, false, true, 6, true)), NODATE3TZ(
                    new DateTimeFormatter(true, false, true, 3, true)), NODATE0TZ(
                            new DateTimeFormatter(true, false, true, 0, true)), NODATE9(
                                    new DateTimeFormatter(true, false, true, 9, false)), NODATE6(
                                            new DateTimeFormatter(true, false, true, 6, false)), NODATE3(
                                                    new DateTimeFormatter(true, false, true, 3, false)), NODATE0(
                                                            new DateTimeFormatter(true, false, true, 0, false)),

    DATEONLYTZ(new DateTimeFormatter(true, true, false, 0, true)), DATEONLY(
            new DateTimeFormatter(true, true, false, 0, false)),
            ;

    private final DateTimeFormatter formatter;

    DateTimeFormatters(DateTimeFormatter formatter) {
        this.formatter = formatter;
    }

    public DateTimeFormatter getFormatter() {
        return formatter;
    }

    @Override
    public String toString() {
        return formatter.toString();
    }
}
