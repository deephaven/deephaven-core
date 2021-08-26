/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

/**
 * Common DBDateTimeFormatters.
 */
public enum DBDateTimeFormatters {
    ISO9TZ(new DBDateTimeFormatter(true, true, true, 9, true)), ISO6TZ(
        new DBDateTimeFormatter(true, true, true, 6, true)), ISO3TZ(
            new DBDateTimeFormatter(true, true, true, 3, true)), ISO0TZ(
                new DBDateTimeFormatter(true, true, true, 0, true)), ISO9(
                    new DBDateTimeFormatter(true, true, true, 9, false)), ISO6(
                        new DBDateTimeFormatter(true, true, true, 6, false)), ISO3(
                            new DBDateTimeFormatter(true, true, true, 3, false)), ISO0(
                                new DBDateTimeFormatter(true, true, true, 0, false)),

    NONISO9TZ(new DBDateTimeFormatter(false, true, true, 9, true)), NONISO6TZ(
        new DBDateTimeFormatter(false, true, true, 6, true)), NONISO3TZ(
            new DBDateTimeFormatter(false, true, true, 3, true)), NONISO0TZ(
                new DBDateTimeFormatter(false, true, true, 0, true)), NONISO9(
                    new DBDateTimeFormatter(false, true, true, 9, false)), NONISO6(
                        new DBDateTimeFormatter(false, true, true, 6, false)), NONISO3(
                            new DBDateTimeFormatter(false, true, true, 3, false)), NONISO0(
                                new DBDateTimeFormatter(false, true, true, 0, false)),

    NODATE9TZ(new DBDateTimeFormatter(true, false, true, 9, true)), NODATE6TZ(
        new DBDateTimeFormatter(true, false, true, 6, true)), NODATE3TZ(
            new DBDateTimeFormatter(true, false, true, 3, true)), NODATE0TZ(
                new DBDateTimeFormatter(true, false, true, 0, true)), NODATE9(
                    new DBDateTimeFormatter(true, false, true, 9, false)), NODATE6(
                        new DBDateTimeFormatter(true, false, true, 6, false)), NODATE3(
                            new DBDateTimeFormatter(true, false, true, 3, false)), NODATE0(
                                new DBDateTimeFormatter(true, false, true, 0, false)),

    DATEONLYTZ(new DBDateTimeFormatter(true, true, false, 0, true)), DATEONLY(
        new DBDateTimeFormatter(true, true, false, 0, false)),
        ;

    private final DBDateTimeFormatter formatter;

    DBDateTimeFormatters(DBDateTimeFormatter formatter) {
        this.formatter = formatter;
    }

    public DBDateTimeFormatter getFormatter() {
        return formatter;
    }

    @Override
    public String toString() {
        return formatter.toString();
    }
}
