/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import org.apache.commons.lang3.StringUtils;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Formatter for DBDateTimes.
 */
public class DBDateTimeFormatter {
    private final String pattern;
    private final Map<DBTimeZone, DateTimeFormatter> formatCache = new HashMap<>(3);

    public DBDateTimeFormatter(String pattern) {
        this.pattern = pattern;

        // Do this here so we fail fast if there's a problem with the format string
        getFormatter(DBTimeZone.TZ_DEFAULT);
    }

    public DBDateTimeFormatter(final boolean isISO, final boolean hasDate, final boolean hasTime,
        final int subsecondDigits, final boolean hasTZ) {
        this((hasDate ? "yyyy-MM-dd" : "") + (!hasDate || !hasTime ? "" : isISO ? "'T'" : " ") +
            (hasTime ? "HH:mm:ss" : "") + (hasTime && subsecondDigits > 0 ? "." : "") +
            (hasTime ? StringUtils.repeat("S", subsecondDigits) : "") + (hasTZ ? " %t" : ""));
    }

    private DateTimeFormatter getFormatter(DBTimeZone tz) {
        return formatCache.computeIfAbsent(tz, newTz -> DateTimeFormatter
            .ofPattern(pattern.replaceAll("%t", '\'' + tz.toString().substring(3) + '\'')));
    }

    public String format(DBDateTime dateTime, DBTimeZone tz) {
        final ZoneId zone = tz.getTimeZone().toTimeZone().toZoneId();
        return dateTime.getInstant().atZone(zone).format(getFormatter(tz));
    }

    public String format(DBDateTime dateTime) {
        return format(dateTime, DBTimeZone.TZ_DEFAULT);
    }

    @Override
    public String toString() {
        return format(DBDateTime.now());
    }

    public String getPattern() {
        return pattern;
    }
}
