/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.time;


import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

/**
 * Formatter for DateTimes.
 */
public class DateTimeFormatter {
    private final String pattern;
    private final Map<TimeZone, java.time.format.DateTimeFormatter> formatCache = new HashMap<>(3);

    public DateTimeFormatter(String pattern) {
        this.pattern = pattern;

        // Do this here so we fail fast if there's a problem with the format string
        getFormatter(TimeZone.TZ_DEFAULT);
    }

    public DateTimeFormatter(final boolean isISO, final boolean hasDate, final boolean hasTime,
            final int subsecondDigits, final boolean hasTZ) {
        this((hasDate ? "yyyy-MM-dd" : "") + (!hasDate || !hasTime ? "" : isISO ? "'T'" : " ") +
                (hasTime ? "HH:mm:ss" : "") + (hasTime && subsecondDigits > 0 ? "." : "") +
                (hasTime ? "S".repeat(subsecondDigits) : "") + (hasTZ ? " %t" : ""));
    }

    private java.time.format.DateTimeFormatter getFormatter(TimeZone tz) {
        return formatCache.computeIfAbsent(tz, newTz -> java.time.format.DateTimeFormatter
                .ofPattern(pattern.replaceAll("%t", '\'' + tz.toString().substring(3) + '\'')));
    }

    public String format(DateTime dateTime, TimeZone tz) {
        final ZoneId zone = tz.getTimeZone().toTimeZone().toZoneId();
        return dateTime.getInstant().atZone(zone).format(getFormatter(tz));
    }

    public String format(DateTime dateTime) {
        return format(dateTime, TimeZone.TZ_DEFAULT);
    }

    @Override
    public String toString() {
        return format(DateTime.now());
    }

    public String getPattern() {
        return pattern;
    }
}
