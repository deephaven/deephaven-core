/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time;


import org.jetbrains.annotations.NotNull;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

/**
 * String formatter for {@link DateTime}.
 */
public class DateTimeFormatter {

    private final String pattern;
    private final Map<TimeZone, java.time.format.DateTimeFormatter> formatCacheTZ = new HashMap<>(3);
    private final Map<ZoneId, java.time.format.DateTimeFormatter> formatCacheID = new HashMap<>(3);

    /**
     * Creates a new date time formatter using the default time zone.
     *
     * See {@link java.time.format.DateTimeFormatter} for valid format strings.
     *
     * @param pattern format pattern.
     * @see TimeZone#TZ_DEFAULT
     * @see java.time.format.DateTimeFormatter
     */
    public DateTimeFormatter(String pattern) {
        this.pattern = pattern;

        // Do this here so we fail fast if there's a problem with the format string
        getFormatter(TimeZone.TZ_DEFAULT.getZoneId());
    }

    /**
     * Creates a new date time formatter.
     *
     * @param isISO ISO 8601 format
     * @param hasDate include date
     * @param hasTime include time
     * @param subsecondDigits include subsecond digits
     * @param hasTZ include time zone
     */
    public DateTimeFormatter(final boolean isISO, final boolean hasDate, final boolean hasTime,
            final int subsecondDigits, final boolean hasTZ) {
        this((hasDate ? "yyyy-MM-dd" : "") + (!hasDate || !hasTime ? "" : isISO ? "'T'" : " ") +
                (hasTime ? "HH:mm:ss" : "") + (hasTime && subsecondDigits > 0 ? "." : "") +
                (hasTime ? "S".repeat(subsecondDigits) : "") + (hasTZ ? " %t" : ""));
    }

    private java.time.format.DateTimeFormatter getFormatter(ZoneId tz) {
        return formatCacheID.computeIfAbsent(tz, newTz -> java.time.format.DateTimeFormatter
                .ofPattern(pattern.replaceAll("%t", '\'' + tz.getId() + '\'')));
    }

    private java.time.format.DateTimeFormatter getFormatter(TimeZone tz) {
        return formatCacheTZ.computeIfAbsent(tz, newTz -> java.time.format.DateTimeFormatter
                .ofPattern(pattern.replaceAll("%t", '\'' + tz.toString().substring(3) + '\'')));
    }

    /**
     * Returns a DateTime formatted as a string.
     *
     * @param dateTime time to format as a string.
     * @param timeZone time zone to use when formatting the string.
     * @return date time formatted as a string.
     */
    @NotNull
    public String format(@NotNull final DateTime dateTime, @NotNull final ZoneId timeZone) {
        return dateTime.toInstant().atZone(timeZone).format(getFormatter(timeZone));
    }

    /**
     * Returns a DateTime formatted as a string.
     *
     * @param dateTime time to format as a string.
     * @param timeZone time zone to use when formatting the string.
     * @return date time formatted as a string.
     */
    @NotNull
    public String format(@NotNull final DateTime dateTime, @NotNull final TimeZone timeZone) {
        return dateTime.toInstant().atZone(timeZone.getZoneId()).format(getFormatter(timeZone));
    }

    /**
     * Returns a DateTime formatted as a string using the default time zone.
     *
     * @param dateTime time to format as a string.
     * @return date time formatted as a string.
     * @see TimeZone#TZ_DEFAULT
     */
    @NotNull
    public String format(@NotNull final DateTime dateTime) {
        return format(dateTime, TimeZone.TZ_DEFAULT);
    }

    @Override
    public String toString() {
        return "DateTimeFormatter{" +
                "pattern='" + pattern + '\'' +
                '}';
    }

    /**
     * Gets the format pattern.
     *
     * @return format pattern.
     */
    public String getPattern() {
        return pattern;
    }
}
