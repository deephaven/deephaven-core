//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time;


import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * String formatter for {@link Instant} and {@link ZonedDateTime}.
 */
public class DateTimeFormatter {

    private final String pattern;
    private final Map<ZoneId, java.time.format.DateTimeFormatter> formatCacheID = new HashMap<>(3);

    /**
     * Creates a new date time formatter using the default time zone.
     *
     * See {@link java.time.format.DateTimeFormatter} for valid format strings.
     *
     * @param pattern format pattern.
     * @see ZoneId#systemDefault()
     * @see java.time.format.DateTimeFormatter
     */
    public DateTimeFormatter(String pattern) {
        this.pattern = pattern;

        // Do this here so we fail fast if there's a problem with the format string
        getFormatter(DateTimeUtils.timeZone());
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
        final String timeZone = TimeZoneAliases.zoneName(tz);
        return formatCacheID.computeIfAbsent(tz, newTz -> java.time.format.DateTimeFormatter
                .ofPattern(pattern.replaceAll("%t", '\'' + timeZone + '\'')));
    }

    /**
     * Returns a ZonedDateTime formatted as a string.
     *
     * @param dateTime date time to format as a string.
     * @return date time formatted as a string.
     */
    @NotNull
    public String format(@NotNull final ZonedDateTime dateTime) {
        return dateTime.format(getFormatter(dateTime.getZone()));
    }

    /**
     * Returns an Instant formatted as a string.
     *
     * @param instant time to format as a string.
     * @param timeZone time zone to use when formatting the string.
     * @return date time formatted as a string.
     */
    @NotNull
    public String format(@NotNull final Instant instant, @NotNull final ZoneId timeZone) {
        return format(instant.atZone(timeZone));
    }

    /**
     * Returns an Instant formatted as a string using the default time zone.
     *
     * @param instant time to format as a string.
     * @return date time formatted as a string.
     * @see ZoneId#systemDefault()
     */
    @NotNull
    public String format(@NotNull final Instant instant) {
        return format(instant, DateTimeUtils.timeZone());
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
