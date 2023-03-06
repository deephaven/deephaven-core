/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time;

import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTimeZone;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

/**
 * Defines Deephaven-supported timezones, which may be used for PQ-scheduling and display purposes
 */
public enum TimeZone {
    /**
     * America/New_York
     */
    TZ_NY("America/New_York"),
    /**
     * America/New_York
     */
    TZ_ET("America/New_York"),
    /**
     * America/Chicago
     */
    TZ_MN("America/Chicago"),
    /**
     * America/Chicago
     */
    TZ_CT("America/Chicago"),
    /**
     * America/Denver
     */
    TZ_MT("America/Denver"),
    /**
     * America/Los_Angeles
     */
    TZ_PT("America/Los_Angeles"),
    /**
     * Pacific/Honolulu
     */
    TZ_HI("Pacific/Honolulu"),
    /**
     * America/Sao_Paulo
     */
    TZ_BT("America/Sao_Paulo"),
    /**
     * Asia/Seoul
     */
    TZ_KR("Asia/Seoul"),
    /**
     * Asia/Hong_Kong
     */
    TZ_HK("Asia/Hong_Kong"),
    /**
     * Asia/Tokyo
     */
    TZ_JP("Asia/Tokyo"),
    /**
     * Canada/Atlantic
     */
    TZ_AT("Canada/Atlantic"),
    /**
     * Canada/Newfoundland
     */
    TZ_NF("Canada/Newfoundland"),
    /**
     * America/Anchorage
     */
    TZ_AL("America/Anchorage"),
    /**
     * Asia/Kolkata
     */
    TZ_IN("Asia/Kolkata"),
    /**
     * Europe/Berlin
     */
    TZ_CE("Europe/Berlin"),
    /**
     * Asia/Singapore
     */
    TZ_SG("Asia/Singapore"),
    /**
     * Europe/London
     */
    TZ_LON("Europe/London"),
    /**
     * Europe/Moscow
     */
    TZ_MOS("Europe/Moscow"),
    /**
     * Asia/Shanghai
     */
    TZ_SHG("Asia/Shanghai"),
    /**
     * Europe/Zurich
     */
    TZ_CH("Europe/Zurich"),
    /**
     * Europe/Amsterdam
     */
    TZ_NL("Europe/Amsterdam"),
    /**
     * Asia/Taipei
     */
    TZ_TW("Asia/Taipei"),
    /**
     * Australia/Sydney
     */
    TZ_SYD("Australia/Sydney"),
    /**
     * UTC
     */
    TZ_UTC("UTC");

    /**
     * The default time zone for display purposes.
     */
    public static TimeZone TZ_DEFAULT = TZ_NY;

    private final DateTimeZone timeZone;
    private final ZoneId zoneId;

    TimeZone(final @NotNull String timeZone) {
        this.timeZone = DateTimeZone.forID(timeZone);
        this.zoneId = ZoneId.of(timeZone);
    }

    /**
     * Returns the underlying Joda time zone for this TimeZone.
     *
     * @return the underlying Joda time zone.
     */
    public DateTimeZone getTimeZone() {
        return timeZone;
    }

    /**
     * Returns the Java ZoneID for this DBTimeZone;
     * 
     * @return the ZoneId
     */
    public ZoneId getZoneId() {
        return zoneId;
    }

    /**
     * Find the corresponding TimeZone for a given Joda DateTimeZone.
     *
     * @param dateTimeZone the time zone to search for
     *
     * @return the corresponding TimeZone, or null if none was found
     */
    public static TimeZone lookup(DateTimeZone dateTimeZone) {
        for (TimeZone zone : values()) {
            if (zone.getTimeZone().equals(dateTimeZone)) {
                return zone;
            }
        }
        return lookupByOffset(dateTimeZone);
    }

    private static TimeZone lookupByOffset(DateTimeZone dateTimeZone) {
        for (TimeZone zone : values()) {
            if (zone.getTimeZone().getOffset(System.currentTimeMillis()) == dateTimeZone
                    .getOffset(System.currentTimeMillis())) {
                return zone;
            }
        }
        return null;
    }

    /**
     * This method returns the same contents as {@link TimeZone#values()}, but ordered by geographic location / UTC
     * offset. If two elements exist within the same timezone, they are second-order-sorted by name
     *
     * @return An array of TimeZones ordered by UTC-offset
     */
    public static TimeZone[] valuesByOffset() {
        final List<TimeZone> allZones = Arrays.asList(values());
        final long now = System.currentTimeMillis();

        allZones.sort((t1, t2) -> {
            int ret = t2.getTimeZone().getOffset(now) - t1.getTimeZone().getOffset(now);
            if (ret != 0) {
                return ret;
            } else {
                ret = t1.getTimeZone().getID().compareTo(t2.getTimeZone().getID());
                return ret != 0 ? ret : t1.name().compareTo(t2.name());
            }
        });

        return allZones.toArray(new TimeZone[0]);
    }

    /**
     * Get the default time zone.
     *
     * @return the default {@link TimeZone}
     */
    public static TimeZone getTzDefault() {
        return TZ_DEFAULT;
    }

    /**
     * Set the default time zone.
     *
     * @param tzDefault the {@link TimeZone} to be used as the default.
     */
    public static void setTzDefault(TimeZone tzDefault) {
        TZ_DEFAULT = tzDefault;
    }
}
