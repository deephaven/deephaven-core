/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time;

import org.jetbrains.annotations.NotNull;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

/**
 * Common time zones.
 *
 * In Deephaven queries, these time zones can be referenced using short 2- or 3-character abbreviations.
 * For example, in the date time string '2023-04-05T10:13 NY', 'NY' refers to the #TZ_NY time zone.
 */
//TODO: test coverage
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

    //TODO: this is public and there are getter/setter methods.  Decide which is the right approach.
    /**
     * The default time zone for display purposes.
     */
    public static TimeZone TZ_DEFAULT = TZ_NY;

    private final ZoneId zoneId;

    private TimeZone(final @NotNull String timeZone) {
        this.zoneId = ZoneId.of(timeZone);
    }

    /**
     * Returns the {@link ZoneId} for this time zone;
     * 
     * @return the ZoneId
     */
    public ZoneId getZoneId() {
        return zoneId;
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
            final java.util.TimeZone tz1 = java.util.TimeZone.getTimeZone(t1.getZoneId());
            final java.util.TimeZone tz2 = java.util.TimeZone.getTimeZone(t1.getZoneId());

            int ret = tz2.getOffset(now) - tz1.getOffset(now);
            if (ret != 0) {
                return ret;
            } else {
                ret = t1.getZoneId().getId().compareTo(t2.getZoneId().getId());
                return ret != 0 ? ret : t1.name().compareTo(t2.name());
            }
        });

        return allZones.toArray(new TimeZone[0]);
    }

    /**
     * Gets the default time zone.
     *
     * @return the default {@link TimeZone}
     */
    public static TimeZone getDefault() {
        return TZ_DEFAULT;
    }

    /**
     * Sets the default time zone.
     *
     * @param tzDefault the {@link TimeZone} to be used as the default.
     */
    public static void setDefault(TimeZone tzDefault) {
        TZ_DEFAULT = tzDefault;
    }
}
