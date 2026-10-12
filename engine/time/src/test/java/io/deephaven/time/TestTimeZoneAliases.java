//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Map;

import static org.junit.Assert.*;

public class TestTimeZoneAliases {

    final String[][] values = {
            {"NY", "America/New_York"},
            {"MN", "America/Chicago"},
            {"JP", "Asia/Tokyo"},
            {"SG", "Asia/Singapore"},
            {"UTC", "UTC"},
            {"America/Argentina/Buenos_Aires", "America/Argentina/Buenos_Aires"}
    };

    @Test
    public void testDefaultAliases() {
        for (final String[] v : values) {
            final ZoneId target = ZoneId.of(v[1]);
            final ZoneId id = TimeZoneAliases.zoneId(v[1]);
            assertEquals(target, id);
            assertEquals(v[0], TimeZoneAliases.zoneName(id));
        }
    }

    @Test
    public void testAllZones() {
        @NotNull
        final Map<String, ZoneId> all = TimeZoneAliases.getAllZones();

        for (final String[] v : values) {
            final ZoneId target = ZoneId.of(v[1]);
            assertEquals(target, all.get(v[0]));
            assertEquals(target, all.get(v[1]));
        }
    }

    @Test
    public void testAddRmAlias() {
        final String alias = "BA";
        final String tz = "America/Argentina/Buenos_Aires";
        assertFalse(TimeZoneAliases.rmAlias(alias));
        assertFalse(TimeZoneAliases.getAllZones().containsKey(alias));
        TimeZoneAliases.addAlias(alias, tz);
        assertTrue(TimeZoneAliases.getAllZones().containsKey(alias));
        assertEquals(ZoneId.of(tz), TimeZoneAliases.zoneId(alias));
        assertEquals(alias, TimeZoneAliases.zoneName(ZoneId.of(tz)));
        assertTrue(TimeZoneAliases.rmAlias(alias));
        assertFalse(TimeZoneAliases.getAllZones().containsKey(alias));
    }
}
