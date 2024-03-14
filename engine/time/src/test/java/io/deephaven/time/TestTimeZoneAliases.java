//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time;

import io.deephaven.base.testing.BaseArrayTestCase;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;

import java.time.ZoneId;
import java.util.Map;

public class TestTimeZoneAliases extends BaseArrayTestCase {

    final String[][] values = {
            {"NY", "America/New_York"},
            {"MN", "America/Chicago"},
            {"JP", "Asia/Tokyo"},
            {"SG", "Asia/Singapore"},
            {"UTC", "UTC"},
            {"America/Argentina/Buenos_Aires", "America/Argentina/Buenos_Aires"}
    };

    public void testDefaultAliases() {
        for (final String[] v : values) {
            final ZoneId target = ZoneId.of(v[1]);
            final ZoneId id = TimeZoneAliases.zoneId(v[1]);
            TestCase.assertEquals(target, id);
            TestCase.assertEquals(v[0], TimeZoneAliases.zoneName(id));
        }
    }

    public void testAllZones() {
        @NotNull
        final Map<String, ZoneId> all = TimeZoneAliases.getAllZones();

        for (final String[] v : values) {
            final ZoneId target = ZoneId.of(v[1]);
            TestCase.assertEquals(target, all.get(v[0]));
            TestCase.assertEquals(target, all.get(v[1]));
        }
    }

    public void testAddRmAlias() {
        final String alias = "BA";
        final String tz = "America/Argentina/Buenos_Aires";
        TestCase.assertFalse(TimeZoneAliases.rmAlias(alias));
        TestCase.assertFalse(TimeZoneAliases.getAllZones().containsKey(alias));
        TimeZoneAliases.addAlias(alias, tz);
        TestCase.assertTrue(TimeZoneAliases.getAllZones().containsKey(alias));
        TestCase.assertEquals(ZoneId.of(tz), TimeZoneAliases.zoneId(alias));
        TestCase.assertEquals(alias, TimeZoneAliases.zoneName(ZoneId.of(tz)));
        TestCase.assertTrue(TimeZoneAliases.rmAlias(alias));
        TestCase.assertFalse(TimeZoneAliases.getAllZones().containsKey(alias));
    }
}
