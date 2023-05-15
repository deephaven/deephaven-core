/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time;

import io.deephaven.base.testing.BaseArrayTestCase;
import junit.framework.TestCase;

import java.time.Instant;
import java.time.ZoneId;

public class TestTimeZone extends BaseArrayTestCase {

    public void testConstructor() {
        TestCase.assertEquals(TimeZone.TZ_MN.getZoneId(), ZoneId.of("America/Chicago"));
    }

    public void testValuesByOffset() {
        final long now = Instant.now().toEpochMilli();
        final TimeZone[] vbo = TimeZone.valuesByOffset();
        assertEquals(vbo.length, TimeZone.values().length);

        for(int i=1; i<vbo.length; i++) {
            final int o1 = java.util.TimeZone.getTimeZone(vbo[i-1].getZoneId()).getOffset(now);
            final int o2 = java.util.TimeZone.getTimeZone(vbo[i].getZoneId()).getOffset(now);
            TestCase.assertTrue("Offsets: " + o1 + " " + o2, o2 <= o1);
        }
    }

    public void testDefaults() {
        final TimeZone initial = TimeZone.TZ_DEFAULT;
        TestCase.assertEquals(initial, TimeZone.getDefault());
        TimeZone.setDefault(TimeZone.TZ_AL);
        TestCase.assertEquals(TimeZone.TZ_AL, TimeZone.getDefault());
        TimeZone.setDefault(initial);
    }
}
