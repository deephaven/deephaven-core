/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.configuration.Configuration;

public class TestCalendars extends BaseArrayTestCase {

    public void testDefault() {
        final BusinessCalendar calendar = Calendars.calendar();
        assertEquals(Configuration.getInstance().getProperty("Calendar.default"), calendar.name());

        final String defaultCal = calendar.name();

        Calendars.setDefaultCalendar("CAL2");
        assertEquals("CAL2", Calendars.calendar().name());

        Calendars.setDefaultCalendar(defaultCal);
    }

    public void testCalendarNames() {
        assertEquals(new String[] {"CAL1", "CAL2"}, Calendars.calendarNames());
    }

    public void testCalendar() {
        for (final String cn : Calendars.calendarNames()) {
            final BusinessCalendar calendar = Calendars.calendar(cn);
            assertEquals(cn, calendar.name());
        }

        try {
            Calendars.calendar("JUNK");
            fail("Should have thrown an exception");
        } catch (Exception e) {
            // pass
        }
    }
}
