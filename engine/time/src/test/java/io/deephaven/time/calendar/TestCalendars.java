//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.configuration.Configuration;
import io.deephaven.time.DateTimeUtils;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Objects;

public class TestCalendars extends BaseArrayTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        CalendarInit.init();
    }

    public void testDefault() {
        final BusinessCalendar calendar = Calendars.calendar();
        assertEquals(Configuration.getInstance().getProperty("Calendar.default"), Calendars.calendarName());
        assertEquals(Configuration.getInstance().getProperty("Calendar.default"), calendar.name());

        final String defaultCal = calendar.name();

        assertNotSame("CAL2", defaultCal);
        Calendars.setCalendar("CAL2");
        assertEquals("CAL2", Calendars.calendarName());
        assertEquals("CAL2", Calendars.calendar().name());

        Calendars.setCalendar(defaultCal);
    }

    public void testCalendarNames() {
        assertEquals(new String[] {"CAL1", "CAL2", "USBANK_EXAMPLE", "USNYSE_EXAMPLE", "UTC"},
                Calendars.calendarNames());
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

        assertNull(Calendars.calendar(null));
    }

    public void testAdd() throws URISyntaxException {

        final String path = Paths
                .get(Objects.requireNonNull(TestBusinessCalendarXMLParser.class.getResource("/PARSER-TEST.calendar"))
                        .toURI())
                .toString();
        final String calName = "PARSER_TEST_CAL";

        try {
            Calendars.calendar(calName);
            fail("Should have thrown an exception");
        } catch (Exception e) {
            // pass
        }

        Calendars.addCalendarFromFile(path);

        final BusinessCalendar cal = Calendars.calendar(calName);
        TestBusinessCalendarXMLParser.assertParserTestCal(cal);

        Calendars.removeCalendar(calName);

        try {
            Calendars.calendar(calName);
            fail("Should have thrown an exception");
        } catch (Exception e) {
            // pass
        }
    }

    public void testUTCDayLength() {
        final BusinessCalendar cal = Calendars.calendar("UTC");
        assertEquals(DateTimeUtils.DAY, cal.standardBusinessDay().businessNanos());
    }

    public void testNYSEDayLength() {
        final BusinessCalendar cal = Calendars.calendar("USNYSE_EXAMPLE");
        assertEquals(6 * DateTimeUtils.HOUR + 30 * DateTimeUtils.MINUTE, cal.standardBusinessDay().businessNanos());
    }
}
