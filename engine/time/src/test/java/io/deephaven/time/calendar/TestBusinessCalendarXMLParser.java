//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.time.DateTimeUtils;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Objects;

public class TestBusinessCalendarXMLParser extends BaseArrayTestCase {

    public static void assertParserTestCal(final BusinessCalendar cal) {
        assertEquals("PARSER_TEST_CAL", cal.name());
        assertEquals("Test Calendar", cal.description());
        assertEquals(DateTimeUtils.timeZone("Asia/Tokyo"), cal.timeZone());
        assertEquals(LocalDate.of(2000, 1, 2), cal.firstValidDate());
        assertEquals(LocalDate.of(2030, 11, 12), cal.lastValidDate());
        assertEquals(2, cal.weekendDays().size());
        assertEquals(LocalTime.of(6, 14), cal.standardBusinessDay().businessStart());
        assertEquals(LocalTime.of(12, 34), cal.standardBusinessDay().businessEnd());
        assertTrue(cal.weekendDays().contains(DayOfWeek.MONDAY));
        assertTrue(cal.weekendDays().contains(DayOfWeek.WEDNESDAY));
        assertEquals(2, cal.holidays().size());
        assertTrue(cal.holidays().containsKey(LocalDate.of(2015, 1, 1)));
        assertTrue(cal.holidays().containsKey(LocalDate.of(2015, 4, 6)));

        assertEquals(DateTimeUtils.parseInstant("2015-04-06T14:15 Asia/Tokyo"),
                cal.calendarDay("2015-04-06").businessStart());
        assertEquals(DateTimeUtils.parseInstant("2015-04-06T16:46 Asia/Tokyo"),
                cal.calendarDay("2015-04-06").businessEnd());

        assertTrue(cal.calendarDay("2015-04-06").isInclusiveEnd());
        assertFalse(cal.calendarDay("2015-04-07").isInclusiveEnd());
    }

    public void testLoad() throws URISyntaxException {
        final String path = Paths
                .get(Objects.requireNonNull(TestBusinessCalendarXMLParser.class.getResource("/PARSER-TEST.calendar"))
                        .toURI())
                .toString();
        final File f = new File(path);
        final BusinessCalendar cal = BusinessCalendarXMLParser.loadBusinessCalendar(f);
        assertParserTestCal(cal);
    }

    public static void assertLegacyCal(final BusinessCalendar cal) {
        assertEquals("JPOSE", cal.name());
        assertNull(cal.description());
        assertEquals(DateTimeUtils.timeZone("Asia/Tokyo"), cal.timeZone());
        assertEquals(LocalDate.of(2006, 1, 2), cal.firstValidDate());
        assertEquals(LocalDate.of(2022, 11, 23), cal.lastValidDate());
        assertEquals(2, cal.weekendDays().size());
        assertEquals(LocalTime.of(9, 0), cal.standardBusinessDay().businessStart());
        assertEquals(LocalTime.of(15, 0), cal.standardBusinessDay().businessEnd());
        assertEquals(LocalTime.of(9, 0), cal.standardBusinessDay().businessTimeRanges().get(0).start());
        assertEquals(LocalTime.of(11, 30), cal.standardBusinessDay().businessTimeRanges().get(0).end());
        assertEquals(LocalTime.of(12, 30), cal.standardBusinessDay().businessTimeRanges().get(1).start());
        assertEquals(LocalTime.of(15, 0), cal.standardBusinessDay().businessTimeRanges().get(1).end());
        assertTrue(cal.weekendDays().contains(DayOfWeek.SATURDAY));
        assertTrue(cal.weekendDays().contains(DayOfWeek.SUNDAY));
        assertEquals(156, cal.holidays().size());
        assertTrue(cal.holidays().containsKey(LocalDate.of(2006, 1, 3)));
        assertTrue(cal.holidays().containsKey(LocalDate.of(2007, 12, 23)));

        final CalendarDay<Instant> halfDay = cal.calendarDay("2007-12-28");
        assertEquals(1, halfDay.businessTimeRanges().size());
        assertEquals(DateTimeUtils.parseInstant("2007-12-28T09:00 Asia/Tokyo"), halfDay.businessStart());
        assertEquals(DateTimeUtils.parseInstant("2007-12-28T11:30 Asia/Tokyo"), halfDay.businessEnd());
    }

    public void testLoadLegacy() throws URISyntaxException {
        final String path = Paths
                .get(Objects.requireNonNull(TestBusinessCalendarXMLParser.class.getResource("/LEGACY.calendar"))
                        .toURI())
                .toString();
        final File f = new File(path);
        final BusinessCalendar cal = BusinessCalendarXMLParser.loadBusinessCalendar(f);
        assertLegacyCal(cal);
    }

}
