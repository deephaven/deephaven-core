package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.time.DateTimeUtils;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.DayOfWeek;
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
}
