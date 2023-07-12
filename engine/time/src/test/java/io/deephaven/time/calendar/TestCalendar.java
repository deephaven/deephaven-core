package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.time.DateTimeUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class TestCalendar extends BaseArrayTestCase {
    protected final String name = "TEST CALENDAR";
    protected final String description = "This is a test";
    protected final ZoneId timeZone = ZoneId.of("America/Los_Angeles");

    protected Calendar calendar;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        calendar = new Calendar(name, description, timeZone);
    }

    public void testGetters(){
        assertEquals(name, calendar.name());
        assertEquals(description, calendar.description());
        assertEquals(timeZone, calendar.timeZone());
    }

    public void testToString() {
        assertEquals("Calendar{name='TEST CALENDAR', description='This is a test', timeZone=America/Los_Angeles}", calendar.toString());
    }

    public void testPlusDays() {
        final LocalDate d = LocalDate.of(2023,2,3);
        final String s = "2023-02-03";
        final ZonedDateTime z = d.atTime(1,24).atZone(timeZone);
        final Instant i = z.toInstant();

        assertEquals(d, calendar.plusDays(d,0));
        assertEquals(d, calendar.plusDays(s,0));
        assertEquals(d, calendar.plusDays(z,0));
        assertEquals(d, calendar.plusDays(i,0));

        final LocalDate d2 = LocalDate.of(2023,2,5);
        assertEquals(d2, calendar.plusDays(d,2));
        assertEquals(d2, calendar.plusDays(s,2));
        assertEquals(d2, calendar.plusDays(z,2));
        assertEquals(d2, calendar.plusDays(i,2));

        final LocalDate d3 = LocalDate.of(2023,2,1);
        assertEquals(d3, calendar.plusDays(d,-2));
        assertEquals(d3, calendar.plusDays(s,-2));
        assertEquals(d3, calendar.plusDays(z,-2));
        assertEquals(d3, calendar.plusDays(i,-2));
    }

    public void testMinusDays() {
        final LocalDate d = LocalDate.of(2023,2,3);
        final String s = "2023-02-03";
        final ZonedDateTime z = d.atTime(1,24).atZone(timeZone);
        final Instant i = z.toInstant();

        assertEquals(d, calendar.minusDays(d,0));
        assertEquals(d, calendar.minusDays(s,0));
        assertEquals(d, calendar.minusDays(z,0));
        assertEquals(d, calendar.minusDays(i,0));

        final LocalDate d2 = LocalDate.of(2023,2,1);
        assertEquals(d2, calendar.minusDays(d,2));
        assertEquals(d2, calendar.minusDays(s,2));
        assertEquals(d2, calendar.minusDays(z,2));
        assertEquals(d2, calendar.minusDays(i,2));

        final LocalDate d3 = LocalDate.of(2023,2,5);
        assertEquals(d3, calendar.minusDays(d,-2));
        assertEquals(d3, calendar.minusDays(s,-2));
        assertEquals(d3, calendar.minusDays(z,-2));
        assertEquals(d3, calendar.minusDays(i,-2));
    }

    public void testCurrentDate() {
        assertEquals(DateTimeUtils.todayDate(), calendar.currentDate());
    }

    public void testFutureDate() {
        assertEquals(calendar.plusDays(DateTimeUtils.todayDate(),3), calendar.futureDate(3));
        assertEquals(calendar.plusDays(DateTimeUtils.todayDate(),-3), calendar.futureDate(-3));
    }

    public void testPastDate() {
        assertEquals(calendar.minusDays(DateTimeUtils.todayDate(),3), calendar.pastDate(3));
        assertEquals(calendar.minusDays(DateTimeUtils.todayDate(),-3), calendar.pastDate(-3));
    }

    public void testCalendarDates() {
        final LocalDate start = LocalDate.of(2023,2,3);
        final LocalDate middle = LocalDate.of(2023,2,4);
        final LocalDate end = LocalDate.of(2023,2,5);

        assertEquals(new LocalDate[]{start, middle, end},calendar.calendarDates(start, end));
        assertEquals(new LocalDate[]{start, middle, end},calendar.calendarDates(start.toString(), end.toString()));
        assertEquals(new LocalDate[]{start, middle, end},calendar.calendarDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone)));
        assertEquals(new LocalDate[]{start, middle, end},calendar.calendarDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant()));

        assertEquals(new LocalDate[]{start, middle},calendar.calendarDates(start, end, true, false));
        assertEquals(new LocalDate[]{start, middle},calendar.calendarDates(start.toString(), end.toString(), true, false));
        assertEquals(new LocalDate[]{start, middle},calendar.calendarDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone), true, false));
        assertEquals(new LocalDate[]{start, middle},calendar.calendarDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant(), true, false));

        assertEquals(new LocalDate[]{middle, end},calendar.calendarDates(start, end, false, true));
        assertEquals(new LocalDate[]{middle, end},calendar.calendarDates(start.toString(), end.toString(), false, true));
        assertEquals(new LocalDate[]{middle, end},calendar.calendarDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone), false, true));
        assertEquals(new LocalDate[]{middle, end},calendar.calendarDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant(), false, true));

        assertEquals(new LocalDate[]{middle},calendar.calendarDates(start, end, false, false));
        assertEquals(new LocalDate[]{middle},calendar.calendarDates(start.toString(), end.toString(), false, false));
        assertEquals(new LocalDate[]{middle},calendar.calendarDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone), false, false));
        assertEquals(new LocalDate[]{middle},calendar.calendarDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant(), false, false));
    }

    public void testNumberCalendarDates() {
        final LocalDate start = LocalDate.of(2023,2,3);
        final LocalDate middle = LocalDate.of(2023,2,4);
        final LocalDate end = LocalDate.of(2023,2,5);

        assertEquals(3,calendar.numberCalendarDates(start, end));
        assertEquals(3,calendar.numberCalendarDates(start.toString(), end.toString()));
        assertEquals(3,calendar.numberCalendarDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone)));
        assertEquals(3,calendar.numberCalendarDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant()));

        assertEquals(2,calendar.numberCalendarDates(start, end, true, false));
        assertEquals(2,calendar.numberCalendarDates(start.toString(), end.toString(), true, false));
        assertEquals(2,calendar.numberCalendarDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone), true, false));
        assertEquals(2,calendar.numberCalendarDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant(), true, false));

        assertEquals(2,calendar.numberCalendarDates(start, end, false, true));
        assertEquals(2,calendar.numberCalendarDates(start.toString(), end.toString(), false, true));
        assertEquals(2,calendar.numberCalendarDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone), false, true));
        assertEquals(2,calendar.numberCalendarDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant(), false, true));

        assertEquals(1,calendar.numberCalendarDates(start, end, false, false));
        assertEquals(1,calendar.numberCalendarDates(start.toString(), end.toString(), false, false));
        assertEquals(1,calendar.numberCalendarDates(start.atTime(1,24).atZone(timeZone), end.atTime(1,24).atZone(timeZone), false, false));
        assertEquals(1,calendar.numberCalendarDates(start.atTime(1,24).atZone(timeZone).toInstant(), end.atTime(1,24).atZone(timeZone).toInstant(), false, false));
    }
}
