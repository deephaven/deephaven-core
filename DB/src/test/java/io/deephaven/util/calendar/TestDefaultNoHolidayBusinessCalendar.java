package io.deephaven.util.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.DBTimeZone;

import java.io.File;
import java.io.FileWriter;

public class TestDefaultNoHolidayBusinessCalendar extends BaseArrayTestCase {

    private BusinessCalendar noNonBusinessDays;
    private BusinessCalendar onlyWeekends;
    private BusinessCalendar onlyHolidays;
    private BusinessCalendar weekendsAndHolidays;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final File noNonBusinessDaysFile = File.createTempFile("noNonBusinessDays", ".calendar");
        FileWriter fw = new FileWriter(noNonBusinessDaysFile);
        fw.write("<!--\n" +
            "  ~ Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending\n" +
            "  -->\n" +
            "\n" +
            "<calendar>\n" +
            "    <name>noNonBusinessDays</name>\n" +
            "    <timeZone>TZ_NY</timeZone>\n" +
            "    <language>en</language>\n" +
            "    <country>US</country>\n" +
            "    <default>\n" +
            "        <businessPeriod>09:30,16:00</businessPeriod>\n" +
            "    </default>\n" +
            "</calendar>");
        fw.flush();
        fw.close();

        noNonBusinessDays = DefaultBusinessCalendar.getInstance(noNonBusinessDaysFile);

        final File onlyWeekendsFile = File.createTempFile("onlyWeekends", ".calendar");
        fw = new FileWriter(onlyWeekendsFile);
        fw.write("<!--\n" +
            "  ~ Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending\n" +
            "  -->\n" +
            "\n" +
            "<calendar>\n" +
            "    <name>onlyWeekends</name>\n" +
            "    <timeZone>TZ_NY</timeZone>\n" +
            "    <language>en</language>\n" +
            "    <country>US</country>\n" +
            "    <default>\n" +
            "        <businessPeriod>09:30,16:00</businessPeriod>\n" +
            "        <weekend>Saturday</weekend>\n" +
            "        <weekend>Sunday</weekend>\n" +
            "    </default>\n" +
            "</calendar>");
        fw.flush();
        fw.close();

        onlyWeekends = DefaultBusinessCalendar.getInstance(onlyWeekendsFile);

        final File onlyHolidaysFile = File.createTempFile("onlyHolidays", ".calendar");
        fw = new FileWriter(onlyHolidaysFile);
        fw.write("<!--\n" +
            "  ~ Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending\n" +
            "  -->\n" +
            "\n" +
            "<calendar>\n" +
            "    <name>onlyHolidays</name>\n" +
            "    <timeZone>TZ_NY</timeZone>\n" +
            "    <language>en</language>\n" +
            "    <country>US</country>\n" +
            "    <default>\n" +
            "        <businessPeriod>09:30,16:00</businessPeriod>\n" +
            "    </default>\n" +
            "    <holiday>\n" +
            "        <date>20210215</date>\n" +
            "    </holiday>" +
            "</calendar>");
        fw.flush();
        fw.close();

        onlyHolidays = DefaultBusinessCalendar.getInstance(onlyWeekendsFile);

        final File weekendsAndHolidaysFile =
            File.createTempFile("weekendsAndHolidays", ".calendar");
        fw = new FileWriter(weekendsAndHolidaysFile);
        fw.write("<!--\n" +
            "  ~ Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending\n" +
            "  -->\n" +
            "\n" +
            "<calendar>\n" +
            "    <name>weekendsAndHolidays</name>\n" +
            "    <timeZone>TZ_NY</timeZone>\n" +
            "    <language>en</language>\n" +
            "    <country>US</country>\n" +
            "    <default>\n" +
            "        <businessPeriod>09:30,16:00</businessPeriod>\n" +
            "        <weekend>Saturday</weekend>\n" +
            "        <weekend>Sunday</weekend>\n" +
            "    </default>\n" +
            "    <holiday>\n" +
            "        <date>20210215</date>\n" +
            "    </holiday>" +
            "</calendar>");
        fw.flush();
        fw.close();

        weekendsAndHolidays = DefaultBusinessCalendar.getInstance(weekendsAndHolidaysFile);
    }

    public void testInstance() {
        assertTrue(noNonBusinessDays instanceof DefaultNoHolidayBusinessCalendar);
        assertFalse(onlyHolidays instanceof DefaultNoHolidayBusinessCalendar);
        assertFalse(onlyWeekends instanceof DefaultNoHolidayBusinessCalendar);
        assertFalse(weekendsAndHolidays instanceof DefaultNoHolidayBusinessCalendar);
    }

    public void testNonBusinessDayMethods() {
        try {
            noNonBusinessDays.previousNonBusinessDay();
            fail();
        } catch (UnsupportedOperationException e) {
            // ok
        }
        try {
            noNonBusinessDays.previousNonBusinessDay(1);
            fail();
        } catch (UnsupportedOperationException e) {
            // ok
        }
        try {
            noNonBusinessDays.previousNonBusinessDay("20190626");
            fail();
        } catch (UnsupportedOperationException e) {
            // ok
        }
        try {
            noNonBusinessDays.nextNonBusinessDay();
            fail();
        } catch (UnsupportedOperationException e) {
            // ok
        }
        try {
            noNonBusinessDays.nextNonBusinessDay(1);
            fail();
        } catch (UnsupportedOperationException e) {
            // ok
        }
        try {
            noNonBusinessDays.nextNonBusinessDay("20190626");
            fail();
        } catch (UnsupportedOperationException e) {
            // ok
        }

        assertEquals(noNonBusinessDays.nonBusinessDaysInRange("2010-01-01", "2019-01-01"),
            new String[0]);
        assertEquals(noNonBusinessDays.diffNonBusinessNanos(
            DBTimeUtils.convertDateTime("2010-01-01T01:00:00.000000000 NY"),
            DBTimeUtils.convertDateTime("2019-01-01T01:00:00.000000000 NY")), 0);
        assertEquals(noNonBusinessDays.numberOfNonBusinessDays("2010-01-01", "2019-01-01"), 0);


        assertEquals(noNonBusinessDays.name(), "noNonBusinessDays");
        assertEquals(noNonBusinessDays.timeZone(), DBTimeZone.TZ_NY);
        assertEquals(noNonBusinessDays.standardBusinessDayLengthNanos(),
            6 * DBTimeUtils.HOUR + (30 * DBTimeUtils.MINUTE));
        assertEquals(noNonBusinessDays.getBusinessSchedule("2019-06-26").getSOBD(),
            onlyWeekends.getBusinessSchedule("2019-06-26").getSOBD());
        assertEquals(noNonBusinessDays.getBusinessSchedule("2019-06-26").getEOBD(),
            onlyWeekends.getBusinessSchedule("2019-06-26").getEOBD());
    }
}
