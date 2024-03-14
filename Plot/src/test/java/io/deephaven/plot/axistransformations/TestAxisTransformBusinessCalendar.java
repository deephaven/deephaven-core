//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.axistransformations;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.calendar.CalendarInit;
import io.deephaven.time.calendar.Calendars;
import junit.framework.TestCase;

import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;

public class TestAxisTransformBusinessCalendar extends TestCase {

    private static final ZoneId TZ_JP = ZoneId.of("Asia/Tokyo");

    private AxisTransformBusinessCalendar bt;

    private final Instant holiday = DateTimeUtils.parseInstant("2017-01-03T10:00:00 JP");
    private final Instant weekend = DateTimeUtils.parseInstant("2017-01-02T10:00:00 JP");

    private final Instant pre1 = DateTimeUtils.parseInstant("2017-01-04T06:00:00 JP");
    private final Instant bus11 = DateTimeUtils.parseInstant("2017-01-04T10:00:00 JP");
    private final Instant lunch1 = DateTimeUtils.parseInstant("2017-01-04T12:00:00 JP");
    private final Instant bus12 = DateTimeUtils.parseInstant("2017-01-04T12:45:00 JP");
    private final Instant close1 = DateTimeUtils.parseInstant("2017-01-04T20:00:00 JP");

    private final Instant pre2 = DateTimeUtils.parseInstant("2017-01-06T06:00:00 JP");
    private final Instant bus21 = DateTimeUtils.parseInstant("2017-01-06T10:00:00 JP");
    private final Instant lunch2 = DateTimeUtils.parseInstant("2017-01-06T12:00:00 JP");
    private final Instant bus22 = DateTimeUtils.parseInstant("2017-01-06T12:45:00 JP");
    private final Instant close2 = DateTimeUtils.parseInstant("2017-01-06T20:00:00 JP");

    private final Instant pre3 = DateTimeUtils.parseInstant("2017-01-11T06:00:00 JP");
    private final Instant bus31 = DateTimeUtils.parseInstant("2017-01-11T10:00:00 JP");
    private final Instant lunch3 = DateTimeUtils.parseInstant("2017-01-11T12:00:00 JP");
    private final Instant bus32 = DateTimeUtils.parseInstant("2017-01-11T12:45:00 JP");
    private final Instant close3 = DateTimeUtils.parseInstant("2017-01-11T20:00:00 JP");

    @Override
    public void setUp() throws Exception {
        CalendarInit.init();
        final String path = Paths
                .get(Objects.requireNonNull(TestAxisTransformBusinessCalendar.class.getResource("/JPOSE.calendar"))
                        .toURI())
                .toString();
        final String calName = "PARSER_TEST_CAL";

        Calendars.addCalendarFromFile(path);
        bt = new AxisTransformBusinessCalendar(Calendars.calendar("JPOSE"));
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        Calendars.removeCalendar("JPOSE");
        super.tearDown();
    }

    public void testIsVisible() {
        assertFalse(bt.isVisible((double) DateTimeUtils.epochNanos(holiday)));
        assertFalse(bt.isVisible((double) DateTimeUtils.epochNanos(weekend)));
        assertFalse(bt.isVisible((double) DateTimeUtils.epochNanos(pre1)));
        assertTrue(bt.isVisible((double) DateTimeUtils.epochNanos(bus11)));
        assertFalse(bt.isVisible((double) DateTimeUtils.epochNanos(lunch1)));
        assertTrue(bt.isVisible((double) DateTimeUtils.epochNanos(bus12)));
        assertFalse(bt.isVisible((double) DateTimeUtils.epochNanos(close1)));

        assertFalse(bt.isVisible((double) DateTimeUtils.epochNanos(pre2)));
        assertTrue(bt.isVisible((double) DateTimeUtils.epochNanos(bus21)));
        assertFalse(bt.isVisible((double) DateTimeUtils.epochNanos(lunch2)));
        assertTrue(bt.isVisible((double) DateTimeUtils.epochNanos(bus22)));
        assertFalse(bt.isVisible((double) DateTimeUtils.epochNanos(close2)));

        assertFalse(bt.isVisible((double) DateTimeUtils.epochNanos(pre3)));
        assertTrue(bt.isVisible((double) DateTimeUtils.epochNanos(bus31)));
        assertFalse(bt.isVisible((double) DateTimeUtils.epochNanos(lunch3)));
        assertTrue(bt.isVisible((double) DateTimeUtils.epochNanos(bus32)));
        assertFalse(bt.isVisible((double) DateTimeUtils.epochNanos(close3)));
    }

    public void testTransform() {

        testTransform(holiday, DateTimeUtils.parseInstant("2017-01-04T09:00:00 JP"));
        testTransform(weekend, DateTimeUtils.parseInstant("2017-01-04T09:00:00 JP"));

        testTransform(pre1, DateTimeUtils.parseInstant("2017-01-04T09:00:00 JP"));
        testTransform(pre2, DateTimeUtils.parseInstant("2017-01-06T09:00:00 JP"));
        testTransform(pre3, DateTimeUtils.parseInstant("2017-01-11T09:00:00 JP"));

        testTransform(lunch1, DateTimeUtils.parseInstant("2017-01-04T11:30:00 JP"));
        testTransform(lunch2, DateTimeUtils.parseInstant("2017-01-06T11:30:00 JP"));
        testTransform(lunch3, DateTimeUtils.parseInstant("2017-01-11T11:30:00 JP"));

        testTransform(close1, DateTimeUtils.parseInstant("2017-01-05T09:00:00 JP"));
        testTransform(close2, DateTimeUtils.parseInstant("2017-01-10T09:00:00 JP"));
        testTransform(close3, DateTimeUtils.parseInstant("2017-01-12T09:00:00 JP"));

        for (Instant t : new Instant[] {bus11, bus12, bus21, bus22, bus31, bus32}) {
            testTransform(t, t);
        }

    }

    // tests bugs where first day was transformed incorrectly
    public void testFirstTransformedDay() {
        AxisTransform transform = new AxisTransformBusinessCalendar(Calendars.calendar("USNYSE_EXAMPLE"));
        double d = transform.transform(DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2018-02-02T09:30:01 NY")));
        double d2 = transform.transform(DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2018-02-02T14:30:01 NY")));
        assertFalse(d == d2);

        // first day holiday
        transform = new AxisTransformBusinessCalendar(Calendars.calendar("USNYSE_EXAMPLE"));
        transform.transform(DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2018-02-03T09:30:01 NY")));
        assertEquals(0.0 + 30 * DateTimeUtils.MINUTE,
                transform.transform(DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2018-02-02T10:00:00 NY"))));
        assertEquals(2.34E13 + 30 * DateTimeUtils.MINUTE,
                transform.transform(DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2018-02-05T10:00:00 NY"))));

        // first time outside business hours
        transform = new AxisTransformBusinessCalendar(Calendars.calendar("USNYSE_EXAMPLE"));
        transform.transform(DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2018-02-02T09:29:00 NY")));
        assertEquals(2.34E13 + 30 * DateTimeUtils.MINUTE,
                transform.transform(DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2018-02-02T10:00:00 NY"))));

        // previous day was holiday
        transform = new AxisTransformBusinessCalendar(Calendars.calendar("USNYSE_EXAMPLE"));
        transform.transform(DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2018-01-29T09:29:00 NY")));
        assertEquals(2 * 2.34E13 + 30 * DateTimeUtils.MINUTE,
                transform.transform(DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2018-01-30T10:00:00 NY"))));
    }

    private void testTransform(final Instant tIn, final Instant tTarget) {
        double v = bt.transform(DateTimeUtils.epochNanos(tIn));
        double t2 = bt.inverseTransform(v);

        assertEquals(DateTimeUtils.formatDateTime(tIn, TZ_JP), (double) DateTimeUtils.epochNanos(tTarget), t2);
    }
}
