/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.axistransformations;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.DBTimeZone;
import io.deephaven.util.calendar.Calendars;

public class TestAxisTransformBusinessCalendar extends BaseArrayTestCase {

    private final AxisTransformBusinessCalendar bt = new AxisTransformBusinessCalendar(Calendars.calendar("JPOSE"));

    private final DBDateTime holiday = DBTimeUtils.convertDateTime("2017-01-03T10:00:00 JP");
    private final DBDateTime weekend = DBTimeUtils.convertDateTime("2017-01-02T10:00:00 JP");

    private final DBDateTime pre1 = DBTimeUtils.convertDateTime("2017-01-04T06:00:00 JP");
    private final DBDateTime bus11 = DBTimeUtils.convertDateTime("2017-01-04T10:00:00 JP");
    private final DBDateTime lunch1 = DBTimeUtils.convertDateTime("2017-01-04T12:00:00 JP");
    private final DBDateTime bus12 = DBTimeUtils.convertDateTime("2017-01-04T12:45:00 JP");
    private final DBDateTime close1 = DBTimeUtils.convertDateTime("2017-01-04T20:00:00 JP");

    private final DBDateTime pre2 = DBTimeUtils.convertDateTime("2017-01-06T06:00:00 JP");
    private final DBDateTime bus21 = DBTimeUtils.convertDateTime("2017-01-06T10:00:00 JP");
    private final DBDateTime lunch2 = DBTimeUtils.convertDateTime("2017-01-06T12:00:00 JP");
    private final DBDateTime bus22 = DBTimeUtils.convertDateTime("2017-01-06T12:45:00 JP");
    private final DBDateTime close2 = DBTimeUtils.convertDateTime("2017-01-06T20:00:00 JP");

    private final DBDateTime pre3 = DBTimeUtils.convertDateTime("2017-01-11T06:00:00 JP");
    private final DBDateTime bus31 = DBTimeUtils.convertDateTime("2017-01-11T10:00:00 JP");
    private final DBDateTime lunch3 = DBTimeUtils.convertDateTime("2017-01-11T12:00:00 JP");
    private final DBDateTime bus32 = DBTimeUtils.convertDateTime("2017-01-11T12:45:00 JP");
    private final DBDateTime close3 = DBTimeUtils.convertDateTime("2017-01-11T20:00:00 JP");


    public void testIsVisible() {
        assertFalse(bt.isVisible((double) holiday.getNanos()));
        assertFalse(bt.isVisible((double) weekend.getNanos()));
        assertFalse(bt.isVisible((double) pre1.getNanos()));
        assertTrue(bt.isVisible((double) bus11.getNanos()));
        assertFalse(bt.isVisible((double) lunch1.getNanos()));
        assertTrue(bt.isVisible((double) bus12.getNanos()));
        assertFalse(bt.isVisible((double) close1.getNanos()));

        assertFalse(bt.isVisible((double) pre2.getNanos()));
        assertTrue(bt.isVisible((double) bus21.getNanos()));
        assertFalse(bt.isVisible((double) lunch2.getNanos()));
        assertTrue(bt.isVisible((double) bus22.getNanos()));
        assertFalse(bt.isVisible((double) close2.getNanos()));

        assertFalse(bt.isVisible((double) pre3.getNanos()));
        assertTrue(bt.isVisible((double) bus31.getNanos()));
        assertFalse(bt.isVisible((double) lunch3.getNanos()));
        assertTrue(bt.isVisible((double) bus32.getNanos()));
        assertFalse(bt.isVisible((double) close3.getNanos()));
    }

    public void testTransform() {

        testTransform(holiday, DBTimeUtils.convertDateTime("2017-01-04T09:00:00 JP"));
        testTransform(weekend, DBTimeUtils.convertDateTime("2017-01-04T09:00:00 JP"));

        testTransform(pre1, DBTimeUtils.convertDateTime("2017-01-04T09:00:00 JP"));
        testTransform(pre2, DBTimeUtils.convertDateTime("2017-01-06T09:00:00 JP"));
        testTransform(pre3, DBTimeUtils.convertDateTime("2017-01-11T09:00:00 JP"));

        testTransform(lunch1, DBTimeUtils.convertDateTime("2017-01-04T11:30:00 JP"));
        testTransform(lunch2, DBTimeUtils.convertDateTime("2017-01-06T11:30:00 JP"));
        testTransform(lunch3, DBTimeUtils.convertDateTime("2017-01-11T11:30:00 JP"));

        testTransform(close1, DBTimeUtils.convertDateTime("2017-01-05T09:00:00 JP"));
        testTransform(close2, DBTimeUtils.convertDateTime("2017-01-10T09:00:00 JP"));
        testTransform(close3, DBTimeUtils.convertDateTime("2017-01-12T09:00:00 JP"));

        for (DBDateTime t : new DBDateTime[] {bus11, bus12, bus21, bus22, bus31, bus32}) {
            testTransform(t, t);
        }

    }

    // tests bugs where first day was transformed incorrectly
    public void testFirstTransformedDay() {
        AxisTransform transform = new AxisTransformBusinessCalendar(Calendars.calendar("USNYSE"));
        double d = transform.transform(DBTimeUtils.convertDateTime("2018-02-02T09:30:01 NY").getNanos());
        double d2 = transform.transform(DBTimeUtils.convertDateTime("2018-02-02T14:30:01 NY").getNanos());
        assertFalse(d == d2);

        // first day holiday
        transform = new AxisTransformBusinessCalendar(Calendars.calendar("USNYSE"));
        transform.transform(DBTimeUtils.convertDateTime("2018-02-03T09:30:01 NY").getNanos());
        assertEquals(0.0 + 30 * DBTimeUtils.MINUTE,
                transform.transform(DBTimeUtils.convertDateTime("2018-02-02T10:00:00 NY").getNanos()));
        assertEquals(2.34E13 + 30 * DBTimeUtils.MINUTE,
                transform.transform(DBTimeUtils.convertDateTime("2018-02-05T10:00:00 NY").getNanos()));

        // first time outside business hours
        transform = new AxisTransformBusinessCalendar(Calendars.calendar("USNYSE"));
        transform.transform(DBTimeUtils.convertDateTime("2018-02-02T09:29:00 NY").getNanos());
        assertEquals(2.34E13 + 30 * DBTimeUtils.MINUTE,
                transform.transform(DBTimeUtils.convertDateTime("2018-02-02T10:00:00 NY").getNanos()));

        // previous day was holiday
        transform = new AxisTransformBusinessCalendar(Calendars.calendar("USNYSE"));
        transform.transform(DBTimeUtils.convertDateTime("2018-01-29T09:29:00 NY").getNanos());
        assertEquals(2 * 2.34E13 + 30 * DBTimeUtils.MINUTE,
                transform.transform(DBTimeUtils.convertDateTime("2018-01-30T10:00:00 NY").getNanos()));
    }

    private void testTransform(final DBDateTime tIn, final DBDateTime tTarget) {
        double v = bt.transform(tIn.getNanos());
        double t2 = bt.inverseTransform(v);

        assertEquals(tIn.toString(DBTimeZone.TZ_JP), (double) tTarget.getNanos(), t2);
    }

}
