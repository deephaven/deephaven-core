package io.deephaven.util.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.tables.utils.DBTimeUtils;
import junit.framework.TestCase;

public class TestBusinessPeriod extends BaseArrayTestCase {

    public void testBusinessPeriod() {
        final DBDateTime open1 = DBTimeUtils.convertDateTime("2017-03-11T10:00:00.000000000 NY");
        final DBDateTime close1 = DBTimeUtils.convertDateTime("2017-03-11T11:00:00.000000000 NY");

        try {
            new BusinessPeriod(null, close1);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("null"));
        }

        try {
            new BusinessPeriod(close1, null);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("null"));
        }

        try {
            new BusinessPeriod(close1, open1);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("after"));
        }

        BusinessPeriod period = new BusinessPeriod(open1, close1);
        assertEquals(open1, period.getStartTime());
        assertEquals(close1, period.getEndTime());
        assertEquals(DBTimeUtils.HOUR, period.getLength());

        assertTrue(period.contains(open1));
        assertTrue(period.contains(new DBDateTime(open1.getNanos() + DBTimeUtils.MINUTE)));
        assertFalse(period.contains(new DBDateTime(open1.getNanos() - DBTimeUtils.MINUTE)));
        assertTrue(period.contains(close1));
        assertTrue(period.contains(new DBDateTime(close1.getNanos() - DBTimeUtils.MINUTE)));
        assertFalse(period.contains(new DBDateTime(close1.getNanos() + DBTimeUtils.MINUTE)));
    }
}
