/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time;

import io.deephaven.base.testing.BaseArrayTestCase;
import junit.framework.TestCase;

public class TestPeriod extends BaseArrayTestCase {

    public void testAll() throws Exception {
        Period period = new Period("1Dt1H");

        TestCase.assertEquals("1dT1h", period.toString());

        TestCase.assertEquals(java.time.Duration.parse("P1dT1h"), period.getDuration());

        TestCase.assertTrue(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new Period("t1H");

        TestCase.assertEquals("T1h", period.toString());

        TestCase.assertEquals(java.time.Duration.parse("PT1h"), period.getDuration());

        TestCase.assertTrue(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new Period("1D");

        TestCase.assertEquals("1d", period.toString());

        TestCase.assertEquals(java.time.Duration.parse("P1d"), period.getDuration());

        TestCase.assertTrue(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new Period("-1Dt1H");

        TestCase.assertEquals("-1dT1h", period.toString());

        TestCase.assertEquals(java.time.Duration.parse("P1dT1h"), period.getDuration());

        TestCase.assertFalse(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new Period("-t1H");

        TestCase.assertEquals("-T1h", period.toString());

        TestCase.assertEquals(java.time.Duration.parse("PT1h"), period.getDuration());

        TestCase.assertFalse(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new Period("-1D");

        TestCase.assertEquals("-1d", period.toString());

        TestCase.assertEquals(java.time.Duration.parse("P1d"), period.getDuration());

        TestCase.assertFalse(period.isPositive());

    }

    public void testEqualsCompare() {
        Period p1 = new Period("2D");
        Period p2 = new Period("1D");
        Period p3 = new Period("-1D");
        Period p4 = new Period("-2D");
        Period p1_2 = new Period("2D");

        assertEquals(p1, p1);
        assertEquals(p1, p1_2);
        assertFalse(p1.equals(p2));
        assertEquals(1, p1.compareTo(p2));
        assertEquals(-1, p2.compareTo(p1));
        assertEquals(0, p1.compareTo(p1));
        assertEquals(0, p1.compareTo(p1_2));
        assertEquals(1, p3.compareTo(p4));
        assertEquals(-1, p4.compareTo(p3));
        assertEquals(1, p1.compareTo(p4));
        assertEquals(-1, p4.compareTo(p1));
    }

}
