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

}
