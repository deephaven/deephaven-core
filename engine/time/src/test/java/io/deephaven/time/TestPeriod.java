/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.time;

import io.deephaven.base.testing.BaseArrayTestCase;
import junit.framework.TestCase;

public class TestPeriod extends BaseArrayTestCase {

    public void testAll() throws Exception {
        Period period = new Period("1Dt1H");

        TestCase.assertEquals("1dT1h", period.toString());

        TestCase.assertEquals(new org.joda.time.Period("P1dT1h"), period.getJodaPeriod());

        TestCase.assertTrue(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new Period("t1H");

        TestCase.assertEquals("T1h", period.toString());

        TestCase.assertEquals(new org.joda.time.Period("PT1h"), period.getJodaPeriod());

        TestCase.assertTrue(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new Period("1D");

        TestCase.assertEquals("1d", period.toString());

        TestCase.assertEquals(new org.joda.time.Period("P1d"), period.getJodaPeriod());

        TestCase.assertTrue(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new Period("-1Dt1H");

        TestCase.assertEquals("-1dT1h", period.toString());

        TestCase.assertEquals(new org.joda.time.Period("P1dT1h"), period.getJodaPeriod());

        TestCase.assertFalse(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new Period("-t1H");

        TestCase.assertEquals("-T1h", period.toString());

        TestCase.assertEquals(new org.joda.time.Period("PT1h"), period.getJodaPeriod());

        TestCase.assertFalse(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new Period("-1D");

        TestCase.assertEquals("-1d", period.toString());

        TestCase.assertEquals(new org.joda.time.Period("P1d"), period.getJodaPeriod());

        TestCase.assertFalse(period.isPositive());

    }

}
