/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.tables.utils;

import io.deephaven.base.testing.BaseArrayTestCase;

public class TestPeriod extends BaseArrayTestCase {

    public void testAll() throws Exception {
        Period period = new Period("1Dt1H");

        assertEquals("1dT1h", period.toString());

        assertEquals(new org.joda.time.Period("P1dT1h"), period.getJodaPeriod());

        assertTrue(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new Period("t1H");

        assertEquals("T1h", period.toString());

        assertEquals(new org.joda.time.Period("PT1h"), period.getJodaPeriod());

        assertTrue(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new Period("1D");

        assertEquals("1d", period.toString());

        assertEquals(new org.joda.time.Period("P1d"), period.getJodaPeriod());

        assertTrue(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new Period("-1Dt1H");

        assertEquals("-1dT1h", period.toString());

        assertEquals(new org.joda.time.Period("P1dT1h"), period.getJodaPeriod());

        assertFalse(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new Period("-t1H");

        assertEquals("-T1h", period.toString());

        assertEquals(new org.joda.time.Period("PT1h"), period.getJodaPeriod());

        assertFalse(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new Period("-1D");

        assertEquals("-1d", period.toString());

        assertEquals(new org.joda.time.Period("P1d"), period.getJodaPeriod());

        assertFalse(period.isPositive());

    }

}
