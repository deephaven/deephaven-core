/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.base.testing.BaseArrayTestCase;
import org.joda.time.*;

public class TestDBPeriod extends BaseArrayTestCase {

    public void testAll() throws Exception {
        DBPeriod period = new DBPeriod("1Dt1H");

        assertEquals("1dT1h", period.toString());

        assertEquals(new Period("P1dT1h"), period.getJodaPeriod());

        assertTrue(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new DBPeriod("t1H");

        assertEquals("T1h", period.toString());

        assertEquals(new Period("PT1h"), period.getJodaPeriod());

        assertTrue(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new DBPeriod("1D");

        assertEquals("1d", period.toString());

        assertEquals(new Period("P1d"), period.getJodaPeriod());

        assertTrue(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new DBPeriod("-1Dt1H");

        assertEquals("-1dT1h", period.toString());

        assertEquals(new Period("P1dT1h"), period.getJodaPeriod());

        assertFalse(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new DBPeriod("-t1H");

        assertEquals("-T1h", period.toString());

        assertEquals(new Period("PT1h"), period.getJodaPeriod());

        assertFalse(period.isPositive());

        // ------------------------------------------------------------------------------------------------------------------------------------------------------------------

        period = new DBPeriod("-1D");

        assertEquals("-1d", period.toString());

        assertEquals(new Period("P1d"), period.getJodaPeriod());

        assertFalse(period.isPositive());

    }

}
