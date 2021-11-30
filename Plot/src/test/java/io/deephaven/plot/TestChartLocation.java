/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot;

import io.deephaven.base.testing.BaseArrayTestCase;

/**
 * Test ChartLocation.
 */
public class TestChartLocation extends BaseArrayTestCase {

    public void testLocation() {
        final BaseFigureImpl fig1 = new BaseFigureImpl(3, 2);
        final ChartImpl c11 = fig1.newChart(2, 1);

        final BaseFigureImpl fig2 = new BaseFigureImpl(3, 2);
        final ChartImpl c21 = fig2.newChart(2, 1);

        final ChartLocation location = new ChartLocation(c11);

        assertEquals(c11, location.get(fig1));
        assertEquals(c21, location.get(fig2));
    }

}
