/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot;

import io.deephaven.base.testing.BaseArrayTestCase;

/**
 * Test AxesLocation.
 */
public class TestAxesLocation extends BaseArrayTestCase {

    public void testLocation() {
        final BaseFigureImpl fig1 = new BaseFigureImpl(3, 2);
        final ChartImpl c11 = fig1.newChart(2, 1);
        final AxesImpl a11 = c11.newAxes();
        final AxesImpl a12 = c11.newAxes();

        final BaseFigureImpl fig2 = new BaseFigureImpl(3, 2);
        final ChartImpl c21 = fig2.newChart(2, 1);
        final AxesImpl a21 = c21.newAxes();
        final AxesImpl a22 = c21.newAxes();

        assertEquals(a12, new AxesLocation(a12).get(fig1));
        assertEquals(a22, new AxesLocation(a12).get(fig2));

        assertEquals(a11, new AxesLocation(a11).get(fig1));
        assertEquals(a21, new AxesLocation(a11).get(fig2));
    }

}
