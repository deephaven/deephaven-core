/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot;

import io.deephaven.base.testing.BaseArrayTestCase;

/**
 * Test AxisLocation.
 */
public class TestAxisLocation extends BaseArrayTestCase {

    public void testLocation() {
        final BaseFigureImpl fig1 = new BaseFigureImpl(3, 2);
        final ChartImpl c11 = fig1.newChart(2, 1);
        final AxesImpl a11 = c11.newAxes();
        final AxesImpl a12 = c11.newAxes();
        a11.plot("S1", new double[] {1, 2, 3}, new double[] {4, 5, 6});
        a12.plot("S2", new double[] {1, 2, 3}, new double[] {4, 5, 6});
        final AxisImpl ax11 = a11.axis(0);
        final AxisImpl ax12 = a12.axis(1);

        final BaseFigureImpl fig2 = new BaseFigureImpl(3, 2);
        final ChartImpl c21 = fig2.newChart(2, 1);
        final AxesImpl a21 = c21.newAxes();
        final AxesImpl a22 = c21.newAxes();
        a21.plot("S1", new double[] {1, 2, 3}, new double[] {4, 5, 6});
        a22.plot("S2", new double[] {1, 2, 3}, new double[] {4, 5, 6});
        final AxisImpl ax21 = a21.axis(0);
        final AxisImpl ax22 = a22.axis(1);

        assertEquals(ax11, new AxisLocation(ax11).get(fig1));
        assertEquals(ax21, new AxisLocation(ax11).get(fig2));

        assertEquals(ax12, new AxisLocation(ax12).get(fig1));
        assertEquals(ax22, new AxisLocation(ax12).get(fig2));
    }

}
