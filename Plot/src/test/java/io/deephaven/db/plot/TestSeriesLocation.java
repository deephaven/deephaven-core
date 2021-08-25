/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.db.plot.datasets.multiseries.MultiXYSeries;
import io.deephaven.db.plot.datasets.xy.XYDataSeriesArray;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.TableTools;

/**
 * Test SeriesLocation.
 */
public class TestSeriesLocation extends BaseArrayTestCase {

    public void testLocation() {
        final Table t = TableTools.emptyTable(100).update("A=i%2==0?`A`:`B`", "X=1.0*i", "Y=1.0*i*i");

        final BaseFigureImpl fig1 = new BaseFigureImpl(3, 2);
        final ChartImpl c11 = fig1.newChart(2, 1);
        final AxesImpl a11 = c11.newAxes();
        final AxesImpl a12 = c11.newAxes();
        final XYDataSeriesArray s11 = a11.plot("S1", new double[] {1, 2, 3}, new double[] {4, 5, 6});
        final XYDataSeriesArray s12 = a12.plot("S2", new double[] {1, 2, 3}, new double[] {4, 5, 6});
        final MultiXYSeries ms11 = a11.plotBy("MS1", t, "X", "Y", "A");
        final MultiXYSeries ms12 = a12.plotBy("MS2", t, "X", "Y", "A");
        final AxisImpl ax11 = a11.axis(0);
        final AxisImpl ax12 = a12.axis(1);

        final BaseFigureImpl fig2 = new BaseFigureImpl(3, 2);
        final ChartImpl c21 = fig2.newChart(2, 1);
        final AxesImpl a21 = c21.newAxes();
        final AxesImpl a22 = c21.newAxes();
        final XYDataSeriesArray s21 = a21.plot("S1", new double[] {1, 2, 3}, new double[] {4, 5, 6});
        final XYDataSeriesArray s22 = a22.plot("S2", new double[] {1, 2, 3}, new double[] {4, 5, 6});
        final MultiXYSeries ms21 = a21.plotBy("MS1", t, "X", "Y", "A");
        final MultiXYSeries ms22 = a22.plotBy("MS2", t, "X", "Y", "A");
        final AxisImpl ax21 = a21.axis(0);
        final AxisImpl ax22 = a22.axis(1);

        assertEquals(s11, new SeriesLocation(s11).get(fig1));
        assertEquals(s21, new SeriesLocation(s11).get(fig2));

        assertEquals(s12, new SeriesLocation(s12).get(fig1));
        assertEquals(s22, new SeriesLocation(s12).get(fig2));

        assertEquals(ms11, new SeriesLocation(ms11).get(fig1));
        assertEquals(ms21, new SeriesLocation(ms11).get(fig2));

        assertEquals(ms12, new SeriesLocation(ms12).get(fig1));
        assertEquals(ms22, new SeriesLocation(ms12).get(fig2));
    }

}
