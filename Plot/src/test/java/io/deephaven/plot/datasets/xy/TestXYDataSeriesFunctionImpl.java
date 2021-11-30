/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.xy;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.plot.BaseFigureImpl;
import io.deephaven.plot.errors.PlotIllegalArgumentException;
import junit.framework.TestCase;

public class TestXYDataSeriesFunctionImpl extends BaseArrayTestCase {

    public void testXYDataSeriesFunction() {
        final XYDataSeriesFunctionImpl f1 = new BaseFigureImpl().newChart().newAxes().plot("Test", x -> x);
        final XYDataSeriesFunctionImpl f2 = new BaseFigureImpl().newChart().newAxes().plot("Test", Math::log);
        final XYDataSeriesFunctionImpl f3 = new BaseFigureImpl().newChart().newAxes().plot("Test", x -> Double.NaN);

        assertEquals(f1.size(), 0);
        assertEquals(f2.size(), 0);
        assertEquals(f3.size(), 0);

        f1.funcRange(1, 100, 100);
        f2.funcRange(1, 100, 100);
        f3.funcRange(1, 100, 100);

        assertEquals(f1.size(), 100);
        assertEquals(f2.size(), 100);
        assertEquals(f3.size(), 100);
        assertEquals(f1.getX(50), 51.0);
        assertEquals(f2.getX(50), 51.0);
        assertEquals(f3.getX(50), 51.0);
        assertEquals(f1.getY(50), 51.0);
        assertEquals(f2.getY(50), Math.log(51));
        assertEquals(f3.getY(50), Double.NaN);

        f1.funcRange(0, 99);
        f2.funcRange(0, 99);
        f3.funcRange(0, 99);


        assertEquals(f1.size(), 101);
        assertEquals(f2.size(), 101);
        assertEquals(f3.size(), 101);
        assertEquals(f1.getX(50), 50.0);
        assertEquals(f2.getX(50), 50.0);
        assertEquals(f3.getX(50), 50.0);
        assertEquals(f1.getY(50), 50.0);
        assertEquals(f2.getY(50), Math.log(50));
        assertEquals(f3.getY(50), Double.NaN);

        try {
            f1.funcRange(100, 99, -10);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("xmin"));
        }

        try {
            f1.funcRange(0, 99, -10);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("0"));
        }


        try {
            f1.funcNPoints(-6);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("0"));
        }

        f1.funcNPoints(16);
        assertEquals(1.0, f1.getX(1));
        assertEquals(1.0, f1.getY(1));


        try {
            new XYDataSeriesFunctionImpl(new BaseFigureImpl().newChart().newAxes(), 1, "Test", null);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }

    }

    public void testCopy() {
        final XYDataSeriesFunctionImpl f1 = new BaseFigureImpl().newChart().newAxes().plot("Test", x -> x);
        testFunction(f1, f1.copy(new BaseFigureImpl().newChart().newAxes()));


        final XYDataSeriesFunctionImpl f2 = new BaseFigureImpl().newChart().newAxes().plot("Test2", Math::log);
        f2.pointsVisible(false);
        f2.linesVisible(true);
        f2.pointLabelFormat("{0}: {1}, {2}");
        f2.xToolTipPattern("0.0E0");
        f2.yToolTipPattern("0.0E1");
        f2.seriesColor("blue");
        f2.lineColor("red");
        f2.pointSize(0.5, 4.2, 3.0);
        testFunction(f2, f2.copy(new BaseFigureImpl().newChart().newAxes()));

        final XYDataSeriesFunctionImpl f3 = new BaseFigureImpl().newChart().newAxes().plot("Test", x -> Double.NaN);
        f3.funcNPoints(1);
        testFunction(f3, f3.copy(new BaseFigureImpl().newChart().newAxes()));
    }

    private void testFunction(final XYDataSeriesFunctionImpl original, final XYDataSeriesFunctionImpl copy) {
        TestAbstractXYDataSeries.testCopy(original, copy);
    }

}
