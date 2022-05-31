/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.gui.color.Color;
import junit.framework.TestCase;

public class TestBaseFigureImpl extends BaseArrayTestCase {
    @Override
    public void setUp() throws Exception {
        super.setUp();
        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(true);
    }

    public void testSubplot() {
        BaseFigureImpl fig1 = new BaseFigureImpl();

        fig1.updateInterval(1000L);

        ChartImpl c1 = fig1.newChart();
        ChartImpl c2 = fig1.newChart();
        c1.setChartType(ChartType.XY);
        c2.setChartType(ChartType.XY);
        Axes az = c1.newAxes();
        Axes az2 = c2.newAxes();
        az.plot("Test1", x -> x);
        az2.plot("Test2", x -> x);

        try {
            az.plot("Test1", x -> x);
            az2.plot("Test2", x -> x);
            fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("same name"));
        }

        fig1.figureRemoveSeries("Test1", "Test2");
        az.plot("Test1", x -> x);
        az2.plot("Test2", x -> x);

        try {
            new BaseFigureImpl(0, 0);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Grid must be at least 1x1"));
        }

        fig1 = new BaseFigureImpl(5, 5);
        fig1.newChart(0);
        fig1.newChart(0, 0);

        fig1.removeChart(0);

        try {
            fig1.removeChart(0);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("exist"));
        }

        Chart c = fig1.newChart(0);


        try {
            c.rowSpan(100);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("not in grid"));
        }

        try {
            fig1.newChart(100);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("not in grid"));
        }

        try {
            fig1.newChart(-1);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("indices must be"));
        }

        fig1 = new BaseFigureImpl(2, 2);
        c1 = fig1.newChart(1, 0);
        fig1.newChart(0, 0);
        c2 = fig1.newChart(0, 1).span(2, 1);

        try {
            fig1.newChart();
            fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("No open space"));
        }

        try {
            c2.colSpan(2);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("not in grid"));
        }

        try {
            c2.rowSpan(3);
            TestCase.fail("Expected an exception");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("not in grid"));
        }

        c2.rowSpan(1);
        c1.colSpan(2);
    }

    public void testCopy() {
        final BaseFigureImpl figure = new BaseFigureImpl();
        final String title = "TEST";
        final Font font = new Font("Ariel", Font.FontStyle.PLAIN, 10);
        final Color color = new io.deephaven.gui.color.Color(0, 0, 0);
        figure.newChart(0).newAxes().plot("test", new int[] {1}, new int[] {1});

        figure.figureTitle(title);
        figure.figureTitleColor(color);
        figure.figureTitleFont(font);

        final BaseFigureImpl copy = figure.copy();

        assertNotSame(figure, copy);
    }

    public void testNoCharts() {
        final Figure figure = FigureFactory.figure();

        try {
            figure.show();
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("No charts"));
        }

        final Figure c = figure.newChart();

        try {
            figure.show();
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("No plots"));
        }

        c.newAxes();

        try {
            figure.show();
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("No plots"));
        }
    }
}
