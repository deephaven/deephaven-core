/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.errorbar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.plot.BaseFigureImpl;
import io.deephaven.plot.ChartImpl;
import io.deephaven.plot.datasets.xyerrorbar.XYErrorBarDataSeriesInternal;
import io.deephaven.plot.datasets.xyerrorbar.XYErrorBarDataSeriesTableArray;
import io.deephaven.plot.util.tables.TableHandle;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TstUtils;

import static io.deephaven.engine.table.impl.TstUtils.*;
import static io.deephaven.engine.table.impl.TstUtils.i;

public class XYErrorBarDataSeriesTableArrayTest extends BaseArrayTestCase {

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

    public void testXYErrorBarDataSeriesTableArray() {
        final BaseFigureImpl figure = new BaseFigureImpl();
        final ChartImpl chart = figure.newChart();
        final double[] dataX = new double[500];
        final double[] dataY = new double[dataX.length];
        final double[] dataYLow = new double[dataX.length];
        final double[] dataYHigh = new double[dataX.length];
        for (int i = 0; i < dataX.length; i++) {
            dataX[i] = i;
            dataY[i] = 2 * i;
            dataYLow[i] = dataY[i] - 10;
            dataYHigh[i] = dataY[i] + 10;
        }
        dataY[0] = Double.NaN;
        dataYLow[0] = Double.NaN;
        dataYHigh[0] = Double.NaN;
        dataX[10] = Double.NaN;
        dataX[100] = Double.NaN;
        Table t = TableTools.newTable(TableTools.doubleCol("x", dataX),
                TableTools.doubleCol("y", dataY),
                TableTools.doubleCol("yLow", dataYLow),
                TableTools.doubleCol("yHigh", dataYHigh));

        final TableHandle h = new TableHandle(t, "x", "y", "yLow", "yHigh");
        final XYErrorBarDataSeriesTableArray series = new XYErrorBarDataSeriesTableArray(chart.newAxes(), 1, "Test", h,
                "x", null, null, "y", "yLow", "yHigh", false, true);

        assertEquals(series.getX(0), 0.0);
        assertEquals(series.getX(5), 5.0);
        assertEquals(series.getX(10), Double.NaN);
        assertEquals(series.getX(25), 25.0);

        assertEquals(series.getY(0), Double.NaN);
        assertEquals(series.getY(5), 10.0);
        assertEquals(series.getY(10), 20.0);
        assertEquals(series.getY(25), 50.0);

        assertEquals(series.getStartY(0), -10 + series.getY(0));
        assertEquals(series.getStartY(5), -10 + series.getY(5));
        assertEquals(series.getStartY(55), -10 + series.getY(55));
        assertEquals(series.getStartY(100), -10 + series.getY(100));

        assertEquals(series.getEndY(0), 10 + series.getY(0));
        assertEquals(series.getEndY(5), 10 + series.getY(5));
        assertEquals(series.getEndY(55), 10 + series.getY(55));
        assertEquals(series.getEndY(100), 10 + series.getY(100));
    }

    public void testRefreshingTable() {
        final BaseFigureImpl figure = new BaseFigureImpl();
        final ChartImpl chart = figure.newChart();

        final QueryTable refreshingTable = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                c("x", 1, 2, 3), c("y", 1, 2, 3), c("yLow", 0, 1, 2), c("yHigh", 11, 22, 33));

        final TableHandle h = new TableHandle(refreshingTable, "x", "y", "yLow", "yHigh");
        final XYErrorBarDataSeriesTableArray series = new XYErrorBarDataSeriesTableArray(chart.newAxes(), 1, "Test", h,
                "x", null, null, "y", "yLow", "yHigh", false, true);


        assertEquals(series.getX(4), Double.NaN);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(refreshingTable, i(7, 9), c("x", 4, 5), c("y", 4, 5), c("yLow", 3, 4), c("yHigh", 5, 6));
            refreshingTable.notifyListeners(i(7, 9), i(), i());
        });

        assertEquals(5.0, series.getX(4));
        assertEquals(4.0, series.getStartY(4));
        assertEquals(6.0, series.getEndY(4));
    }

    public void testCopy() {
        final BaseFigureImpl figure = new BaseFigureImpl();
        final ChartImpl chart = figure.newChart();
        final double[] dataX = new double[500];
        final double[] dataY = new double[dataX.length];
        final double[] dataYLow = new double[dataX.length];
        final double[] dataYHigh = new double[dataX.length];
        for (int i = 0; i < dataX.length; i++) {
            dataX[i] = i;
            dataY[i] = 2 * i;
            dataYLow[i] = dataY[i] - 10;
            dataYHigh[i] = dataY[i] + 10;
        }
        dataY[0] = Double.NaN;
        dataYLow[0] = Double.NaN;
        dataYHigh[0] = Double.NaN;
        dataX[10] = Double.NaN;
        dataX[100] = Double.NaN;
        Table t = TableTools.newTable(TableTools.doubleCol("x", dataX),
                TableTools.doubleCol("y", dataY),
                TableTools.doubleCol("yLow", dataYLow),
                TableTools.doubleCol("yHigh", dataYHigh));

        final TableHandle h = new TableHandle(t, "x", "y", "yLow", "yHigh");
        XYErrorBarDataSeriesTableArray series = new XYErrorBarDataSeriesTableArray(chart.newAxes(), 1, "Test", h, "x",
                null, null, "y", "yLow", "yHigh", false, true);
        XYErrorBarDataSeriesTableArray copy = series.copy(chart.newAxes());

        series.size();
        copy.size();

        testCopy(series, copy);

    }

    private void testCopy(final XYErrorBarDataSeriesTableArray series, final XYErrorBarDataSeriesInternal copy) {
        assertEquals(series.getX(0), copy.getX(0));
        assertEquals(series.getX(5), copy.getX(5));
        assertEquals(series.getX(10), copy.getX(10));

        assertEquals(series.getStartX(0), copy.getStartX(0));
        assertEquals(series.getStartX(5), copy.getStartX(5));
        assertEquals(series.getStartX(10), copy.getStartX(10));

        assertEquals(series.getEndX(0), copy.getEndX(0));
        assertEquals(series.getEndX(5), copy.getEndX(5));
        assertEquals(series.getEndX(10), copy.getEndX(10));

        assertEquals(series.getY(0), copy.getY(0));
        assertEquals(series.getY(5), copy.getY(5));
        assertEquals(series.getY(10), copy.getY(10));

        assertEquals(series.getStartY(0), copy.getStartY(0));
        assertEquals(series.getStartY(5), copy.getStartY(5));
        assertEquals(series.getStartY(10), copy.getStartY(10));

        assertEquals(series.getEndY(0), copy.getEndY(0));
        assertEquals(series.getEndY(5), copy.getEndY(5));
        assertEquals(series.getEndY(10), copy.getEndY(10));
    }
}
