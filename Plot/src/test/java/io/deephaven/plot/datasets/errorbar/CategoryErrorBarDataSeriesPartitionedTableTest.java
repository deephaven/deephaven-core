/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.datasets.errorbar;

import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.plot.*;
import io.deephaven.plot.datasets.categoryerrorbar.CategoryErrorBarDataSeriesInternal;
import io.deephaven.plot.datasets.categoryerrorbar.CategoryErrorBarDataSeriesPartitionedTable;
import io.deephaven.plot.util.PlotUtils;
import io.deephaven.plot.util.tables.TableHandle;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

public class CategoryErrorBarDataSeriesPartitionedTableTest {

    @Rule
    final public EngineCleanup framework = new EngineCleanup();

    @Test
    public void testCopy() {
        final BaseFigureImpl figure = new BaseFigureImpl();
        final ChartImpl chart = figure.newChart();
        final String[] dataX = new String[500];
        final double[] dataY = new double[dataX.length];
        final double[] dataYLow = new double[dataX.length];
        final double[] dataYHigh = new double[dataX.length];
        for (int i = 0; i < dataX.length; i++) {
            dataX[i] = i + "";
            dataY[i] = 2 * i;
            dataYLow[i] = dataY[i] - 10;
            dataYHigh[i] = dataY[i] + 10;
        }
        dataY[0] = Double.NaN;
        dataYLow[0] = Double.NaN;
        dataYHigh[0] = Double.NaN;
        dataX[10] = null;
        Table t = TableTools.newTable(TableTools.col("x", dataX),
                TableTools.doubleCol("y", dataY),
                TableTools.doubleCol("yLow", dataYLow),
                TableTools.doubleCol("yHigh", dataYHigh));

        final TableHandle h = PlotUtils.createCategoryTableHandle(t, "x", "y", "yLow", "yHigh");
        final CategoryErrorBarDataSeriesPartitionedTable series = new CategoryErrorBarDataSeriesPartitionedTable(
                chart.newAxes(), 1, "Test", h, "x", "y", "yLow", "yHigh");
        final CategoryErrorBarDataSeriesPartitionedTable copy = series.copy(chart.newAxes());

        series.size();
        copy.size();

        testCopy(series, copy);
        assertNull(copy.getValue(dataX.length));

    }

    private void testCopy(final CategoryErrorBarDataSeriesPartitionedTable series,
            final CategoryErrorBarDataSeriesInternal copy) {
        assertEquals(series.getValue("0"), copy.getValue("0"));
        assertEquals(series.getValue("5"), copy.getValue("5"));
        assertEquals(series.getValue("55"), copy.getValue("55"));
    }
}
