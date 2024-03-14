//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.datasets.category;

import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.plot.*;
import io.deephaven.plot.util.PlotUtils;
import io.deephaven.plot.util.tables.TableHandle;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

public class TestCategoryDataSeriesPartitionedTable {

    @Rule
    final public EngineCleanup framework = new EngineCleanup();

    @Test
    public void testCopy() {
        final BaseFigureImpl figure = new BaseFigureImpl();
        final ChartImpl chart = figure.newChart();
        final String[] dataX = new String[500];
        final double[] dataY = new double[dataX.length];
        for (int i = 0; i < dataX.length; i++) {
            dataX[i] = i + "";
            dataY[i] = i % 10 == 0 ? Double.NaN : 2 * i;
        }

        Table t = TableTools.newTable(TableTools.col("x", dataX),
                TableTools.doubleCol("y", dataY));

        final TableHandle h = PlotUtils.createCategoryTableHandle(t, "x", "y");
        final CategoryDataSeriesPartitionedTable series =
                new CategoryDataSeriesPartitionedTable(chart.newAxes(), 1, "Test", h, "x", "y");
        final CategoryDataSeriesPartitionedTable copy = series.copy(new BaseFigureImpl().newChart().newAxes());

        series.size();
        copy.size();

        testCopy(series, copy);
        assertNull(copy.getValue(dataX.length));

    }

    private void testCopy(final CategoryDataSeriesPartitionedTable series, final AbstractCategoryDataSeries copy) {
        assertEquals(series.getValue("0"), copy.getValue("0"));
        assertEquals(series.getValue("5"), copy.getValue("5"));
        assertEquals(series.getValue("55"), copy.getValue("55"));
    }
}
