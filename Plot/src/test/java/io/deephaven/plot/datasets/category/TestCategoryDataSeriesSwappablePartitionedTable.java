/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.datasets.category;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.plot.BaseFigureImpl;
import io.deephaven.util.SafeCloseable;

public class TestCategoryDataSeriesSwappablePartitionedTable extends BaseArrayTestCase {

    private SafeCloseable executionContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        executionContext = TestExecutionContext.createForUnitTests().open();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        executionContext.close();
    }

    public void testCategoryDataSeriesPartitionedTable() {
        final BaseFigureImpl figure = new BaseFigureImpl();
        figure.newChart().newAxes();
        final String[] dataX = new String[500];
        final double[] dataY = new double[dataX.length];
        for (int i = 0; i < dataX.length; i++) {
            dataX[i] = i + "";
            dataY[i] = i % 10 == 0 ? Double.NaN : 2 * i;
        }

        Table t = TableTools.newTable(TableTools.col("x", dataX),
                TableTools.doubleCol("y", dataY));
        t = t.updateView("Cat = `A`");

        // todo test oneClick
    }

    public void testRefreshingTable() {
        final BaseFigureImpl figure = new BaseFigureImpl();
        figure.newChart().newAxes();
        final String[] dataX = new String[500];
        final double[] dataY = new double[dataX.length];
        for (int i = 0; i < dataX.length; i++) {
            dataX[i] = i + "";
            dataY[i] = i % 10 == 0 ? Double.NaN : 2 * i;
        }

        Table t = TableTools.newTable(TableTools.col("x", dataX),
                TableTools.doubleCol("y", dataY));
        t = t.updateView("Cat = `A`");

        // todo test oneClick
    }

    public void testCopy() {
        final BaseFigureImpl figure = new BaseFigureImpl();
        figure.newChart().newAxes();
        final String[] dataX = new String[500];
        final double[] dataY = new double[dataX.length];
        for (int i = 0; i < dataX.length; i++) {
            dataX[i] = i + "";
            dataY[i] = i % 10 == 0 ? Double.NaN : 2 * i;
        }

        Table t = TableTools.newTable(TableTools.col("x", dataX),
                TableTools.doubleCol("y", dataY));
        t = t.updateView("Cat = `A`");

        // todo test oneClick
    }
}
