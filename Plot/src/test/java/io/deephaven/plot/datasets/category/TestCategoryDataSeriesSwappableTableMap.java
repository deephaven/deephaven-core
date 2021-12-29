/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.category;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.plot.BaseFigureImpl;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;

public class TestCategoryDataSeriesSwappableTableMap extends BaseArrayTestCase {

    @Override
    public void setUp() throws Exception {
        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
    }

    public void testCategoryDataSeriesTableMap() {
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
