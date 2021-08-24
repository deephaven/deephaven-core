/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;


public class SimpleXYPlot2b {

    public static void main(String[] args) {
        final Number[] x1 = {1, 2, 3, 4};
        final Number[] y1 = {2, 3, 1, 9};
        final Number[] x2 = {10, 11.5, 14.5};
        final Number[] y2 = {1.3, 123.2, 1223.4};

        Figure fig = FigureFactory.figure();
        Figure cht = fig.newChart(0)
            .chartTitle("Chart Title")

            .newAxes()
            .xLabel("X1")
            .yLabel("Y1")
            .xLog()
            .plot("Test1", x1, y1)

            .newAxes()
            .xLabel("X2")
            .yLabel("Y2")
            .yLog()
            .plot("Test2", x2, y2);

        ExamplePlotUtils.display(cht);
    }

}
