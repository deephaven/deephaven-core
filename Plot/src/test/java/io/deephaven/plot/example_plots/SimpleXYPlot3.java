/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.example_plots;

import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;


public class SimpleXYPlot3 {

    public static void main(String[] args) {
        final Number[] x1 = {1, 2, 3, 4};
        final Number[] y1 = {2, 3, 1, 9};
        final Number[] x2 = {0, 1.5, 4.5};
        final Number[] y2 = {1.3, 3.2, 3.4};
        final Number[] x3 = {10, 11.5, 12.5};
        final Number[] y3 = {11.3, 2.2, 8.4};

        Figure fig = FigureFactory.figure();
        Figure cht = fig.newChart(0)
                .chartTitle("Chart Title")

                .newAxes()
                .xLabel("X1")
                .yLabel("Y1")
                .plot("Test1", x1, y1)

                .newAxes()
                .xLabel("X2")
                .yLabel("Y2")
                .plot("Test2", x2, y2)

                .newAxes()
                .xLabel("X3")
                .yLabel("Y3")
                .plot("Test3", x3, y3);

        ExamplePlotUtils.display(cht);
    }

}
