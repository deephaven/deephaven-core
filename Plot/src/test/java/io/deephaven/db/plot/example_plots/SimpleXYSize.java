/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;


public class SimpleXYSize {

    public static void main(String[] args) {

        final Number[] x1 = {1, 2, 3, 4};
        final Number[] y1 = {2, 3, 1, 9};
        final double[] s1 = new double[4];
        s1[0] = 1.0;
        s1[2] = 2.0;
        s1[3] = 2.0;

        final Number[] x2 = {0, 1.5, 4.5};
        final Number[] y2 = {1.3, 3.2, 3.4};

        final Number[] x3 = {0, 1, 2};
        final Number[] y3 = {1.5, 2.8, 4.5};

        Figure fig = FigureFactory.figure();
        Figure cht = fig.newChart(0)
                .chartTitle("Chart Title");
        Figure axs = cht.newAxes()
                .xLabel("X")
                .yLabel("Y")
                .plot("Test1", x1, y1).pointSize(s1).linesVisible(false)
                .plot("Test2", x2, y2).pointSize(.5)
                .plot("Test3", x3, y3);

        ExamplePlotUtils.display(axs);
    }

}
