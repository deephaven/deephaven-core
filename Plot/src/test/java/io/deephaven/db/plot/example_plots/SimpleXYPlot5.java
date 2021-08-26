/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;


public class SimpleXYPlot5 {

    public static void main(String[] args) {
        final Number[] x1 = {1, 2, 3, 4};
        final Number[] y1 = {2, 3, 1, 9};
        final Number[] x2 = {0, 1.5, 4.5};
        final Number[] y2 = {1.3, 3.2, 3.4};
        final Number[] x3 = {10, 11.5, 12.5};
        final Number[] y3 = {11.3, 2.2, 8.4};

        Figure fig = FigureFactory.figure(2, 2)
            .figureTitle("Figure");

        Figure cht = fig.newChart(0)
            .colSpan(2)
            .chartTitle("My Chart 1");

        Figure ax = cht.newAxes()
            .xLabel("X1")
            .yLabel("Y1")
            .plot("Test1", x1, y1)
            .plotStyle("SCATTER");


        Figure cht2 = ax.newChart(1, 0)
            .chartTitle("My Chart 2");

        Figure ax2 = cht2.newAxes()
            .xLabel("X2")
            .yLabel("Y2")
            .plot("Test1", x2, y2)
            .plotStyle("SCATTER");

        Figure cht3 = ax2.newChart(1, 1)
            .chartTitle("My Chart 3");

        Figure ax3 = cht3.newAxes()
            .xLabel("X3")
            .yLabel("Y3")
            .plot("Test1", x3, y3)
            .plotStyle("SCATTER");

        ExamplePlotUtils.display(ax3);
    }

}
