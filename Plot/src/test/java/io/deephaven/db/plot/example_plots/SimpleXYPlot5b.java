/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;


public class SimpleXYPlot5b {

    public static void main(String[] args) {
        final Number[] x1 = {1, 2, 3, 4};
        final Number[] y1 = {2, 3, 1, 9};
        final Number[] x2 = {0, 1.5, 4.5};
        final Number[] y2 = {1.3, 3.2, 3.4};
        final Number[] x3 = {10, 11.5, 12.5};
        final Number[] y3 = {11.3, 2.2, 8.4};
        final String[] labels = {"A", "B", "C"};

        Figure fig = FigureFactory.figure(2, 2);

        Figure cht = fig.newChart(0).rowSpan(2)
                .chartTitle("My Chart 1")
                .chartTitleFont("Courier", "PLAIN", 40);

        Figure ax = cht.newAxes()
                .xLabel("X1")
                .yLabel("Y1")
                .plot("Test1", x1, y1)
                .linesVisible(false).pointsVisible(true);


        Figure cht2 = ax.newChart()
                .chartTitle("My Chart 2")
                .chartTitleFont("Courier", "PLAIN", 40);

        Figure ax2 = cht2.newAxes()
                .xLabel("X2")
                .yLabel("Y2")
                .plot("Test1", x2, y2)
                .linesVisible(false).pointsVisible(true);

        Figure cht3 = ax2.newChart()
                .chartTitle("My Chart 3")
                .chartTitleFont("Courier", "PLAIN", 40);

        Figure ax3 = cht3.newAxes()
                .xLabel("X3")
                .yLabel("Y3")
                .plot("Test1", x3, y3).pointLabel(labels)
                .linesVisible(false).pointsVisible(true);


        ExamplePlotUtils.display(ax3);
    }

}
