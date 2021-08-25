/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;


public class SimpleXYStackedArea {

    public static void main(String[] args) {
        final Number[] x1 = {5.0, 10.0, 15.0, 20.0};
        final Number[] y1 = {5.0, 15.5, 9.5, 7.5};
        final Number[] y2 = {7.0, 15.5, 9.5, 3.5};

        Figure fig = FigureFactory.figure();
        Figure cht = fig.newChart(0)
                .chartTitle("Chart Title");
        Figure axs = cht.newAxes()
                .xColor("blue")
                .yColor("red")
                .xLabel("X")
                .yLabel("Y").plotStyle("STACKED_AREA")
                .plot("Test1", x1, y1).pointsVisible(true)
                .plot("Test2", x1, y2).pointsVisible(true);

        ExamplePlotUtils.display(axs);
    }

}
