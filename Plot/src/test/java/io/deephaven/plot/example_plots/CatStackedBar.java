/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.example_plots;

import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;
import io.deephaven.plot.PlotStyle;


public class CatStackedBar {

    public static void main(String[] args) {
        final String[] x1 = {"A", "B", "C", "D"};
        final Number[] y1 = {2, 3, -1, 9};
        final String[] x2 = {"A", "C", "D"};
        final Number[] y2 = {0.1, 3.2, 3.4};
        final String[] x3 = {"A", "B", "C", "D"};
        final Number[] y3 = {2.3, 0, 2.0, 3.4};

        Figure fig = FigureFactory.figure();
        Figure cht = fig.newChart(0)
                .chartTitle("Chart Title");
        Figure axs = cht.newAxes()
                .xLabel("X")
                .yLabel("Y")
                .plotStyle(PlotStyle.STACKED_BAR)
                .catPlot("Test1", x1, y1).gradientVisible(true)
                .catPlot("Test2", x2, y2)
                .catPlot("Test3", x3, y3).group(2)
                .yTicks(new double[] {1, 5});

        ExamplePlotUtils.display(axs);
    }
}
