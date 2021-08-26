/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;


public class SimpleCategoryPlot {

    public static void main(String[] args) {
        final String[] x1 = {"A", "B", "C", "D"};

        Figure fig = FigureFactory.figure();
        Figure cht = fig.newChart(0)
                .chartTitle("Chart Title");
        Figure axs = cht.newAxes()
                .xLabel("X")
                .yLabel("Y")
                .piePlot("Test1", x1, new int[] {2, 2, 3, 2}).piePercentLabelFormat("0.00")
                .axis(0).axisLabelFont("Courier", "BOLD_ITALIC", 25);

        ExamplePlotUtils.display(axs);
    }

}
