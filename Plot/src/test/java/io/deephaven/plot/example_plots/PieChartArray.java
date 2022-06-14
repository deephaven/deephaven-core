/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.example_plots;

import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;


public class PieChartArray {
    public static void main(String[] args) {
        String[] categories = {"Samsung", "Others", "Nokia", "Apple"};
        Number[] values = {27.8, 55.3, 16.8, 17.1};

        Figure fig = FigureFactory.figure();
        Figure cht = fig.newChart(0)
                .chartTitle("Chart Title");
        Figure axs = cht.newAxes()
                .xLabel("X")
                .yLabel("Y")
                .piePlot("Test", categories, values).pointLabelFormat("{0}");

        ExamplePlotUtils.display(axs);
    }
}
