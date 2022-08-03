/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.example_plots;

import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;

import java.util.Date;

public class SimpleXYDateBar {

    public static void main(String[] args) {

        final Date[] x1 = {
                new Date(2016 - 1900, 11, 12),
                new Date(2016 - 1900, 11, 13),
                new Date(2016 - 1900, 11, 14),
                new Date(2016 - 1900, 11, 15)
        };

        final Number[] y1 = {1, 2, 3, 4};

        Figure fig = FigureFactory.figure();
        Figure cht = fig.newChart(0)
                .chartTitle("Chart Title");
        Figure axs = cht.newAxes()
                .xLabel("X")
                .yLabel("Y").plotStyle("BAR")
                .plot("Test1", x1, y1).pointColor("red");

        ExamplePlotUtils.display(axs);
    }

}
