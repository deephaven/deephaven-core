/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.gui.color.Color;
import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;


public class SimpleXYTicks {

    public static void main(String[] args) {
        final Number[] x1 = {1, 2, 3, 4};
        final Number[] y1 = {2, 3, 1, 9};
        final Number[] x2 = {0, 1.5, 4.5};
        final Number[] y2 = {1.3, 3.2, 3.4};

        Figure fig = FigureFactory.figure();
        Figure cht = fig.newChart(0)
            .chartTitle("Chart Title");
        Figure axs = cht.newAxes()
            .xTicks(5)
            .xMinorTicks(3)
            .yTicksVisible(false)
            .yMinorTicks(3)
            .xLabel("X")
            .yLabel("Y")
            .plot("Test1", x1, y1)
            .plot("Test2", x2, y2)
            .plot("TestF", x -> x * x / 5).funcRange(5, 10) // .npoints(5); //.range(-10,10);
            .xAxis().axisColor(new Color(0, 255, 255))
            .axis(0).axisLabelFont("Courier", "BOLD_ITALIC", 25);

        ExamplePlotUtils.display(axs);
    }

}
