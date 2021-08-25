/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.gui.color.Color;
import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;


public class SimpleXYColor {

    public static void main(String[] args) {
        Color red = new Color(255, 0, 0);
        Color green = new Color("green");
        Color yellow = new Color(255, 255, 0);

        final Number[] x1 = {1, 2, 3, 4};
        final Number[] y1 = {2, 3, 1, 9};
        final Color[] c1 = {red, red, yellow, yellow};

        final Number[] x2 = {0, 1.5, 4.5};
        final Number[] y2 = {1.3, 3.2, 3.4};

        Figure fig = FigureFactory.figure();
        Figure cht = fig.newChart(0)
                .chartTitle("Chart Title");
        Figure axs = cht.newAxes()
                .xLabel("X")
                .yLabel("Y")
                .plot("Test1", x1, y1).pointColor(c1).linesVisible(false)
                .plot("Test2", x2, y2).pointColor(green)
                .plot("TestF", x -> x * x / 5).pointColor("black"); // .npoints(5); //.range(-10,10);

        ExamplePlotUtils.display(axs);
    }

}
