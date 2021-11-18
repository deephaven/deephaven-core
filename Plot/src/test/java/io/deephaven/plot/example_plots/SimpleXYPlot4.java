/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.example_plots;

import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;
import io.deephaven.plot.LineStyle;


public class SimpleXYPlot4 {

    public static void main(String[] args) {

        LineStyle thickDash = LineStyle.lineStyle(2.0, "BUTT", "MITER", 10.0f);

        Figure fig = FigureFactory.figure();

        Figure cht = fig.newChart(0)
                .chartTitle("triad_mixing")
                .chartTitleFont("Courier", "PLAIN", 40);

        Figure ax = cht.newAxes()
                .xLabel("X1")
                .yLabel("Y1")
                .plot("Func0", x -> -5)
                .plot("Func1", x -> -4)
                .plot("Func2", x -> -3).lineStyle(thickDash);

        ExamplePlotUtils.display(ax);
    }

}
