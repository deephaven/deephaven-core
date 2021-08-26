/*
 *
 * * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 *
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;
import io.deephaven.db.plot.axistransformations.AxisTransforms;


public class SimpleCatErrorAxisTransform {
    public static void main(String[] args) {
        final String[] x1 = {"A", "B", "C", "D"};
        final double[] y1 = {5.4, 2.3, 5.4, 4.4};
        final double[] yLow = {4.9, -1.8, 4.9, 3.9};
        final double[] yHigh = {5.9, 2.8, 5.9, 4.9};


        Figure fig = FigureFactory.figure()
            .catErrorBar("S1", x1, y1, yLow, yHigh)
            .plotStyle("bar")
            .yTransform(AxisTransforms.LOG);

        ExamplePlotUtils.display(fig);
    }
}
