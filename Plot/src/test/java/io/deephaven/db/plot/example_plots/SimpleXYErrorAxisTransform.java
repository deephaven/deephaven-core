/*
 *
 * * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 *
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;


public class SimpleXYErrorAxisTransform {

    public static void main(String[] args) {
        final double[] x1 = {1, 2, 3, 4};
        final double[] y1 = {5.4, 2.3, 5.4, 4.4};

        final double[] xLow = {0.5, 1.5, 2.5, 3.5};
        final double[] xHigh = {1.5, 2.5, 3.5, 4.5};
        final double[] yLow = {4.9, -1.8, 4.9, 3.9};
        final double[] yHigh = {5.9, 2.8, 5.9, 4.9};


        Figure fig = FigureFactory.figure()
            .errorBarXY("S1", x1, xLow, xHigh, y1, yLow, yHigh)
            .plotStyle("bar")
            .plotOrientation("H")
            .yLog();

        ExamplePlotUtils.display(fig);
    }
}
