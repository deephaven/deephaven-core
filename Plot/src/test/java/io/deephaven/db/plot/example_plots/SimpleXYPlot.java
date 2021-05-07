/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;


public class SimpleXYPlot {

    public static void main(String[] args) {
        final Number[] x1 = {1, 2, 3, 4};
        final Number[] y1 = {2, 3, 1, 9};
        final Number[] x2 = {0, 1.5, 15.5};
        final Number[] y2 = {1.3, 3.2, 3.4};

        Figure fig = FigureFactory.figure()
                .xInvert(true)
                .plot("TestF", x1, y1);
        ExamplePlotUtils.display(fig);
    }

}
