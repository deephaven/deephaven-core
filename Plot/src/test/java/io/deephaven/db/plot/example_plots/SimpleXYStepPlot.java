/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;

public class SimpleXYStepPlot {

    public static void main(String[] args) {
        final Number[] x1 = {1, 2, 3, 4};
        final Number[] y1 = {2, 3, 1, 9};

        Figure fig = FigureFactory.figure();
        Figure cht = fig
            .plot("Test1", x1, y1).plotStyle("step");

        ExamplePlotUtils.display(cht);
    }
}
