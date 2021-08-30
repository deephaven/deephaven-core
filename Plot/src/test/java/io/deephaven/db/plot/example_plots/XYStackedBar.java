/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;


import static io.deephaven.db.plot.PlottingConvenience.plot;

public class XYStackedBar {

    public static void main(String[] args) {
        final double[] x1 = {1.5, 2, 3, 4};
        final Number[] y1 = {2, 3, 1, 9};
        final double[] x2 = {1, 2, 3, 4};
        final Number[] y2 = {1.3, 3.2, 3.4, 2.0};
        final double[] x3 = {1, 2, 3, 4};
        final Number[] y3 = {2.3, 1.0, 3.4, 2.3};

        Figure axs = plot("Test1", x1, y1)
            .plot("Test2", x2, y2)
            .plot("Test3", x3, y3).plotStyle("stacked_bar");

        ExamplePlotUtils.display(axs);
    }
}
