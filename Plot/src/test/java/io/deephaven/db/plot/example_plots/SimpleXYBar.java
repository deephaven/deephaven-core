/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.axistransformations.AxisTransforms;


import static io.deephaven.db.plot.PlottingConvenience.plot;

public class SimpleXYBar {

    public static void main(String[] args) {

        final Number[] x1 = {1, 2, 3, 4};
        final Number[] y1 = {2, 3, 1, 9};

        final Number[] x2 = {0, 1.5, 4.5};
        final Number[] y2 = {1.3, 3.2, 3.4};

        Figure axs2 = plot("Test2", x2, y2)
                .plot("Test1", x1, y1)
                .plotStyle("BAR")
                .yTransform(AxisTransforms.LOG);

        ExamplePlotUtils.display(axs2);
    }

}
