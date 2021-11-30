/*
 *
 * * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 *
 */

package io.deephaven.plot.example_plots;

import io.deephaven.plot.Figure;
import io.deephaven.plot.axistransformations.AxisTransforms;


import static io.deephaven.plot.PlottingConvenience.catPlot;

public class SimpleCategoryAxisTransform {

    public static void main(String[] args) {
        final String[] x1 = {"A", "B", "C", "D"};
        final Number[] y1 = {4.5, 0.9, -4, 7};

        final String[] x2 = {"A", "B", "C", "D", "E"};
        final Number[] y2 = {18, -5, 0, 9, 12};

        Figure axs2 = catPlot("Test2", x2, y2)
                .catPlot("Test1", x1, y1)
                .xLabel("Cats")
                .yLabel("Y")
                .yTransform(AxisTransforms.SQRT)
                .plotOrientation("H");

        ExamplePlotUtils.display(axs2);
    }
}
