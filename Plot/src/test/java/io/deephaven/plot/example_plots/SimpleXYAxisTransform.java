/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.example_plots;

import io.deephaven.plot.Figure;
import io.deephaven.plot.axistransformations.AxisTransforms;


import static io.deephaven.plot.PlottingConvenience.plot;

public class SimpleXYAxisTransform {

    public static void main(String[] args) {
        final Number[] x1 = {1, 2, 3, 4};
        final Number[] y1 = {2, 3, -1, 9};

        final Number[] x2 = {1, 2, 3, 4};
        final Number[] y2 = {1.3, -3.2, 3.4, 0};

        Figure axs2 = plot("Test2", x2, y2).pointsVisible(true)
                .plot("Test1", x1, y1).pointsVisible(true)
                .xLabel("X")
                .yLabel("Y")
                .yTransform(AxisTransforms.LOG)
                .plotOrientation("H");

        ExamplePlotUtils.display(axs2);
    }
}
