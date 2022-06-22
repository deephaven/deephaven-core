/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.example_plots;

import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;

import java.util.Date;

public class SimpleTsPlot {

    public static void main(String[] args) {
        final Date[] x1 = {
                new Date(2016 - 1900, 11, 12),
                new Date(2016 - 1900, 12, 31),
                new Date(2017 - 1900, 1, 2),
                new Date(2017 - 1900, 3, 3)
        };
        final Number[] y1 = {2, 3, 1, 9};
        final Date[] x2 = {
                new Date(2016 - 1900, 6, 14),
                new Date(2016 - 1900, 12, 12),
                new Date(2017 - 1900, 4, 1)
        };
        final Number[] y2 = {1.3, 3.2, 3.4};

        int size = 5;
        Figure fig = FigureFactory.figure(size, size);

        for (int i = 0; i < size * size; i++) {
            fig = fig.newChart()
                    .newAxes()
                    .plot("Test1", x1, y1)
                    .plot("Test2", x2, y2);
        }

        ExamplePlotUtils.display(fig);
    }

}
