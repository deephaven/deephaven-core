/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.example_plots;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;
import io.deephaven.time.DateTime;


/**
 * Sample errorBar plot.
 */
public class SimpleXYError {

    public static void main(String[] args) {
        boolean testOHLC = false;
        final double[] x1 = {1.0, 2.0, 3.0, 4.0};
        final double[] y1 = {5.4, 2.3, 5.4, 4.4};

        final double[] xLow = {0.5, 1.5, 2.5, 3.5};
        final double[] xHigh = {1.5, 2.5, 3.5, 4.5};
        final double[] yLow = {4.9, -1.8, 4.9, 3.9};
        final double[] yHigh = {5.9, 2.8, 5.9, 4.9};

        final long time = 1491946585000000000L;
        DateTime[] date = {
                new DateTime(time + DateTimeUtils.DAY * 1),
                new DateTime(time + DateTimeUtils.DAY * 2),
                new DateTime(time + DateTimeUtils.DAY * 3),
                new DateTime(time + DateTimeUtils.DAY * 4)};

        final Number[] open = {3, 4, 3, 5};
        final Number[] high = {5, 6, 5, 7};
        final Number[] low = {2, 3, 1, 4};
        final Number[] close = {4, 5, 4, 6};

        Figure fig;

        if (testOHLC) {
            fig = FigureFactory.figure()
                    .plot("S1", date, y1).plotStyle("Line")
                    .twin()
                    .ohlcPlot("S2", date, open, high, low, close).plotStyle("OHLC");
        } else {
            fig = FigureFactory.figure()
                    .errorBarXY("S1", x1, xLow, xHigh, y1, yLow, yHigh).plotStyle("bar").pointsVisible(true);
        }

        ExamplePlotUtils.display(fig);
    }
}
