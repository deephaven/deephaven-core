/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.example_plots;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;
import io.deephaven.time.DateTime;


public class OHLCChart {
    public static void main(String[] args) {

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

        Figure fig = FigureFactory.figure();
        Figure cht = fig.newChart(0)
                .chartTitle("Chart Title");
        Figure axs = cht.newAxes()
                .xLabel("X")
                .yLabel("Y")
                .ohlcPlot("Test", date, open, high, low, close);


        ExamplePlotUtils.display(axs);
    }
}
