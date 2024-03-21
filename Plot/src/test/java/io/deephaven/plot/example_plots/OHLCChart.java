//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.example_plots;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;

import java.time.Instant;


public class OHLCChart {
    public static void main(String[] args) {

        final long time = 1491946585000000000L;

        Instant[] date = {
                DateTimeUtils.epochNanosToInstant(time + DateTimeUtils.DAY * 1),
                DateTimeUtils.epochNanosToInstant(time + DateTimeUtils.DAY * 2),
                DateTimeUtils.epochNanosToInstant(time + DateTimeUtils.DAY * 3),
                DateTimeUtils.epochNanosToInstant(time + DateTimeUtils.DAY * 4)};
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
