/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;


public class OHLCChart {
    public static void main(String[] args) {

        final long time = 1491946585000000000L;

        DBDateTime[] date = {
                new DBDateTime(time + DBTimeUtils.DAY * 1),
                new DBDateTime(time + DBTimeUtils.DAY * 2),
                new DBDateTime(time + DBTimeUtils.DAY * 3),
                new DBDateTime(time + DBTimeUtils.DAY * 4)};
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
