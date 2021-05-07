/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;


public class SimpleTsDBDatePlot {

    public static void main(String[] args) {
        final long time = 1491946585000000000L;
        final DBDateTime[] x1 = {
                new DBDateTime(time),
                new DBDateTime(time + DBTimeUtils.MINUTE),
                new DBDateTime(time + 2 * DBTimeUtils.MINUTE),
                new DBDateTime(time + 3 * DBTimeUtils.MINUTE)
        };
        final Number[] y1 = {2, 3, 1, 9};
        final DBDateTime[] x2 = {
                new DBDateTime(time + DBTimeUtils.MINUTE),
                new DBDateTime(time + 3 * DBTimeUtils.MINUTE),
                new DBDateTime(time + 4 * DBTimeUtils.MINUTE)
        };
        final Number[] y2 = {1.3, 3.2, 3.4};

        Figure fig = FigureFactory.figure();
        Figure cht = fig.newChart(0)
                .chartTitle("Chart Title");
        Figure axs = cht.newAxes()
                .xLabel("X")
                .yLabel("Y")
                .plot("Test1", x1, y1).pointLabelFormat("{0}: {1}, {2}").xToolTipPattern("HH:mm:SSSSSSSSS")
                .plot("Test2", x2, y2)
                .axis(0).axisLabelFont("Courier", "BOLD_ITALIC", 25);

        ExamplePlotUtils.display(axs);
    }

}
