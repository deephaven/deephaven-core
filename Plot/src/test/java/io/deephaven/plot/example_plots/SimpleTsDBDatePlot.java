/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.example_plots;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;

import java.time.Instant;

public class SimpleTsDBDatePlot {

    public static void main(String[] args) {
        final long time = 1491946585000000000L;
        final Instant[] x1 = {
                DateTimeUtils.epochNanosToInstant(time),
                DateTimeUtils.epochNanosToInstant(time + DateTimeUtils.MINUTE),
                DateTimeUtils.epochNanosToInstant(time + 2 * DateTimeUtils.MINUTE),
                DateTimeUtils.epochNanosToInstant(time + 3 * DateTimeUtils.MINUTE)
        };
        final Number[] y1 = {2, 3, 1, 9};
        final Instant[] x2 = {
                DateTimeUtils.epochNanosToInstant(time + DateTimeUtils.MINUTE),
                DateTimeUtils.epochNanosToInstant(time + 3 * DateTimeUtils.MINUTE),
                DateTimeUtils.epochNanosToInstant(time + 4 * DateTimeUtils.MINUTE)
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
