//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.example_plots;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.plot.Figure;
import io.deephaven.plot.PlotStyle;

import java.time.Instant;

import static io.deephaven.plot.PlottingConvenience.plot;

/**
 * XY plot with date time axis
 */
public class SimpleXYDateTime {
    public static void main(String[] args) {

        final long time = DateTimeUtils.epochNanos(DateTimeUtils.parseInstant("2018-02-01T09:30:00 NY"));
        final Instant[] instants = new Instant[] {
                DateTimeUtils.epochNanosToInstant(time),
                DateTimeUtils.epochNanosToInstant(time + DateTimeUtils.HOUR),
                DateTimeUtils.epochNanosToInstant(time + 2 * DateTimeUtils.HOUR),
                DateTimeUtils.epochNanosToInstant(time + 3 * DateTimeUtils.HOUR),
                DateTimeUtils.epochNanosToInstant(time + 4 * DateTimeUtils.HOUR),
                DateTimeUtils.epochNanosToInstant(time + 5 * DateTimeUtils.HOUR),
                DateTimeUtils.epochNanosToInstant(time + 6 * DateTimeUtils.HOUR),
                DateTimeUtils.epochNanosToInstant(time + 6 * DateTimeUtils.HOUR + 30 * DateTimeUtils.MINUTE),
        };

        final double[] data = new double[] {1, 2, 3, 4, 5, 6, 7, 8};

        Figure axs2 = plot("Test2", instants, data)
                .xBusinessTime()
                .plotStyle(PlotStyle.SCATTER)
                .linesVisible(true)
                .xFormatPattern("HH:mm");

        ExamplePlotUtils.display(axs2);
    }
}
