package io.deephaven.plot.example_plots;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.plot.Figure;
import io.deephaven.plot.PlotStyle;
import io.deephaven.time.DateTime;

import static io.deephaven.plot.PlottingConvenience.plot;

/**
 * XY plot with DateTime axis
 */
public class SimpleXYDateTime {
    public static void main(String[] args) {

        final long dateTime = DateTimeUtils.convertDateTime("2018-02-01T09:30:00 NY").getNanos();
        final DateTime[] dates = new DateTime[] {DateTimeUtils.nanosToTime(dateTime),
                DateTimeUtils.nanosToTime(dateTime + DateTimeUtils.HOUR),
                DateTimeUtils.nanosToTime(dateTime + 2 * DateTimeUtils.HOUR),
                DateTimeUtils.nanosToTime(dateTime + 3 * DateTimeUtils.HOUR),
                DateTimeUtils.nanosToTime(dateTime + 4 * DateTimeUtils.HOUR),
                DateTimeUtils.nanosToTime(dateTime + 5 * DateTimeUtils.HOUR),
                DateTimeUtils.nanosToTime(dateTime + 6 * DateTimeUtils.HOUR),
                DateTimeUtils.nanosToTime(dateTime + 6 * DateTimeUtils.HOUR + 30 * DateTimeUtils.MINUTE),
        };

        final double[] data = new double[] {1, 2, 3, 4, 5, 6, 7, 8};

        Figure axs2 = plot("Test2", dates, data)
                .xBusinessTime()
                .plotStyle(PlotStyle.SCATTER)
                .linesVisible(true)
                .xFormatPattern("HH:mm");

        ExamplePlotUtils.display(axs2);
    }
}
