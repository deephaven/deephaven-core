package io.deephaven.plot.example_plots;

import io.deephaven.plot.Figure;
import io.deephaven.plot.PlotStyle;
import io.deephaven.engine.time.DateTime;
import io.deephaven.engine.time.DateTimeUtil;

import static io.deephaven.plot.PlottingConvenience.plot;

/**
 * XY plot with DateTime axis
 */
public class SimpleXYDateTime {
    public static void main(String[] args) {

        final long dateTime = DateTimeUtil.convertDateTime("2018-02-01T09:30:00 NY").getNanos();
        final DateTime[] dates = new DateTime[] {DateTimeUtil.nanosToTime(dateTime),
                DateTimeUtil.nanosToTime(dateTime + DateTimeUtil.HOUR),
                DateTimeUtil.nanosToTime(dateTime + 2 * DateTimeUtil.HOUR),
                DateTimeUtil.nanosToTime(dateTime + 3 * DateTimeUtil.HOUR),
                DateTimeUtil.nanosToTime(dateTime + 4 * DateTimeUtil.HOUR),
                DateTimeUtil.nanosToTime(dateTime + 5 * DateTimeUtil.HOUR),
                DateTimeUtil.nanosToTime(dateTime + 6 * DateTimeUtil.HOUR),
                DateTimeUtil.nanosToTime(dateTime + 6 * DateTimeUtil.HOUR + 30 * DateTimeUtil.MINUTE),
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
