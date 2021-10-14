package io.deephaven.engine.plot.example_plots;

import io.deephaven.engine.plot.Figure;
import io.deephaven.engine.plot.PlotStyle;
import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.tables.utils.DBTimeUtils;

import static io.deephaven.engine.plot.PlottingConvenience.plot;

/**
 * XY plot with DateTime axis
 */
public class SimpleXYDateTime {
    public static void main(String[] args) {

        final long dbDateTime = DBTimeUtils.convertDateTime("2018-02-01T09:30:00 NY").getNanos();
        final DBDateTime[] dates = new DBDateTime[] {DBTimeUtils.nanosToTime(dbDateTime),
                DBTimeUtils.nanosToTime(dbDateTime + DBTimeUtils.HOUR),
                DBTimeUtils.nanosToTime(dbDateTime + 2 * DBTimeUtils.HOUR),
                DBTimeUtils.nanosToTime(dbDateTime + 3 * DBTimeUtils.HOUR),
                DBTimeUtils.nanosToTime(dbDateTime + 4 * DBTimeUtils.HOUR),
                DBTimeUtils.nanosToTime(dbDateTime + 5 * DBTimeUtils.HOUR),
                DBTimeUtils.nanosToTime(dbDateTime + 6 * DBTimeUtils.HOUR),
                DBTimeUtils.nanosToTime(dbDateTime + 6 * DBTimeUtils.HOUR + 30 * DBTimeUtils.MINUTE),
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
