/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.errors;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.plot.*;

import java.io.Serializable;

/**
 * Information about a plot. Has 3 pieces of information (all optional) Figure title, if it has one Chart title, if it
 * has one Series name
 */
public class PlotInfo implements Serializable, LogOutputAppendable {

    private String info;

    public PlotInfo(final AxesImpl axes, final Comparable seriesName) {
        this(getAxesFigure(axes), getAxesChart(axes), seriesName);
    }

    public PlotInfo(final BaseFigureImpl figure, final ChartImpl chart, final SeriesInternal series) {
        this(figure, chart, series == null ? null : series.name());
    }

    public PlotInfo(final BaseFigureImpl figure, final ChartImpl chart, final Comparable seriesName) {
        this(figure, chart, seriesName == null ? null : seriesName.toString());
    }

    public PlotInfo(final BaseFigureImpl figure, final ChartImpl chart, final String seriesName) {
        this(figure == null ? null : figure.getTitle(), chart == null ? null : chart.getTitle(), seriesName);
    }

    public PlotInfo(final String figureName, final String chartName, final String seriesName) {
        this.info = encodeInfo(figureName, chartName, seriesName);
    }

    private static BaseFigureImpl getAxesFigure(final AxesImpl axes) {
        final ChartImpl chart = getAxesChart(axes);
        return chart == null ? null : chart.figure();
    }

    private static ChartImpl getAxesChart(final AxesImpl axes) {
        return axes == null ? null : axes.chart();
    }

    private String encodeInfo(final String figureName, final String chartName, final String seriesName) {
        String info = "";

        if (figureName != null) {
            info += "Figure: " + figureName + ".";
        }

        if (chartName != null) {
            if (figureName != null) {
                info += " ";
            }
            info += "Chart: " + chartName + ".";
        }

        if (seriesName != null) {
            if (figureName != null || chartName != null) {
                info += " ";
            }
            info += "Series: " + seriesName + ".";
        }

        return info.isEmpty() ? null : info;
    }

    @Override
    public String toString() {
        return info;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append(info);
    }
}
