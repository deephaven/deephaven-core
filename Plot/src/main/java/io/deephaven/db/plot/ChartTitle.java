/*
 * * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot;

import io.deephaven.configuration.Configuration;
import io.deephaven.db.plot.errors.PlotInfo;

import java.io.Serializable;

/**
 * Static chart title
 */
public class ChartTitle implements Serializable {

    /**
     * Static title
     */
    private String staticTitle;

    /**
     * Plot information
     */
    private PlotInfo plotInfo;

    /**
     * Property name literal for {@code chart}
     */
    static final String MAX_VISIBLE_ROWS_COUNT_PROP = "Plot.chartTitle.maxRowsInTitle";

    /**
     * No. of values shown in a dynamic chart title string
     * <p>
     * {@code maxVisibleRowsCount} < 0 allows all the values to be shown in title string
     */
    int maxVisibleRowsCount;


    ChartTitle(final PlotInfo plotInfo) {
        this(plotInfo,
            Configuration.getInstance().getIntegerWithDefault(MAX_VISIBLE_ROWS_COUNT_PROP, 3));
    }

    ChartTitle(final PlotInfo plotInfo, final int maxVisibleRowsCount) {
        this.plotInfo = plotInfo;
        this.maxVisibleRowsCount = maxVisibleRowsCount;
    }

    /**
     * Copies given {@link ChartTitle} instance to this
     *
     * @param chartTitle instance to copy
     */
    void copy(final ChartTitle chartTitle) {
        this.staticTitle = chartTitle.staticTitle;
        this.plotInfo = chartTitle.plotInfo;
        this.maxVisibleRowsCount = chartTitle.maxVisibleRowsCount;
    }

    public PlotInfo getPlotInfo() {
        return plotInfo;
    }

    void setStaticTitle(String staticTitle) {
        this.staticTitle = staticTitle;
    }

    synchronized String getTitle() {
        return staticTitle == null ? "" : staticTitle;
    }
}
