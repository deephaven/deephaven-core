/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot;

import java.util.Arrays;

/**
 * The style of a plot (e.g. line, bar, etc.).
 */
public enum PlotStyle {
    /**
     * A bar chart.
     */
    BAR,

    /**
     * A stacked bar chart.
     */
    STACKED_BAR,

    /**
     * A line chart.
     *
     * Does not display shapes at data points by default.
     */
    LINE,

    /**
     * An area chart.
     *
     * Does not display shapes at data points by default.
     */
    AREA,

    /**
     * A stacked area chart.
     *
     * Does not display shapes at data points by default.
     */
    STACKED_AREA,

    /**
     * A pie chart.
     */
    PIE,

    /**
     * A histogram chart.
     */
    HISTOGRAM,

    /**
     * An open-high-low-close chart.
     */
    OHLC,

    /**
     * A scatter plot.
     *
     * Lines are not displayed by default.
     */
    SCATTER,

    STEP,

    /**
     * An error bar plot.
     *
     * Points are not displayed by default.
     */
    ERROR_BAR;


    /**
     * Returns the requested plot style.
     *
     * @param style case insensitive style name
     * @throws IllegalArgumentException {@code style} must not be null
     * @return PlotStyle with the given name
     */
    @SuppressWarnings("ConstantConditions")
    public static PlotStyle plotStyle(final String style) {
        if (style == null) {
            throw new IllegalArgumentException("PlotStyle can not be null!");
        }

        try {
            return PlotStyle.valueOf(style.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("PlotStyle " + style + " is not defined");
        }
    }

    /**
     * Returns the names of available plot styles.
     *
     * @return array of the PlotStyle names
     */
    public static String[] plotStyleNames() {
        return Arrays.stream(PlotStyle.values()).map(Enum::name).toArray(String[]::new);
    }

}
