/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot;

/**
 * Types of {@link Chart}s.
 */
public enum ChartType {
    /**
     * Has continuous axes.
     */
    XY,

    /**
     * Pie chart.
     */
    PIE,

    /**
     * Open-high-low-close chart.
     */
    OHLC,

    /**
     * Has one discrete axis.
     */
    CATEGORY,

    /**
     * A category axis for hierarchy, and a numeric axis for values.
     */
    TREEMAP,
}
