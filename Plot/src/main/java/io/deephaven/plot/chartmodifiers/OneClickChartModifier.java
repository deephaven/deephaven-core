package io.deephaven.plot.chartmodifiers;

import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.util.tables.SwappableTable;

import java.io.Serializable;

/**
 * Framework for a class which modifies a chart on a OneClick event
 */
public abstract class OneClickChartModifier<T> implements Serializable {

    private final String valueColumn;
    private final PlotInfo plotInfo;
    private final VisibilityLevel visibilityLevel;

    /**
     * Creates a OneClickChartModifier instance. to the given swappable table {@code t}.
     *
     * @param t swappable table
     * @param valueColumn column of swappable table
     * @param visibilityLevel which series to hide before a OneClick event, e.g. AXIS will hide all series for the given
     *        axis
     * @param plotInfo info for exceptions
     */
    public OneClickChartModifier(final SwappableTable t, final String valueColumn,
            final VisibilityLevel visibilityLevel, final PlotInfo plotInfo) {
        this.valueColumn = valueColumn;
        this.visibilityLevel = visibilityLevel;
        this.plotInfo = plotInfo;
    }

    /**
     * @return the visibility level
     */
    public VisibilityLevel getVisibilityLevel() {
        return visibilityLevel;
    }

    /**
     * @return the value column
     */
    public String getValueColumn() {
        return valueColumn;
    }

    /**
     * @return the plot info
     */
    public PlotInfo getPlotInfo() {
        return plotInfo;
    }

    /**
     * At what level in the Figure hierarchy the series of a plot will be restricted, e.g. AXIS will hide all series for
     * the given {@link io.deephaven.plot.Axis}
     */
    public enum VisibilityLevel {
        AXIS
    }
}
