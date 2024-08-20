//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.datasets;

import io.deephaven.plot.LineStyle;
import io.deephaven.plot.Series;
import io.deephaven.gui.color.Color;
import io.deephaven.gui.color.Paint;
import io.deephaven.gui.shape.Shape;
import groovy.lang.Closure;

import java.io.Serializable;
import java.util.function.Function;

/**
 * A generic data series.
 */
public interface DataSeries extends Series, Serializable {


    ////////////////////////// visibility //////////////////////////


    /**
     * Sets whether lines are visible.
     *
     * @param visible line visibility
     * @return this data series.
     */
    DataSeries linesVisible(Boolean visible);

    /**
     * Sets whether points are visible.
     *
     * @param visible point visibility
     * @return this data series.
     */
    DataSeries pointsVisible(Boolean visible);

    /**
     * Sets whether bar gradients are visible.
     *
     * @param gradientVisible bar gradient visibility
     * @return this data series.
     */
    DataSeries gradientVisible(boolean gradientVisible);


    ////////////////////////// point sizes //////////////////////////


    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param pointSize point size
     * @return this data series.
     */
    DataSeries pointSize(int pointSize);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param pointSize point size
     * @return this data series.
     */
    DataSeries pointSize(long pointSize);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param pointSize point size
     * @return this data series.
     */
    DataSeries pointSize(double pointSize);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param pointSize point size
     * @return this data series.
     */
    DataSeries pointSize(Number pointSize);


    ////////////////////////// color //////////////////////////


    /**
     * Defines the default line and point color.
     *
     * @param color color
     * @return this data series.
     */
    default DataSeries seriesColor(final Paint color) {
        lineColor(color);
        pointColor(color);
        return this;
    }

    /**
     * Defines the default line and point color.
     *
     * @param color color
     * @return this data series.
     */
    default DataSeries seriesColor(final int color) {
        lineColor(color);
        pointColor(color);
        return this;
    }

    /**
     * Defines the default line and point color.
     *
     * @param color color
     * @return this data series.
     */
    default DataSeries seriesColor(final String color) {
        return seriesColor(Color.color(color));
    }


    ////////////////////////// point color //////////////////////////


    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param pointColor color
     * @return this data series.
     */
    DataSeries pointColor(Paint pointColor);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param pointColor color
     * @return this data series.
     */
    DataSeries pointColor(int pointColor);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param pointColor color
     * @return this data series.
     */
    DataSeries pointColor(String pointColor);


    ////////////////////////// error bar color //////////////////////////


    /**
     * Sets the error bar {@link Paint} for this dataset.
     *
     * @param errorBarColor color
     * @return this DataSeries
     */
    DataSeries errorBarColor(final Paint errorBarColor);

    /**
     * Sets the error bar {@link Paint} for this dataset.
     *
     * @param errorBarColor index of the color in the series color palette
     * @return this DataSeries
     */
    DataSeries errorBarColor(final int errorBarColor);

    /**
     * Sets the error bar {@link Paint} for this dataset.
     *
     * @param errorBarColor color
     * @return this DataSeries
     */
    DataSeries errorBarColor(final String errorBarColor);


    ////////////////////////// point labels //////////////////////////


    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of these indices are
     * unlabeled.
     *
     * @param pointLabel label
     * @return this XYDataSeries
     */
    DataSeries pointLabel(Object pointLabel);


    ////////////////////////// point shapes //////////////////////////


    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of these indices use
     * default shapes.
     *
     * @param pointShape shape
     * @return this DataSeries
     */
    DataSeries pointShape(final String pointShape);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of these indices use
     * default shapes.
     *
     * @param pointShape shape
     * @return this DataSeries
     */
    DataSeries pointShape(final Shape pointShape);


    ////////////////////////// line color //////////////////////////


    /**
     * Defines the default line color.
     *
     * @param color color
     * @return this data series.
     */
    DataSeries lineColor(final Paint color);

    /**
     * Defines the default line color.
     *
     * @param color color palette index
     * @return this data series.
     */
    DataSeries lineColor(final int color);

    /**
     * Defines the default line color.
     *
     * @param color color
     * @return this data series.
     */
    DataSeries lineColor(final String color);


    ////////////////////////// line style //////////////////////////


    /**
     * Sets the line style.
     *
     * @param lineStyle style
     * @return this data series.
     */
    DataSeries lineStyle(final LineStyle lineStyle);


    ////////////////////////// tool tips //////////////////////////


    /**
     * Sets the point label format.
     * <p>
     * Use {0} where the data series name should be inserted, {1} for the x-value and {2} y-value e.g. "{0}: ({1}, {2})"
     * will display as Series1: (2.0, 5.5).
     *
     * @param pointLabelFormat format
     * @return this data series.
     */
    DataSeries pointLabelFormat(final String pointLabelFormat);

    /**
     * Sets the tooltip format.
     *
     * @param toolTipPattern format
     * @return this data series.
     */
    default DataSeries toolTipPattern(final String toolTipPattern) {
        xToolTipPattern(toolTipPattern);
        yToolTipPattern(toolTipPattern);
        return this;
    }


    /**
     * Sets the x-value tooltip format.
     *
     * @param xToolTipPattern format
     * @return this data series.
     */
    DataSeries xToolTipPattern(final String xToolTipPattern);

    /**
     * Sets the y-value tooltip format.
     *
     * @param yToolTipPattern format
     * @return this data series.
     */
    DataSeries yToolTipPattern(final String yToolTipPattern);
}
