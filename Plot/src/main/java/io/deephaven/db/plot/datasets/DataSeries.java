/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets;

import io.deephaven.db.plot.LineStyle;
import io.deephaven.db.plot.Series;
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
     * @param visible bar gradient visibility
     * @return this data series.
     */
    DataSeries gradientVisible(boolean visible);


    ////////////////////////// point sizes //////////////////////////


    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param factor point size
     * @return this data series.
     */
    DataSeries pointSize(int factor);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param factor point size
     * @return this data series.
     */
    DataSeries pointSize(long factor);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param factor point size
     * @return this data series.
     */
    DataSeries pointSize(double factor);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param factor point size
     * @return this data series.
     */
    DataSeries pointSize(Number factor);


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
     * @param color color
     * @return this data series.
     */
    DataSeries pointColor(Paint color);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param color color
     * @return this data series.
     */
    DataSeries pointColor(int color);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param color color
     * @return this data series.
     */
    DataSeries pointColor(String color);

    /**
     * Sets the point color for a data point based upon the y-value.
     *
     * @param colors function from the y-value of data points to {@link Paint}
     * @return this DataSeries
     */
    <T extends Paint> DataSeries pointColorByY(Function<Double, T> colors);

    /**
     * Sets the point color for a data point based upon the y-value.
     *
     * @param colors function from the y-value of data points to {@link Paint}
     * @return this DataSeries
     */
    <T extends Paint> DataSeries pointColorByY(final Closure<T> colors);


    ////////////////////////// error bar color //////////////////////////


    /**
     * Sets the error bar {@link Paint} for this dataset.
     *
     * @param color color
     * @return this DataSeries
     */
    DataSeries errorBarColor(final Paint color);

    /**
     * Sets the error bar {@link Paint} for this dataset.
     *
     * @param color index of the color in the series color palette
     * @return this DataSeries
     */
    DataSeries errorBarColor(final int color);

    /**
     * Sets the error bar {@link Paint} for this dataset.
     *
     * @param color color
     * @return this DataSeries
     */
    DataSeries errorBarColor(final String color);


    ////////////////////////// point labels //////////////////////////


    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of
     * these indices are unlabeled.
     *
     * @param label label
     * @return this XYDataSeries
     */
    DataSeries pointLabel(Object label);


    ////////////////////////// point shapes //////////////////////////


    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of
     * these indices use default shapes.
     *
     * @param shape shape
     * @return this DataSeries
     */
    DataSeries pointShape(final String shape);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of
     * these indices use default shapes.
     *
     * @param shape shape
     * @return this DataSeries
     */
    DataSeries pointShape(final Shape shape);


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
     * @param style style
     * @return this data series.
     */
    DataSeries lineStyle(final LineStyle style);


    ////////////////////////// tool tips //////////////////////////


    /**
     * Sets the point label format.
     * <p>
     * Use {0} where the data series name should be inserted, {1} for the x-value and {2} y-value
     * e.g. "{0}: ({1}, {2})" will display as Series1: (2.0, 5.5).
     *
     * @param format format
     * @return this data series.
     */
    DataSeries pointLabelFormat(final String format);

    /**
     * Sets the tooltip format.
     *
     * @param format format
     * @return this data series.
     */
    default DataSeries toolTipPattern(final String format) {
        xToolTipPattern(format);
        yToolTipPattern(format);
        return this;
    }


    /**
     * Sets the x-value tooltip format.
     *
     * @param format format
     * @return this data series.
     */
    DataSeries xToolTipPattern(final String format);

    /**
     * Sets the y-value tooltip format.
     *
     * @param format format
     * @return this data series.
     */
    DataSeries yToolTipPattern(final String format);
}
