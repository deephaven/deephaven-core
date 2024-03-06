//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.datasets.category;


import io.deephaven.plot.LineStyle;
import io.deephaven.plot.datasets.DataSeries;
import io.deephaven.plot.filters.SelectableDataSet;
import io.deephaven.plot.util.functions.ClosureFunction;
import io.deephaven.engine.table.Table;
import io.deephaven.gui.color.Paint;
import io.deephaven.gui.shape.Shape;
import groovy.lang.Closure;

import java.util.Map;
import java.util.function.Function;

/**
 * Dataset with discrete and numeric components. Discrete values must extend {@link Comparable} and are called
 * categories.
 */
public interface CategoryDataSeries extends DataSeries {

    String CAT_SERIES_ORDER_COLUMN = "__CAT_ORDER";


    //////////////////////// data organization ////////////////////////


    /**
     * Sets the group for this dataset.
     *
     * @return this data series.
     */
    CategoryDataSeries group(final int group);


    ////////////////////////// visibility //////////////////////////


    /**
     * Sets the visibility of the lines for this dataset.
     *
     * @param visible whether to display lines or not
     * @return this CategoryDataSeries
     */
    CategoryDataSeries linesVisible(Boolean visible);

    /**
     * Sets the visibility of the point shapes for this dataset.
     *
     * @param visible whether to display point shapes or not
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointsVisible(Boolean visible);

    /**
     * Sets the visibility of bar gradients for this dataset.
     *
     * @param gradientVisible whether to display bar gradients or not
     * @return this CategoryDataSeries
     */
    CategoryDataSeries gradientVisible(boolean gradientVisible);


    ////////////////////////// color //////////////////////////


    ////////////////////////// line color //////////////////////////


    /**
     * Sets the default line {@link Paint} for this dataset.
     *
     * @param color color
     * @return this CategoryDataSeries
     */
    CategoryDataSeries lineColor(final Paint color);

    /**
     * Sets the default line {@link Paint} for this dataset.
     *
     * @param color index of the color in the series color palette
     * @return this CategoryDataSeries
     */
    CategoryDataSeries lineColor(final int color);

    /**
     * Sets the default line {@link Paint} for this dataset.
     *
     * @param color color
     * @return this CategoryDataSeries
     */
    CategoryDataSeries lineColor(final String color);


    ////////////////////////// error bar color //////////////////////////


    /**
     * Sets the error bar {@link Paint} for this dataset.
     *
     * @param errorBarColor color
     * @return this CategoryDataSeries
     */
    CategoryDataSeries errorBarColor(final Paint errorBarColor);

    /**
     * Sets the error bar {@link Paint} for this dataset.
     *
     * @param errorBarColor index of the color in the series color palette
     * @return this CategoryDataSeries
     */
    CategoryDataSeries errorBarColor(final int errorBarColor);

    /**
     * Sets the error bar {@link Paint} for this dataset.
     *
     * @param errorBarColor color
     * @return this CategoryDataSeries
     */
    CategoryDataSeries errorBarColor(final String errorBarColor);


    ////////////////////////// line style //////////////////////////


    /**
     * Sets the {@link LineStyle} for this dataset
     *
     * @param lineStyle line style
     * @return this dat
     */
    CategoryDataSeries lineStyle(final LineStyle lineStyle);


    ////////////////////////// point colors //////////////////////////


    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param pointColor default point color
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointColor(Paint pointColor);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param pointColor index of the color in the series color palette to use as the default color
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointColor(int pointColor);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param pointColor default point color
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointColor(String pointColor);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param category data point
     * @param pointColor color
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointColor(final Comparable category, final Paint pointColor);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param category data point
     * @param pointColor index of the color in the series color palette
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointColor(final Comparable category, final int pointColor);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param category data point
     * @param pointColor color
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointColor(final Comparable category, final String pointColor);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param pointColor map from data points to their {@link Paint}s
     * @param <CATEGORY> type of the categorical data
     * @param <COLOR> type of color for the points
     * @return this CategoryDataSeries
     */
    <CATEGORY extends Comparable, COLOR extends Paint> CategoryDataSeries pointColor(
            final Map<CATEGORY, COLOR> pointColor);


    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param pointColor function from data points to their {@link Paint}s
     * @param <COLOR> type of color for the points
     * @return this CategoryDataSeries
     */
    <COLOR extends Paint> CategoryDataSeries pointColor(final Function<Comparable, COLOR> pointColor);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param pointColor closure from data points to their {@link Paint}s
     * @param <COLOR> type of input for the closure
     * @return this CategoryDataSeries
     */
    default <COLOR extends Paint> CategoryDataSeries pointColor(final Closure<COLOR> pointColor) {
        return pointColor(new ClosureFunction<>(pointColor));
    }


    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param colors map from data points to the index of the color palette
     * @param <CATEGORY> type of the categorical data
     * @param <COLOR> type of color for the points
     * @return this CategoryDataSeries
     */
    <CATEGORY extends Comparable, COLOR extends Integer> CategoryDataSeries pointColorInteger(
            final Map<CATEGORY, COLOR> colors);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param colors function from data points to the index of the color palette
     * @param <COLOR> type of color for the points
     * @return this CategoryDataSeries
     */
    <COLOR extends Integer> CategoryDataSeries pointColorInteger(final Function<Comparable, COLOR> colors);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param colors closure from data points to the index of the color palette
     * @param <COLOR> type of color palette indices
     * @return this CategoryDataSeries
     */
    default <COLOR extends Integer> CategoryDataSeries pointColorInteger(final Closure<COLOR> colors) {
        return pointColorInteger(new ClosureFunction<>(colors));
    }

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param t table
     * @param category column in {@code t}, specifying category values
     * @param pointColor column in {@code t}, specifying {@link Paint}s or ints/Integers representing color palette
     *        values.
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointColor(Table t, String category, String pointColor);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param category column in {@code sds}, specifying category values
     * @param pointColor column in {@code sds}, specifying {@link Paint}s or ints/Integers representing color palette
     *        values.
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointColor(SelectableDataSet sds, String category, String pointColor);


    ////////////////////////// point labels //////////////////////////


    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of these indices are
     * unlabeled.
     *
     * @param category category value
     * @param pointLabel label
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointLabel(final Comparable category, final Object pointLabel);

    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of these indices are
     * unlabeled.
     *
     * @param pointLabels map used to determine point labels
     * @param <CATEGORY> type of the categorical data
     * @param <LABEL> data type of the point labels
     * @return this CategoryDataSeries
     */
    <CATEGORY extends Comparable, LABEL> CategoryDataSeries pointLabel(final Map<CATEGORY, LABEL> pointLabels);

    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of these indices are
     * unlabeled.
     *
     * @param pointLabels function used to determine point labels
     * @param <LABEL> data type of the point labels
     * @return this CategoryDataSeries
     */
    <LABEL> CategoryDataSeries pointLabel(final Function<Comparable, LABEL> pointLabels);

    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of these indices are
     * unlabeled.
     *
     * @param pointLabels closure used to determine point labels for input categories
     * @param <LABEL> data type of the point labels
     * @return this CategoryDataSeries
     */
    default <LABEL> CategoryDataSeries pointLabel(final Closure<LABEL> pointLabels) {
        return pointLabel(new ClosureFunction<>(pointLabels));
    }

    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of these indices are
     * unlabeled.
     *
     * @param t table
     * @param category column in {@code t}, specifying category values
     * @param pointLabel column in {@code t}, specifying labels
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointLabel(Table t, String category, String pointLabel);

    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of these indices are
     * unlabeled.
     *
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param category column in {@code sds}, specifying category values
     * @param pointLabel column in {@code sds}, specifying labels
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointLabel(SelectableDataSet sds, String category, String pointLabel);

    /**
     * Sets the format of the percentage point label format in pie plots.
     *
     * @param pieLabelFormat format
     * @return this data series.
     */
    CategoryDataSeries piePercentLabelFormat(final String pieLabelFormat);

    ////////////////////////// point shapes //////////////////////////


    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of these indices use
     * default shapes.
     *
     * @param category category value
     * @param pointShape shape
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointShape(final Comparable category, final String pointShape);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of these indices use
     * default shapes.
     *
     * @param category category value
     * @param pointShape shape
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointShape(final Comparable category, final Shape pointShape);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of these indices use
     * default shapes.
     *
     * @param pointShapes map used to determine point shapes
     * @param <CATEGORY> type of the categorical data
     * @return this CategoryDataSeries
     */
    <CATEGORY extends Comparable> CategoryDataSeries pointShape(final Map<CATEGORY, String> pointShapes);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of these indices use
     * default shapes.
     *
     * @param pointShapes function used to determine point shapes
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointShape(final Function<Comparable, String> pointShapes);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of these indices use
     * default shapes.
     *
     * @param pointShapes closure used to determine point shapes
     * @return this CategoryDataSeries
     */
    default CategoryDataSeries pointShape(final Closure<String> pointShapes) {
        return pointShape(new ClosureFunction<>(pointShapes));
    }

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of these indices use
     * default shapes.
     *
     * @param t table
     * @param category column in {@code t}, specifying category values
     * @param pointShape column in {@code t}, specifying shapes
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointShape(Table t, String category, String pointShape);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of these indices use
     * default shapes.
     *
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param category column in {@code sds}, specifying category values
     * @param pointShape column in {@code sds}, specifying shapes
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointShape(SelectableDataSet sds, String category, String pointShape);



    ////////////////////////// point sizes //////////////////////////


    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param category data point
     * @param pointSize factor to multiply the default size (1) by
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointSize(final Comparable category, final int pointSize);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param category data point
     * @param pointSize factor to multiply the default size (1) by
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointSize(final Comparable category, final long pointSize);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param category data point
     * @param pointSize factor to multiply the default size (1) by
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointSize(final Comparable category, final double pointSize);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param category data point
     * @param pointSize factor to multiply the default size (1) by
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointSize(final Comparable category, final Number pointSize);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param categories data points
     * @param pointSizes factors to multiply the default size (1) by
     * @param <CATEGORY> type of the categorical data
     * @return this CategoryDataSeries
     */
    <CATEGORY extends Comparable> CategoryDataSeries pointSize(final CATEGORY[] categories, int[] pointSizes);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param categories data points
     * @param pointSizes factors to multiply the default size (1) by
     * @param <CATEGORY> type of the categorical data
     * @return this CategoryDataSeries
     */
    <CATEGORY extends Comparable> CategoryDataSeries pointSize(final CATEGORY[] categories, double[] pointSizes);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param categories data points
     * @param pointSizes factors to multiply the default size (1) by
     * @param <CATEGORY> type of the categorical data
     * @return this CategoryDataSeries
     */
    <CATEGORY extends Comparable> CategoryDataSeries pointSize(final CATEGORY[] categories, long[] pointSizes);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param categories data points
     * @param pointSizes factors to multiply the default size (1) by
     * @param <CATEGORY> type of the categorical data
     * @param <NUMBER> data type of the point sizes
     * @return this CategoryDataSeries
     */
    <CATEGORY extends Comparable, NUMBER extends Number> CategoryDataSeries pointSize(final CATEGORY[] categories,
            NUMBER[] pointSizes);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param pointSizes map used to set sizes of specific data points
     * @param <CATEGORY> type of the categorical data
     * @param <NUMBER> data type of the point sizes
     * @return this CategoryDataSeries
     */
    <CATEGORY extends Comparable, NUMBER extends Number> CategoryDataSeries pointSize(
            final Map<CATEGORY, NUMBER> pointSizes);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param pointSizes function used to set sizes of data points
     * @param <NUMBER> data type of the point sizes
     * @return this CategoryDataSeries
     */
    <NUMBER extends Number> CategoryDataSeries pointSize(final Function<Comparable, NUMBER> pointSizes);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param pointSizes closure used to set sizes of data points
     * @param <NUMBER> data type of the point sizes
     * @return this CategoryDataSeries
     */
    default <NUMBER extends Number> CategoryDataSeries pointSize(final Closure<NUMBER> pointSizes) {
        return pointSize(new ClosureFunction<>(pointSizes));
    }

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param t table
     * @param category column in {@code t}, specifying category values
     * @param pointSize column in {@code t}, specifying point sizes
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointSize(Table t, String category, String pointSize);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the default size.
     * Unspecified points use the default size.
     *
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param category column in {@code sds}, specifying category values
     * @param pointSize column in {@code sds}, specifying point sizes
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointSize(SelectableDataSet sds, String category, String pointSize);


    ////////////////////// tool tips /////////////////////////////


}
