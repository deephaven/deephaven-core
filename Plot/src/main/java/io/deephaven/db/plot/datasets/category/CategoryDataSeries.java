/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.category;


import io.deephaven.db.plot.LineStyle;
import io.deephaven.db.plot.datasets.DataSeries;
import io.deephaven.db.plot.filters.SelectableDataSet;
import io.deephaven.db.plot.util.functions.ClosureFunction;
import io.deephaven.db.tables.Table;
import io.deephaven.gui.color.Paint;
import io.deephaven.gui.shape.Shape;
import groovy.lang.Closure;

import java.util.Map;
import java.util.function.Function;

/**
 * Dataset with discrete and numeric components. Discrete values must extend {@link Comparable} and
 * are called categories.
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
     * @param visible whether to display bar gradients or not
     * @return this CategoryDataSeries
     */
    CategoryDataSeries gradientVisible(boolean visible);


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
     * @param color color
     * @return this CategoryDataSeries
     */
    CategoryDataSeries errorBarColor(final Paint color);

    /**
     * Sets the error bar {@link Paint} for this dataset.
     *
     * @param color index of the color in the series color palette
     * @return this CategoryDataSeries
     */
    CategoryDataSeries errorBarColor(final int color);

    /**
     * Sets the error bar {@link Paint} for this dataset.
     *
     * @param color color
     * @return this CategoryDataSeries
     */
    CategoryDataSeries errorBarColor(final String color);


    ////////////////////////// line style //////////////////////////


    /**
     * Sets the {@link LineStyle} for this dataset
     *
     * @param style line style
     * @return this dat
     */
    CategoryDataSeries lineStyle(final LineStyle style);


    ////////////////////////// point colors //////////////////////////


    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param color default point color
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointColor(Paint color);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param color index of the color in the series color palette to use as the default color
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointColor(int color);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param color default point color
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointColor(String color);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param category data point
     * @param color color
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointColor(final Comparable category, final Paint color);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param category data point
     * @param color index of the color in the series color palette
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointColor(final Comparable category, final int color);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param category data point
     * @param color color
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointColor(final Comparable category, final String color);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param colors map from data points to their {@link Paint}s
     * @param <CATEGORY> type of the categorical data
     * @param <COLOR> type of color for the points
     * @return this CategoryDataSeries
     */
    <CATEGORY extends Comparable, COLOR extends Paint> CategoryDataSeries pointColor(
        final Map<CATEGORY, COLOR> colors);


    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param colors function from data points to their {@link Paint}s
     * @param <COLOR> type of color for the points
     * @return this CategoryDataSeries
     */
    <COLOR extends Paint> CategoryDataSeries pointColor(final Function<Comparable, COLOR> colors);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param colors closure from data points to their {@link Paint}s
     * @param <COLOR> type of input for the closure
     * @return this CategoryDataSeries
     */
    default <COLOR extends Paint> CategoryDataSeries pointColor(final Closure<COLOR> colors) {
        return pointColor(new ClosureFunction<>(colors));
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
    <COLOR extends Integer> CategoryDataSeries pointColorInteger(
        final Function<Comparable, COLOR> colors);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param colors closure from data points to the index of the color palette
     * @param <COLOR> type of color palette indices
     * @return this CategoryDataSeries
     */
    default <COLOR extends Integer> CategoryDataSeries pointColorInteger(
        final Closure<COLOR> colors) {
        return pointColorInteger(new ClosureFunction<>(colors));
    }

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param t table
     * @param keyColumn column in {@code t}, specifying category values
     * @param valueColumn column in {@code t}, specifying {@link Paint}s or ints/Integers
     *        representing color palette values.
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointColor(Table t, String keyColumn, String valueColumn);

    /**
     * Sets the point color. Unspecified points use the default color.
     *
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param keyColumn column in {@code sds}, specifying category values
     * @param valueColumn column in {@code sds}, specifying {@link Paint}s or ints/Integers
     *        representing color palette values.
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointColor(SelectableDataSet sds, String keyColumn, String valueColumn);

    /**
     * Sets the point color for a data point based upon the y-value.
     *
     * @param colors map from the y-value of data points to {@link Paint}
     * @return this CategoryDataSeries
     */
    <T extends Paint> CategoryDataSeries pointColorByY(Map<Double, T> colors);

    ////////////////////////// point labels //////////////////////////


    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of
     * these indices are unlabeled.
     *
     * @param category category value
     * @param label label
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointLabel(final Comparable category, final Object label);

    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of
     * these indices are unlabeled.
     *
     * @param labels map used to determine point labels
     * @param <CATEGORY> type of the categorical data
     * @param <LABEL> data type of the point labels
     * @return this CategoryDataSeries
     */
    <CATEGORY extends Comparable, LABEL> CategoryDataSeries pointLabel(
        final Map<CATEGORY, LABEL> labels);

    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of
     * these indices are unlabeled.
     *
     * @param labels function used to determine point labels
     * @param <LABEL> data type of the point labels
     * @return this CategoryDataSeries
     */
    <LABEL> CategoryDataSeries pointLabel(final Function<Comparable, LABEL> labels);

    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of
     * these indices are unlabeled.
     *
     * @param labels closure used to determine point labels for input categories
     * @param <LABEL> data type of the point labels
     * @return this CategoryDataSeries
     */
    default <LABEL> CategoryDataSeries pointLabel(final Closure<LABEL> labels) {
        return pointLabel(new ClosureFunction<>(labels));
    }

    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of
     * these indices are unlabeled.
     *
     * @param t table
     * @param keyColumn column in {@code t}, specifying category values
     * @param valueColumn column in {@code t}, specifying labels
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointLabel(Table t, String keyColumn, String valueColumn);

    /**
     * Sets the point label for data point i from index i of the input labels. Points outside of
     * these indices are unlabeled.
     *
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param keyColumn column in {@code sds}, specifying category values
     * @param valueColumn column in {@code sds}, specifying labels
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointLabel(SelectableDataSet sds, String keyColumn, String valueColumn);

    /**
     * Sets the format of the percentage point label format in pie plots.
     *
     * @param format format
     * @return this data series.
     */
    CategoryDataSeries piePercentLabelFormat(final String format);

    ////////////////////////// point shapes //////////////////////////


    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of
     * these indices use default shapes.
     *
     * @param category category value
     * @param shape shape
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointShape(final Comparable category, final String shape);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of
     * these indices use default shapes.
     *
     * @param category category value
     * @param shape shape
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointShape(final Comparable category, final Shape shape);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of
     * these indices use default shapes.
     *
     * @param shapes map used to determine point shapes
     * @param <CATEGORY> type of the categorical data
     * @return this CategoryDataSeries
     */
    <CATEGORY extends Comparable> CategoryDataSeries pointShape(final Map<CATEGORY, String> shapes);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of
     * these indices use default shapes.
     *
     * @param shapes function used to determine point shapes
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointShape(final Function<Comparable, String> shapes);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of
     * these indices use default shapes.
     *
     * @param shapes closure used to determine point shapes
     * @return this CategoryDataSeries
     */
    default CategoryDataSeries pointShape(final Closure<String> shapes) {
        return pointShape(new ClosureFunction<>(shapes));
    }

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of
     * these indices use default shapes.
     *
     * @param t table
     * @param keyColumn column in {@code t}, specifying category values
     * @param valueColumn column in {@code t}, specifying shapes
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointShape(Table t, String keyColumn, String valueColumn);

    /**
     * Sets the point shapes for data point i from index i of the input labels. Points outside of
     * these indices use default shapes.
     *
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param keyColumn column in {@code sds}, specifying category values
     * @param valueColumn column in {@code sds}, specifying shapes
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointShape(SelectableDataSet sds, String keyColumn, String valueColumn);



    ////////////////////////// point sizes //////////////////////////


    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param category data point
     * @param factor factor to multiply the default size (1) by
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointSize(final Comparable category, final int factor);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param category data point
     * @param factor factor to multiply the default size (1) by
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointSize(final Comparable category, final long factor);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param category data point
     * @param factor factor to multiply the default size (1) by
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointSize(final Comparable category, final double factor);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param category data point
     * @param factor factor to multiply the default size (1) by
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointSize(final Comparable category, final Number factor);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param categories data points
     * @param factors factors to multiply the default size (1) by
     * @param <CATEGORY> type of the categorical data
     * @return this CategoryDataSeries
     */
    <CATEGORY extends Comparable> CategoryDataSeries pointSize(final CATEGORY[] categories,
        int[] factors);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param categories data points
     * @param factors factors to multiply the default size (1) by
     * @param <CATEGORY> type of the categorical data
     * @return this CategoryDataSeries
     */
    <CATEGORY extends Comparable> CategoryDataSeries pointSize(final CATEGORY[] categories,
        double[] factors);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param categories data points
     * @param factors factors to multiply the default size (1) by
     * @param <CATEGORY> type of the categorical data
     * @return this CategoryDataSeries
     */
    <CATEGORY extends Comparable> CategoryDataSeries pointSize(final CATEGORY[] categories,
        long[] factors);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param categories data points
     * @param factors factors to multiply the default size (1) by
     * @param <CATEGORY> type of the categorical data
     * @param <NUMBER> data type of the point sizes
     * @return this CategoryDataSeries
     */
    <CATEGORY extends Comparable, NUMBER extends Number> CategoryDataSeries pointSize(
        final CATEGORY[] categories, NUMBER[] factors);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param factors map used to set sizes of specific data points
     * @param <CATEGORY> type of the categorical data
     * @param <NUMBER> data type of the point sizes
     * @return this CategoryDataSeries
     */
    <CATEGORY extends Comparable, NUMBER extends Number> CategoryDataSeries pointSize(
        final Map<CATEGORY, NUMBER> factors);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param factors function used to set sizes of data points
     * @param <NUMBER> data type of the point sizes
     * @return this CategoryDataSeries
     */
    <NUMBER extends Number> CategoryDataSeries pointSize(
        final Function<Comparable, NUMBER> factors);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param factors closure used to set sizes of data points
     * @param <NUMBER> data type of the point sizes
     * @return this CategoryDataSeries
     */
    default <NUMBER extends Number> CategoryDataSeries pointSize(final Closure<NUMBER> factors) {
        return pointSize(new ClosureFunction<>(factors));
    }

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param t table
     * @param keyColumn column in {@code t}, specifying category values
     * @param valueColumn column in {@code t}, specifying point sizes
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointSize(Table t, String keyColumn, String valueColumn);

    /**
     * Sets the point size. A scale factor of 1 is the default size. A scale factor of 2 is 2x the
     * default size. Unspecified points use the default size.
     *
     * @param sds selectable data set (e.g. OneClick filterable table)
     * @param keyColumn column in {@code sds}, specifying category values
     * @param valueColumn column in {@code sds}, specifying point sizes
     * @return this CategoryDataSeries
     */
    CategoryDataSeries pointSize(SelectableDataSet sds, String keyColumn, String valueColumn);


    ////////////////////// tool tips /////////////////////////////


}
