//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.datasets.multiseries;

import io.deephaven.plot.Series;
import io.deephaven.plot.datasets.DataSeries;
import groovy.lang.Closure;

import java.util.function.Function;


/**
 * A parent data series that spawns a {@link DataSeries} for each unique key in the parent series.
 */
public interface MultiSeries extends Series {
    // add functions to modify multiseries here (e.g. color)

    /**
     * Defines the procedure to name a generated series. If there is only one key column defining the series, the input
     * to the naming function is the singular key column value as an {@code Object}. If there are multiple key columns
     * defining the series, the input to the naming function will be an {@code Object[]} containing the key column
     * values in order.
     *
     * @param namingFunction series naming function
     */
    MultiSeries seriesNamingFunction(final Function<Object, String> namingFunction);

    /**
     * Defines the procedure to name a generated series. If there is only one key column defining the series, the input
     * to the naming function is the singular key column value as an {@code Object}. If there are multiple key columns
     * defining the series, the input to the naming function will be an {@code Object[]} containing the key column
     * values in order.
     *
     * @param namingFunction series naming closure
     */
    MultiSeries seriesNamingFunction(final Closure<String> namingFunction);

    ////////////////////////////// CODE BELOW HERE IS GENERATED -- DO NOT EDIT BY HAND //////////////////////////////
    ////////////////////////////// TO REGENERATE RUN GenerateMultiSeries //////////////////////////////
    ////////////////////////////// AND THEN RUN GenerateFigureImmutable //////////////////////////////
// @formatter:off

    <COLOR extends io.deephaven.gui.color.Paint> MultiSeries pointColor(final groovy.lang.Closure<COLOR> pointColor, final Object... multiSeriesKey);


    <COLOR extends io.deephaven.gui.color.Paint> MultiSeries pointColor(final java.util.function.Function<java.lang.Comparable, COLOR> pointColor, final Object... multiSeriesKey);


    <COLOR extends java.lang.Integer> MultiSeries pointColorInteger(final groovy.lang.Closure<COLOR> colors, final Object... multiSeriesKey);


    <COLOR extends java.lang.Integer> MultiSeries pointColorInteger(final java.util.function.Function<java.lang.Comparable, COLOR> colors, final Object... multiSeriesKey);


    <LABEL> MultiSeries pointLabel(final groovy.lang.Closure<LABEL> pointLabels, final Object... multiSeriesKey);


    <LABEL> MultiSeries pointLabel(final java.util.function.Function<java.lang.Comparable, LABEL> pointLabels, final Object... multiSeriesKey);


    MultiSeries pointShape(final groovy.lang.Closure<java.lang.String> pointShapes, final Object... multiSeriesKey);


    MultiSeries pointShape(final java.util.function.Function<java.lang.Comparable, java.lang.String> pointShapes, final Object... multiSeriesKey);


    <NUMBER extends java.lang.Number> MultiSeries pointSize(final groovy.lang.Closure<NUMBER> pointSizes, final Object... multiSeriesKey);


    <NUMBER extends java.lang.Number> MultiSeries pointSize(final java.util.function.Function<java.lang.Comparable, NUMBER> pointSizes, final Object... multiSeriesKey);


    MultiSeries errorBarColor(final java.lang.String errorBarColor, final Object... multiSeriesKey);


    MultiSeries errorBarColor(final int errorBarColor, final Object... multiSeriesKey);


    MultiSeries errorBarColor(final io.deephaven.gui.color.Paint errorBarColor, final Object... multiSeriesKey);


    MultiSeries gradientVisible(final boolean gradientVisible, final Object... multiSeriesKey);


    MultiSeries lineColor(final java.lang.String color, final Object... multiSeriesKey);


    MultiSeries lineColor(final int color, final Object... multiSeriesKey);


    MultiSeries lineColor(final io.deephaven.gui.color.Paint color, final Object... multiSeriesKey);


    MultiSeries lineStyle(final io.deephaven.plot.LineStyle lineStyle, final Object... multiSeriesKey);


    MultiSeries linesVisible(final java.lang.Boolean visible, final Object... multiSeriesKey);


    MultiSeries pointColor(final java.lang.String pointColor, final Object... multiSeriesKey);


    MultiSeries pointColor(final int pointColor, final Object... multiSeriesKey);


    MultiSeries pointColor(final io.deephaven.gui.color.Paint pointColor, final Object... multiSeriesKey);


    MultiSeries pointLabel(final java.lang.Object pointLabel, final Object... multiSeriesKey);


    MultiSeries pointLabelFormat(final java.lang.String pointLabelFormat, final Object... multiSeriesKey);


    MultiSeries pointShape(final java.lang.String pointShape, final Object... multiSeriesKey);


    MultiSeries pointShape(final io.deephaven.gui.shape.Shape pointShape, final Object... multiSeriesKey);


    MultiSeries pointSize(final java.lang.Number pointSize, final Object... multiSeriesKey);


    MultiSeries pointsVisible(final java.lang.Boolean visible, final Object... multiSeriesKey);


    MultiSeries seriesColor(final java.lang.String color, final Object... multiSeriesKey);


    MultiSeries seriesColor(final int color, final Object... multiSeriesKey);


    MultiSeries seriesColor(final io.deephaven.gui.color.Paint color, final Object... multiSeriesKey);


    MultiSeries toolTipPattern(final java.lang.String toolTipPattern, final Object... multiSeriesKey);


    MultiSeries xToolTipPattern(final java.lang.String xToolTipPattern, final Object... multiSeriesKey);


    MultiSeries yToolTipPattern(final java.lang.String yToolTipPattern, final Object... multiSeriesKey);


    MultiSeries group(final int group, final Object... multiSeriesKey);


    MultiSeries piePercentLabelFormat(final java.lang.String pieLabelFormat, final Object... multiSeriesKey);


    <CATEGORY extends java.lang.Comparable, COLOR extends io.deephaven.gui.color.Paint> MultiSeries pointColor(final java.util.Map<CATEGORY, COLOR> pointColor, final Object... multiSeriesKey);


    MultiSeries pointColor(final java.lang.Comparable category, final java.lang.String pointColor, final Object... multiSeriesKey);


    MultiSeries pointColor(final java.lang.Comparable category, final int pointColor, final Object... multiSeriesKey);


    MultiSeries pointColor(final java.lang.Comparable category, final io.deephaven.gui.color.Paint pointColor, final Object... multiSeriesKey);


    MultiSeries pointColor(final io.deephaven.engine.table.Table t, final java.lang.String category, final java.lang.String pointColor, final Object... multiSeriesKey);


    MultiSeries pointColor(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String category, final java.lang.String pointColor, final Object... multiSeriesKey);


    <CATEGORY extends java.lang.Comparable, COLOR extends java.lang.Integer> MultiSeries pointColorInteger(final java.util.Map<CATEGORY, COLOR> colors, final Object... multiSeriesKey);


    <CATEGORY extends java.lang.Comparable, LABEL> MultiSeries pointLabel(final java.util.Map<CATEGORY, LABEL> pointLabels, final Object... multiSeriesKey);


    MultiSeries pointLabel(final java.lang.Comparable category, final java.lang.Object pointLabel, final Object... multiSeriesKey);


    MultiSeries pointLabel(final io.deephaven.engine.table.Table t, final java.lang.String category, final java.lang.String pointLabel, final Object... multiSeriesKey);


    MultiSeries pointLabel(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String category, final java.lang.String pointLabel, final Object... multiSeriesKey);


    <CATEGORY extends java.lang.Comparable> MultiSeries pointShape(final java.util.Map<CATEGORY, java.lang.String> pointShapes, final Object... multiSeriesKey);


    MultiSeries pointShape(final java.lang.Comparable category, final java.lang.String pointShape, final Object... multiSeriesKey);


    MultiSeries pointShape(final java.lang.Comparable category, final io.deephaven.gui.shape.Shape pointShape, final Object... multiSeriesKey);


    MultiSeries pointShape(final io.deephaven.engine.table.Table t, final java.lang.String category, final java.lang.String pointShape, final Object... multiSeriesKey);


    MultiSeries pointShape(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String category, final java.lang.String pointShape, final Object... multiSeriesKey);


    <CATEGORY extends java.lang.Comparable, NUMBER extends java.lang.Number> MultiSeries pointSize(final java.util.Map<CATEGORY, NUMBER> pointSizes, final Object... multiSeriesKey);


    <CATEGORY extends java.lang.Comparable, NUMBER extends java.lang.Number> MultiSeries pointSize(final CATEGORY[] categories, final NUMBER[] pointSizes, final Object... multiSeriesKey);


    <CATEGORY extends java.lang.Comparable> MultiSeries pointSize(final CATEGORY[] categories, final double[] pointSizes, final Object... multiSeriesKey);


    <CATEGORY extends java.lang.Comparable> MultiSeries pointSize(final CATEGORY[] categories, final int[] pointSizes, final Object... multiSeriesKey);


    <CATEGORY extends java.lang.Comparable> MultiSeries pointSize(final CATEGORY[] categories, final long[] pointSizes, final Object... multiSeriesKey);


    MultiSeries pointSize(final java.lang.Comparable category, final java.lang.Number pointSize, final Object... multiSeriesKey);


    MultiSeries pointSize(final java.lang.Comparable category, final double pointSize, final Object... multiSeriesKey);


    MultiSeries pointSize(final java.lang.Comparable category, final int pointSize, final Object... multiSeriesKey);


    MultiSeries pointSize(final java.lang.Comparable category, final long pointSize, final Object... multiSeriesKey);


    MultiSeries pointSize(final io.deephaven.engine.table.Table t, final java.lang.String category, final java.lang.String pointSize, final Object... multiSeriesKey);


    MultiSeries pointSize(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String category, final java.lang.String pointSize, final Object... multiSeriesKey);


    MultiSeries pointColor(final int[] pointColors, final Object... multiSeriesKey);


    MultiSeries pointColor(final io.deephaven.gui.color.Paint[] pointColor, final Object... multiSeriesKey);


    MultiSeries pointColor(final java.lang.Integer[] pointColors, final Object... multiSeriesKey);


    MultiSeries pointColor(final java.lang.String[] pointColors, final Object... multiSeriesKey);


    <T extends io.deephaven.gui.color.Paint> MultiSeries pointColor(final io.deephaven.plot.datasets.data.IndexableData<T> pointColor, final Object... multiSeriesKey);


    MultiSeries pointColor(final io.deephaven.engine.table.Table t, final java.lang.String pointColors, final Object... multiSeriesKey);


    MultiSeries pointColor(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String pointColors, final Object... multiSeriesKey);


    MultiSeries pointColorInteger(final io.deephaven.plot.datasets.data.IndexableData<java.lang.Integer> colors, final Object... multiSeriesKey);


    MultiSeries pointLabel(final java.lang.Object[] pointLabels, final Object... multiSeriesKey);


    MultiSeries pointLabel(final io.deephaven.plot.datasets.data.IndexableData<?> pointLabels, final Object... multiSeriesKey);


    MultiSeries pointLabel(final io.deephaven.engine.table.Table t, final java.lang.String pointLabel, final Object... multiSeriesKey);


    MultiSeries pointLabel(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String pointLabel, final Object... multiSeriesKey);


    MultiSeries pointShape(final io.deephaven.gui.shape.Shape[] pointShapes, final Object... multiSeriesKey);


    MultiSeries pointShape(final java.lang.String[] pointShapes, final Object... multiSeriesKey);


    MultiSeries pointShape(final io.deephaven.plot.datasets.data.IndexableData<java.lang.String> pointShapes, final Object... multiSeriesKey);


    MultiSeries pointShape(final io.deephaven.engine.table.Table t, final java.lang.String pointShape, final Object... multiSeriesKey);


    MultiSeries pointShape(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String pointShape, final Object... multiSeriesKey);


    <T extends java.lang.Number> MultiSeries pointSize(final T[] pointSizes, final Object... multiSeriesKey);


    MultiSeries pointSize(final double[] pointSizes, final Object... multiSeriesKey);


    MultiSeries pointSize(final int[] pointSizes, final Object... multiSeriesKey);


    MultiSeries pointSize(final long[] pointSizes, final Object... multiSeriesKey);


    MultiSeries pointSize(final io.deephaven.plot.datasets.data.IndexableData<java.lang.Double> pointSizes, final Object... multiSeriesKey);


    MultiSeries pointSize(final io.deephaven.engine.table.Table t, final java.lang.String pointSizes, final Object... multiSeriesKey);


    MultiSeries pointSize(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String pointSize, final Object... multiSeriesKey);



}