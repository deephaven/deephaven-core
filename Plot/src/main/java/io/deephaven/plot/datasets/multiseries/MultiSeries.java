/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.multiseries;

import io.deephaven.plot.Series;
import io.deephaven.plot.datasets.DataSeries;
import groovy.lang.Closure;
import io.deephaven.engine.table.Table;

import java.util.function.Function;


/**
 * A parent data series that spawns a {@link DataSeries} for each unique key in the parent series.
 */
public interface MultiSeries extends Series {
    // add functions to modify multiseries here (e.g. color)

    /**
     * Defines the procedure to name a generated series. The input of the naming function is the table map key
     * corresponding to the new series.
     *
     * @param namingFunction series naming function
     */
    MultiSeries seriesNamingFunction(final Function<Object, String> namingFunction);

    /**
     * Defines the procedure to name a generated series. The input of the naming function is the table map key
     * corresponding to the new series.
     *
     * @param namingFunction series naming closure
     */
    MultiSeries seriesNamingFunction(final Closure<String> namingFunction);

    ////////////////////////////// CODE BELOW HERE IS GENERATED -- DO NOT EDIT BY HAND //////////////////////////////
    ////////////////////////////// TO REGENERATE RUN GenerateMultiSeries //////////////////////////////
    ////////////////////////////// AND THEN RUN GenerateFigureImmutable //////////////////////////////

    <T extends io.deephaven.gui.color.Paint> MultiSeries pointColorByY(final groovy.lang.Closure<T> colors, final Object... keys);


    <T extends io.deephaven.gui.color.Paint> MultiSeries pointColorByY(final java.util.function.Function<java.lang.Double, T> colors, final Object... keys);


    <COLOR extends io.deephaven.gui.color.Paint> MultiSeries pointColor(final groovy.lang.Closure<COLOR> colors, final Object... keys);


    <COLOR extends io.deephaven.gui.color.Paint> MultiSeries pointColor(final java.util.function.Function<java.lang.Comparable, COLOR> colors, final Object... keys);


    <COLOR extends java.lang.Integer> MultiSeries pointColorInteger(final groovy.lang.Closure<COLOR> colors, final Object... keys);


    <COLOR extends java.lang.Integer> MultiSeries pointColorInteger(final java.util.function.Function<java.lang.Comparable, COLOR> colors, final Object... keys);


    <LABEL> MultiSeries pointLabel(final groovy.lang.Closure<LABEL> labels, final Object... keys);


    <LABEL> MultiSeries pointLabel(final java.util.function.Function<java.lang.Comparable, LABEL> labels, final Object... keys);


    MultiSeries pointShape(final groovy.lang.Closure<java.lang.String> shapes, final Object... keys);


    MultiSeries pointShape(final java.util.function.Function<java.lang.Comparable, java.lang.String> shapes, final Object... keys);


    <NUMBER extends java.lang.Number> MultiSeries pointSize(final groovy.lang.Closure<NUMBER> factors, final Object... keys);


    <NUMBER extends java.lang.Number> MultiSeries pointSize(final java.util.function.Function<java.lang.Comparable, NUMBER> factors, final Object... keys);


    MultiSeries errorBarColor(final java.lang.String color, final Object... keys);


    MultiSeries errorBarColor(final int color, final Object... keys);


    MultiSeries errorBarColor(final io.deephaven.gui.color.Paint color, final Object... keys);


    MultiSeries gradientVisible(final boolean visible, final Object... keys);


    MultiSeries lineColor(final java.lang.String color, final Object... keys);


    MultiSeries lineColor(final int color, final Object... keys);


    MultiSeries lineColor(final io.deephaven.gui.color.Paint color, final Object... keys);


    MultiSeries lineStyle(final io.deephaven.plot.LineStyle style, final Object... keys);


    MultiSeries linesVisible(final java.lang.Boolean visible, final Object... keys);


    MultiSeries pointColor(final java.lang.String color, final Object... keys);


    MultiSeries pointColor(final int color, final Object... keys);


    MultiSeries pointColor(final io.deephaven.gui.color.Paint color, final Object... keys);


    MultiSeries pointLabel(final java.lang.Object label, final Object... keys);


    MultiSeries pointLabelFormat(final java.lang.String format, final Object... keys);


    MultiSeries pointShape(final java.lang.String shape, final Object... keys);


    MultiSeries pointShape(final io.deephaven.gui.shape.Shape shape, final Object... keys);


    MultiSeries pointSize(final java.lang.Number factor, final Object... keys);


    MultiSeries pointsVisible(final java.lang.Boolean visible, final Object... keys);


    MultiSeries seriesColor(final java.lang.String color, final Object... keys);


    MultiSeries seriesColor(final int color, final Object... keys);


    MultiSeries seriesColor(final io.deephaven.gui.color.Paint color, final Object... keys);


    MultiSeries toolTipPattern(final java.lang.String format, final Object... keys);


    MultiSeries xToolTipPattern(final java.lang.String format, final Object... keys);


    MultiSeries yToolTipPattern(final java.lang.String format, final Object... keys);


    MultiSeries group(final int group, final Object... keys);


    MultiSeries piePercentLabelFormat(final java.lang.String format, final Object... keys);


    <CATEGORY extends java.lang.Comparable, COLOR extends io.deephaven.gui.color.Paint> MultiSeries pointColor(final java.util.Map<CATEGORY, COLOR> colors, final Object... keys);


    MultiSeries pointColor(final java.lang.Comparable category, final java.lang.String color, final Object... keys);


    MultiSeries pointColor(final java.lang.Comparable category, final int color, final Object... keys);


    MultiSeries pointColor(final java.lang.Comparable category, final io.deephaven.gui.color.Paint color, final Object... keys);


    MultiSeries pointColor(final io.deephaven.engine.table.Table t, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys);


    MultiSeries pointColor(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys);


    <T extends io.deephaven.gui.color.Paint> MultiSeries pointColorByY(final java.util.Map<java.lang.Double, T> colors, final Object... keys);


    <CATEGORY extends java.lang.Comparable, COLOR extends java.lang.Integer> MultiSeries pointColorInteger(final java.util.Map<CATEGORY, COLOR> colors, final Object... keys);


    <CATEGORY extends java.lang.Comparable, LABEL> MultiSeries pointLabel(final java.util.Map<CATEGORY, LABEL> labels, final Object... keys);


    MultiSeries pointLabel(final java.lang.Comparable category, final java.lang.Object label, final Object... keys);


    MultiSeries pointLabel(final io.deephaven.engine.table.Table t, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys);


    MultiSeries pointLabel(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys);


    <CATEGORY extends java.lang.Comparable> MultiSeries pointShape(final java.util.Map<CATEGORY, java.lang.String> shapes, final Object... keys);


    MultiSeries pointShape(final java.lang.Comparable category, final java.lang.String shape, final Object... keys);


    MultiSeries pointShape(final java.lang.Comparable category, final io.deephaven.gui.shape.Shape shape, final Object... keys);


    MultiSeries pointShape(final io.deephaven.engine.table.Table t, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys);


    MultiSeries pointShape(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys);


    <CATEGORY extends java.lang.Comparable, NUMBER extends java.lang.Number> MultiSeries pointSize(final java.util.Map<CATEGORY, NUMBER> factors, final Object... keys);


    <CATEGORY extends java.lang.Comparable, NUMBER extends java.lang.Number> MultiSeries pointSize(final CATEGORY[] categories, final NUMBER[] factors, final Object... keys);


    <CATEGORY extends java.lang.Comparable> MultiSeries pointSize(final CATEGORY[] categories, final double[] factors, final Object... keys);


    <CATEGORY extends java.lang.Comparable> MultiSeries pointSize(final CATEGORY[] categories, final int[] factors, final Object... keys);


    <CATEGORY extends java.lang.Comparable> MultiSeries pointSize(final CATEGORY[] categories, final long[] factors, final Object... keys);


    MultiSeries pointSize(final java.lang.Comparable category, final java.lang.Number factor, final Object... keys);


    MultiSeries pointSize(final java.lang.Comparable category, final double factor, final Object... keys);


    MultiSeries pointSize(final java.lang.Comparable category, final int factor, final Object... keys);


    MultiSeries pointSize(final java.lang.Comparable category, final long factor, final Object... keys);


    MultiSeries pointSize(final io.deephaven.engine.table.Table t, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys);


    MultiSeries pointSize(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys);


    MultiSeries pointColor(final int[] colors, final Object... keys);


    MultiSeries pointColor(final io.deephaven.gui.color.Paint[] colors, final Object... keys);


    MultiSeries pointColor(final java.lang.Integer[] colors, final Object... keys);


    MultiSeries pointColor(final java.lang.String[] colors, final Object... keys);


    <T extends io.deephaven.gui.color.Paint> MultiSeries pointColor(final io.deephaven.plot.datasets.data.IndexableData<T> colors, final Object... keys);


    MultiSeries pointColor(final io.deephaven.engine.table.Table t, final java.lang.String columnName, final Object... keys);


    MultiSeries pointColor(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String columnName, final Object... keys);


    MultiSeries pointColorInteger(final io.deephaven.plot.datasets.data.IndexableData<java.lang.Integer> colors, final Object... keys);


    MultiSeries pointLabel(final java.lang.Object[] labels, final Object... keys);


    MultiSeries pointLabel(final io.deephaven.plot.datasets.data.IndexableData<?> labels, final Object... keys);


    MultiSeries pointLabel(final io.deephaven.engine.table.Table t, final java.lang.String columnName, final Object... keys);


    MultiSeries pointLabel(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String columnName, final Object... keys);


    MultiSeries pointShape(final io.deephaven.gui.shape.Shape[] shapes, final Object... keys);


    MultiSeries pointShape(final java.lang.String[] shapes, final Object... keys);


    MultiSeries pointShape(final io.deephaven.plot.datasets.data.IndexableData<java.lang.String> shapes, final Object... keys);


    MultiSeries pointShape(final io.deephaven.engine.table.Table t, final java.lang.String columnName, final Object... keys);


    MultiSeries pointShape(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String columnName, final Object... keys);


    <T extends java.lang.Number> MultiSeries pointSize(final T[] factors, final Object... keys);


    MultiSeries pointSize(final double[] factors, final Object... keys);


    MultiSeries pointSize(final int[] factors, final Object... keys);


    MultiSeries pointSize(final long[] factors, final Object... keys);


    MultiSeries pointSize(final io.deephaven.plot.datasets.data.IndexableData<java.lang.Double> factors, final Object... keys);


    MultiSeries pointSize(final io.deephaven.engine.table.Table t, final java.lang.String columnName, final Object... keys);


    MultiSeries pointSize(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String columnName, final Object... keys);



}