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

    <COLOR extends io.deephaven.gui.color.Paint> MultiSeries pointColor(final groovy.lang.Closure<COLOR> pointColor, final Object... keys);


    <COLOR extends io.deephaven.gui.color.Paint> MultiSeries pointColor(final java.util.function.Function<java.lang.Comparable, COLOR> pointColor, final Object... keys);


    <COLOR extends java.lang.Integer> MultiSeries pointColorInteger(final groovy.lang.Closure<COLOR> colors, final Object... keys);


    <COLOR extends java.lang.Integer> MultiSeries pointColorInteger(final java.util.function.Function<java.lang.Comparable, COLOR> colors, final Object... keys);


    <LABEL> MultiSeries pointLabel(final groovy.lang.Closure<LABEL> pointLabels, final Object... keys);


    <LABEL> MultiSeries pointLabel(final java.util.function.Function<java.lang.Comparable, LABEL> pointLabels, final Object... keys);


    MultiSeries pointShape(final groovy.lang.Closure<java.lang.String> pointShapes, final Object... keys);


    MultiSeries pointShape(final java.util.function.Function<java.lang.Comparable, java.lang.String> pointShapes, final Object... keys);


    <NUMBER extends java.lang.Number> MultiSeries pointSize(final groovy.lang.Closure<NUMBER> pointSizes, final Object... keys);


    <NUMBER extends java.lang.Number> MultiSeries pointSize(final java.util.function.Function<java.lang.Comparable, NUMBER> pointSizes, final Object... keys);


    MultiSeries errorBarColor(final java.lang.String errorBarColor, final Object... keys);


    MultiSeries errorBarColor(final int errorBarColor, final Object... keys);


    MultiSeries errorBarColor(final io.deephaven.gui.color.Paint errorBarColor, final Object... keys);


    MultiSeries gradientVisible(final boolean gradientVisible, final Object... keys);


    MultiSeries lineColor(final java.lang.String color, final Object... keys);


    MultiSeries lineColor(final int color, final Object... keys);


    MultiSeries lineColor(final io.deephaven.gui.color.Paint color, final Object... keys);


    MultiSeries lineStyle(final io.deephaven.plot.LineStyle lineStyle, final Object... keys);


    MultiSeries linesVisible(final java.lang.Boolean visible, final Object... keys);


    MultiSeries pointColor(final java.lang.String pointColor, final Object... keys);


    MultiSeries pointColor(final int pointColor, final Object... keys);


    MultiSeries pointColor(final io.deephaven.gui.color.Paint pointColor, final Object... keys);


    MultiSeries pointLabel(final java.lang.Object pointLabel, final Object... keys);


    MultiSeries pointLabelFormat(final java.lang.String pointLabelFormat, final Object... keys);


    MultiSeries pointShape(final java.lang.String pointShape, final Object... keys);


    MultiSeries pointShape(final io.deephaven.gui.shape.Shape pointShape, final Object... keys);


    MultiSeries pointSize(final java.lang.Number pointSize, final Object... keys);


    MultiSeries pointsVisible(final java.lang.Boolean visible, final Object... keys);


    MultiSeries seriesColor(final java.lang.String color, final Object... keys);


    MultiSeries seriesColor(final int color, final Object... keys);


    MultiSeries seriesColor(final io.deephaven.gui.color.Paint color, final Object... keys);


    MultiSeries toolTipPattern(final java.lang.String toolTipPattern, final Object... keys);


    MultiSeries xToolTipPattern(final java.lang.String xToolTipPattern, final Object... keys);


    MultiSeries yToolTipPattern(final java.lang.String yToolTipPattern, final Object... keys);


    MultiSeries group(final int group, final Object... keys);


    MultiSeries piePercentLabelFormat(final java.lang.String pieLabelFormat, final Object... keys);


    <CATEGORY extends java.lang.Comparable, COLOR extends io.deephaven.gui.color.Paint> MultiSeries pointColor(final java.util.Map<CATEGORY, COLOR> pointColor, final Object... keys);


    MultiSeries pointColor(final java.lang.Comparable category, final java.lang.String pointColor, final Object... keys);


    MultiSeries pointColor(final java.lang.Comparable category, final int pointColor, final Object... keys);


    MultiSeries pointColor(final java.lang.Comparable category, final io.deephaven.gui.color.Paint color, final Object... keys);


    MultiSeries pointColor(final io.deephaven.engine.table.Table t, final java.lang.String category, final java.lang.String pointColor, final Object... keys);


    MultiSeries pointColor(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String category, final java.lang.String pointColor, final Object... keys);


    <CATEGORY extends java.lang.Comparable, COLOR extends java.lang.Integer> MultiSeries pointColorInteger(final java.util.Map<CATEGORY, COLOR> colors, final Object... keys);


    <CATEGORY extends java.lang.Comparable, LABEL> MultiSeries pointLabel(final java.util.Map<CATEGORY, LABEL> pointLabels, final Object... keys);


    MultiSeries pointLabel(final java.lang.Comparable category, final java.lang.Object pointLabel, final Object... keys);


    MultiSeries pointLabel(final io.deephaven.engine.table.Table t, final java.lang.String category, final java.lang.String pointLabel, final Object... keys);


    MultiSeries pointLabel(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String category, final java.lang.String pointLabel, final Object... keys);


    <CATEGORY extends java.lang.Comparable> MultiSeries pointShape(final java.util.Map<CATEGORY, java.lang.String> pointShapes, final Object... keys);


    MultiSeries pointShape(final java.lang.Comparable category, final java.lang.String pointShape, final Object... keys);


    MultiSeries pointShape(final java.lang.Comparable category, final io.deephaven.gui.shape.Shape pointShape, final Object... keys);


    MultiSeries pointShape(final io.deephaven.engine.table.Table t, final java.lang.String category, final java.lang.String pointShape, final Object... keys);


    MultiSeries pointShape(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String category, final java.lang.String pointShape, final Object... keys);


    <CATEGORY extends java.lang.Comparable, NUMBER extends java.lang.Number> MultiSeries pointSize(final java.util.Map<CATEGORY, NUMBER> pointSizes, final Object... keys);


    <CATEGORY extends java.lang.Comparable, NUMBER extends java.lang.Number> MultiSeries pointSize(final CATEGORY[] categories, final NUMBER[] pointSizes, final Object... keys);


    <CATEGORY extends java.lang.Comparable> MultiSeries pointSize(final CATEGORY[] categories, final double[] pointSizes, final Object... keys);


    <CATEGORY extends java.lang.Comparable> MultiSeries pointSize(final CATEGORY[] categories, final int[] pointSizes, final Object... keys);


    <CATEGORY extends java.lang.Comparable> MultiSeries pointSize(final CATEGORY[] categories, final long[] pointSizes, final Object... keys);


    MultiSeries pointSize(final java.lang.Comparable category, final java.lang.Number pointSize, final Object... keys);


    MultiSeries pointSize(final java.lang.Comparable category, final double pointSize, final Object... keys);


    MultiSeries pointSize(final java.lang.Comparable category, final int pointSize, final Object... keys);


    MultiSeries pointSize(final java.lang.Comparable category, final long pointSize, final Object... keys);


    MultiSeries pointSize(final io.deephaven.engine.table.Table t, final java.lang.String category, final java.lang.String pointSize, final Object... keys);


    MultiSeries pointSize(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String category, final java.lang.String pointSize, final Object... keys);


    MultiSeries pointColor(final int[] pointColors, final Object... keys);


    MultiSeries pointColor(final io.deephaven.gui.color.Paint[] pointColor, final Object... keys);


    MultiSeries pointColor(final java.lang.Integer[] pointColors, final Object... keys);


    MultiSeries pointColor(final java.lang.String[] pointColors, final Object... keys);


    <T extends io.deephaven.gui.color.Paint> MultiSeries pointColor(final io.deephaven.plot.datasets.data.IndexableData<T> pointColor, final Object... keys);


    MultiSeries pointColor(final io.deephaven.engine.table.Table t, final java.lang.String pointColors, final Object... keys);


    MultiSeries pointColor(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String pointColors, final Object... keys);


    MultiSeries pointColorInteger(final io.deephaven.plot.datasets.data.IndexableData<java.lang.Integer> colors, final Object... keys);


    MultiSeries pointLabel(final java.lang.Object[] pointLabels, final Object... keys);


    MultiSeries pointLabel(final io.deephaven.plot.datasets.data.IndexableData<?> pointLabels, final Object... keys);


    MultiSeries pointLabel(final io.deephaven.engine.table.Table t, final java.lang.String pointLabel, final Object... keys);


    MultiSeries pointLabel(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String pointLabel, final Object... keys);


    MultiSeries pointShape(final io.deephaven.gui.shape.Shape[] pointShapes, final Object... keys);


    MultiSeries pointShape(final java.lang.String[] pointShapes, final Object... keys);


    MultiSeries pointShape(final io.deephaven.plot.datasets.data.IndexableData<java.lang.String> pointShapes, final Object... keys);


    MultiSeries pointShape(final io.deephaven.engine.table.Table t, final java.lang.String pointShape, final Object... keys);


    MultiSeries pointShape(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String pointShape, final Object... keys);


    <T extends java.lang.Number> MultiSeries pointSize(final T[] pointSizes, final Object... keys);


    MultiSeries pointSize(final double[] pointSizes, final Object... keys);


    MultiSeries pointSize(final int[] pointSizes, final Object... keys);


    MultiSeries pointSize(final long[] pointSizes, final Object... keys);


    MultiSeries pointSize(final io.deephaven.plot.datasets.data.IndexableData<java.lang.Double> pointSizes, final Object... keys);


    MultiSeries pointSize(final io.deephaven.engine.table.Table t, final java.lang.String pointSizes, final Object... keys);


    MultiSeries pointSize(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String pointSize, final Object... keys);



}