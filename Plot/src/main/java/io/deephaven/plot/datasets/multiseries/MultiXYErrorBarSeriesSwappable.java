/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.multiseries;

import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.datasets.DynamicSeriesNamer;
import io.deephaven.plot.datasets.xyerrorbar.XYErrorBarDataSeriesInternal;
import io.deephaven.plot.datasets.xyerrorbar.XYErrorBarDataSeriesTableArray;
import io.deephaven.plot.filters.SelectableDataSetSwappableTable;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.plot.util.tables.SwappableTable;
import io.deephaven.plot.util.tables.TableHandle;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;

/**
 * XY MultiSeries suitable for error bar plots.
 */
public class MultiXYErrorBarSeriesSwappable extends AbstractSwappableMultiSeries<XYErrorBarDataSeriesInternal> {
    private static final long serialVersionUID = -8775590556717562765L;
    private final String x;
    private final String xLow;
    private final String xHigh;
    private final String y;
    private final String yLow;
    private final String yHigh;
    private final boolean drawXError;
    private final boolean drawYError;

    /**
     * Creates a MultiXYSeries instance.
     *
     * @param axes axes on which this multiseries will be plotted
     * @param id data series id
     * @param name series name
     * @param swappableTable table data
     * @param x column in {@code t} that holds the x-variable data
     * @param yLow column in {@code t} that holds the y-variable data
     * @param byColumns column(s) in {@code t} that holds the grouping data
     */
    public MultiXYErrorBarSeriesSwappable(final AxesImpl axes, final int id, final Comparable name,
            SwappableTable swappableTable, final String x, final String xLow, final String xHigh, final String y,
            final String yLow, final String yHigh, final String[] byColumns, final boolean drawXError,
            final boolean drawYError) {
        super(axes, id, name, swappableTable, x, y, byColumns);
        ArgumentValidations.assertIsNumericOrTime(swappableTable.getTableDefinition(), x, getPlotInfo());
        ArgumentValidations.assertIsNumericOrTime(swappableTable.getTableDefinition(), y, getPlotInfo());

        if (drawXError) {
            ArgumentValidations.assertIsNumericOrTime(swappableTable.getTableDefinition(), xLow, getPlotInfo());
            ArgumentValidations.assertIsNumericOrTime(swappableTable.getTableDefinition(), xHigh, getPlotInfo());
        }

        if (drawYError) {
            ArgumentValidations.assertIsNumericOrTime(swappableTable.getTableDefinition(), yLow, getPlotInfo());
            ArgumentValidations.assertIsNumericOrTime(swappableTable.getTableDefinition(), yHigh, getPlotInfo());
        }

        this.x = x;
        this.xLow = xLow;
        this.xHigh = xHigh;
        this.y = y;
        this.yLow = yLow;
        this.yHigh = yHigh;
        this.drawXError = drawXError;
        this.drawYError = drawYError;
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    private MultiXYErrorBarSeriesSwappable(final MultiXYErrorBarSeriesSwappable series, final AxesImpl axes) {
        super(series, axes);
        this.x = series.x;
        this.xLow = series.xLow;
        this.xHigh = series.xHigh;
        this.y = series.y;
        this.yLow = series.yLow;
        this.yHigh = series.yHigh;
        this.drawXError = series.drawXError;
        this.drawYError = series.drawYError;
    }

    @Override
    public XYErrorBarDataSeriesInternal createSeries(String seriesName, final BaseTable t,
            final DynamicSeriesNamer seriesNamer) {
        seriesName = makeSeriesName(seriesName, seriesNamer);

        final TableHandle tableHandle = new TableHandle(t, x, xLow, xHigh, y, yLow, yHigh);
        addTableHandle(tableHandle);

        final XYErrorBarDataSeriesTableArray series = new XYErrorBarDataSeriesTableArray(axes(), -1, seriesName,
                tableHandle, x, xLow, xHigh, y, yLow, yHigh,
                drawXError, drawYError);

        series.addSwappableTable(getSwappableTable());

        return series;
    }


    ////////////////////////////// CODE BELOW HERE IS GENERATED -- DO NOT EDIT BY HAND //////////////////////////////
    ////////////////////////////// TO REGENERATE RUN GenerateMultiSeries //////////////////////////////
    ////////////////////////////// AND THEN RUN GenerateFigureImmutable //////////////////////////////

    @Override public void initializeSeries(XYErrorBarDataSeriesInternal series) {
        $$initializeSeries$$(series);
    }

    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> errorBarColorSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> errorBarColorSeriesNameToStringMap() {
        return errorBarColorSeriesNameToStringMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable errorBarColor(final java.lang.String errorBarColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            errorBarColorSeriesNameToStringMap.setDefault(errorBarColor);
        } else {
            errorBarColorSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                errorBarColor);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> errorBarColorSeriesNameTointMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> errorBarColorSeriesNameTointMap() {
        return errorBarColorSeriesNameTointMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable errorBarColor(final int errorBarColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            errorBarColorSeriesNameTointMap.setDefault(errorBarColor);
        } else {
            errorBarColorSeriesNameTointMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                errorBarColor);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> errorBarColorSeriesNameToPaintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> errorBarColorSeriesNameToPaintMap() {
        return errorBarColorSeriesNameToPaintMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable errorBarColor(final io.deephaven.gui.color.Paint errorBarColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            errorBarColorSeriesNameToPaintMap.setDefault(errorBarColor);
        } else {
            errorBarColorSeriesNameToPaintMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                errorBarColor);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> gradientVisibleSeriesNameTobooleanMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> gradientVisibleSeriesNameTobooleanMap() {
        return gradientVisibleSeriesNameTobooleanMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable gradientVisible(final boolean gradientVisible, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            gradientVisibleSeriesNameTobooleanMap.setDefault(gradientVisible);
        } else {
            gradientVisibleSeriesNameTobooleanMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                gradientVisible);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> lineColorSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> lineColorSeriesNameToStringMap() {
        return lineColorSeriesNameToStringMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable lineColor(final java.lang.String color, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            lineColorSeriesNameToStringMap.setDefault(color);
        } else {
            lineColorSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> lineColorSeriesNameTointMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> lineColorSeriesNameTointMap() {
        return lineColorSeriesNameTointMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable lineColor(final int color, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            lineColorSeriesNameTointMap.setDefault(color);
        } else {
            lineColorSeriesNameTointMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> lineColorSeriesNameToPaintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> lineColorSeriesNameToPaintMap() {
        return lineColorSeriesNameToPaintMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable lineColor(final io.deephaven.gui.color.Paint color, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            lineColorSeriesNameToPaintMap.setDefault(color);
        } else {
            lineColorSeriesNameToPaintMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.plot.LineStyle> lineStyleSeriesNameToLineStyleMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.plot.LineStyle> lineStyleSeriesNameToLineStyleMap() {
        return lineStyleSeriesNameToLineStyleMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable lineStyle(final io.deephaven.plot.LineStyle lineStyle, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            lineStyleSeriesNameToLineStyleMap.setDefault(lineStyle);
        } else {
            lineStyleSeriesNameToLineStyleMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                lineStyle);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> linesVisibleSeriesNameToBooleanMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> linesVisibleSeriesNameToBooleanMap() {
        return linesVisibleSeriesNameToBooleanMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable linesVisible(final java.lang.Boolean visible, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            linesVisibleSeriesNameToBooleanMap.setDefault(visible);
        } else {
            linesVisibleSeriesNameToBooleanMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                visible);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, int[]> pointColorSeriesNameTointArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, int[]> pointColorSeriesNameTointArrayMap() {
        return pointColorSeriesNameTointArrayMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointColor(final int[] pointColors, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameTointArrayMap.setDefault(pointColors);
        } else {
            pointColorSeriesNameTointArrayMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointColors);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint[]> pointColorSeriesNameToPaintArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint[]> pointColorSeriesNameToPaintArrayMap() {
        return pointColorSeriesNameToPaintArrayMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointColor(final io.deephaven.gui.color.Paint[] pointColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameToPaintArrayMap.setDefault(pointColor);
        } else {
            pointColorSeriesNameToPaintArrayMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointColor);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer[]> pointColorSeriesNameToIntegerArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer[]> pointColorSeriesNameToIntegerArrayMap() {
        return pointColorSeriesNameToIntegerArrayMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointColor(final java.lang.Integer[] pointColors, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameToIntegerArrayMap.setDefault(pointColors);
        } else {
            pointColorSeriesNameToIntegerArrayMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointColors);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String[]> pointColorSeriesNameToStringArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String[]> pointColorSeriesNameToStringArrayMap() {
        return pointColorSeriesNameToStringArrayMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointColor(final java.lang.String[] pointColors, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameToStringArrayMap.setDefault(pointColors);
        } else {
            pointColorSeriesNameToStringArrayMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointColors);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointColorSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointColorSeriesNameToStringMap() {
        return pointColorSeriesNameToStringMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointColor(final java.lang.String pointColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameToStringMap.setDefault(pointColor);
        } else {
            pointColorSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointColor);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> pointColorSeriesNameTointMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> pointColorSeriesNameTointMap() {
        return pointColorSeriesNameTointMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointColor(final int pointColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameTointMap.setDefault(pointColor);
        } else {
            pointColorSeriesNameTointMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointColor);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> pointColorSeriesNameToPaintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> pointColorSeriesNameToPaintMap() {
        return pointColorSeriesNameToPaintMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointColor(final io.deephaven.gui.color.Paint pointColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameToPaintMap.setDefault(pointColor);
        } else {
            pointColorSeriesNameToPaintMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointColor);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.plot.datasets.data.IndexableData> pointColorSeriesNameToIndexableDataMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.plot.datasets.data.IndexableData> pointColorSeriesNameToIndexableDataMap() {
        return pointColorSeriesNameToIndexableDataMap;
    }
    @Override public <T extends io.deephaven.gui.color.Paint> MultiXYErrorBarSeriesSwappable pointColor(final io.deephaven.plot.datasets.data.IndexableData<T> pointColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameToIndexableDataMap.setDefault(pointColor);
        } else {
            pointColorSeriesNameToIndexableDataMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointColor);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToTableStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToTableStringMap() {
        return pointColorSeriesNameToTableStringMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointColor(final io.deephaven.engine.table.Table t, final java.lang.String pointColors, final Object... multiSeriesKey) {
    final io.deephaven.plot.util.tables.TableHandle tHandle = new io.deephaven.plot.util.tables.TableHandle(t, pointColors);
    addTableHandle(tHandle);
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameToTableStringMap.setDefault(new Object[]{tHandle, pointColors});
        } else {
            pointColorSeriesNameToTableStringMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                new Object[]{ tHandle, pointColors});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToSelectableDataSetStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToSelectableDataSetStringMap() {
        return pointColorSeriesNameToSelectableDataSetStringMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointColor(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String pointColors, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameToSelectableDataSetStringMap.setDefault(new Object[]{sds, pointColors});
        } else {
            pointColorSeriesNameToSelectableDataSetStringMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                new Object[]{ sds, pointColors});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.plot.datasets.data.IndexableData> pointColorIntegerSeriesNameToIndexableDataMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.plot.datasets.data.IndexableData> pointColorIntegerSeriesNameToIndexableDataMap() {
        return pointColorIntegerSeriesNameToIndexableDataMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointColorInteger(final io.deephaven.plot.datasets.data.IndexableData<java.lang.Integer> colors, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorIntegerSeriesNameToIndexableDataMap.setDefault(colors);
        } else {
            pointColorIntegerSeriesNameToIndexableDataMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                colors);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToObjectArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToObjectArrayMap() {
        return pointLabelSeriesNameToObjectArrayMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointLabel(final java.lang.Object[] pointLabels, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointLabelSeriesNameToObjectArrayMap.setDefault(new Object[]{pointLabels});
        } else {
            pointLabelSeriesNameToObjectArrayMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                new Object[]{pointLabels});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object> pointLabelSeriesNameToObjectMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object> pointLabelSeriesNameToObjectMap() {
        return pointLabelSeriesNameToObjectMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointLabel(final java.lang.Object pointLabel, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointLabelSeriesNameToObjectMap.setDefault(pointLabel);
        } else {
            pointLabelSeriesNameToObjectMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointLabel);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.plot.datasets.data.IndexableData> pointLabelSeriesNameToIndexableDataMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.plot.datasets.data.IndexableData> pointLabelSeriesNameToIndexableDataMap() {
        return pointLabelSeriesNameToIndexableDataMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointLabel(final io.deephaven.plot.datasets.data.IndexableData<?> pointLabels, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointLabelSeriesNameToIndexableDataMap.setDefault(pointLabels);
        } else {
            pointLabelSeriesNameToIndexableDataMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointLabels);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToTableStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToTableStringMap() {
        return pointLabelSeriesNameToTableStringMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointLabel(final io.deephaven.engine.table.Table t, final java.lang.String pointLabel, final Object... multiSeriesKey) {
    final io.deephaven.plot.util.tables.TableHandle tHandle = new io.deephaven.plot.util.tables.TableHandle(t, pointLabel);
    addTableHandle(tHandle);
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointLabelSeriesNameToTableStringMap.setDefault(new Object[]{tHandle, pointLabel});
        } else {
            pointLabelSeriesNameToTableStringMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                new Object[]{ tHandle, pointLabel});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToSelectableDataSetStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToSelectableDataSetStringMap() {
        return pointLabelSeriesNameToSelectableDataSetStringMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointLabel(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String pointLabel, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointLabelSeriesNameToSelectableDataSetStringMap.setDefault(new Object[]{sds, pointLabel});
        } else {
            pointLabelSeriesNameToSelectableDataSetStringMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                new Object[]{ sds, pointLabel});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointLabelFormatSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointLabelFormatSeriesNameToStringMap() {
        return pointLabelFormatSeriesNameToStringMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointLabelFormat(final java.lang.String pointLabelFormat, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointLabelFormatSeriesNameToStringMap.setDefault(pointLabelFormat);
        } else {
            pointLabelFormatSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointLabelFormat);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.shape.Shape[]> pointShapeSeriesNameToShapeArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.shape.Shape[]> pointShapeSeriesNameToShapeArrayMap() {
        return pointShapeSeriesNameToShapeArrayMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointShape(final io.deephaven.gui.shape.Shape[] pointShapes, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointShapeSeriesNameToShapeArrayMap.setDefault(pointShapes);
        } else {
            pointShapeSeriesNameToShapeArrayMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointShapes);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String[]> pointShapeSeriesNameToStringArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String[]> pointShapeSeriesNameToStringArrayMap() {
        return pointShapeSeriesNameToStringArrayMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointShape(final java.lang.String[] pointShapes, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointShapeSeriesNameToStringArrayMap.setDefault(pointShapes);
        } else {
            pointShapeSeriesNameToStringArrayMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointShapes);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointShapeSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointShapeSeriesNameToStringMap() {
        return pointShapeSeriesNameToStringMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointShape(final java.lang.String pointShape, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointShapeSeriesNameToStringMap.setDefault(pointShape);
        } else {
            pointShapeSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointShape);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.shape.Shape> pointShapeSeriesNameToShapeMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.shape.Shape> pointShapeSeriesNameToShapeMap() {
        return pointShapeSeriesNameToShapeMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointShape(final io.deephaven.gui.shape.Shape pointShape, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointShapeSeriesNameToShapeMap.setDefault(pointShape);
        } else {
            pointShapeSeriesNameToShapeMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointShape);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.plot.datasets.data.IndexableData> pointShapeSeriesNameToIndexableDataMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.plot.datasets.data.IndexableData> pointShapeSeriesNameToIndexableDataMap() {
        return pointShapeSeriesNameToIndexableDataMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointShape(final io.deephaven.plot.datasets.data.IndexableData<java.lang.String> pointShapes, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointShapeSeriesNameToIndexableDataMap.setDefault(pointShapes);
        } else {
            pointShapeSeriesNameToIndexableDataMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointShapes);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToTableStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToTableStringMap() {
        return pointShapeSeriesNameToTableStringMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointShape(final io.deephaven.engine.table.Table t, final java.lang.String pointShape, final Object... multiSeriesKey) {
    final io.deephaven.plot.util.tables.TableHandle tHandle = new io.deephaven.plot.util.tables.TableHandle(t, pointShape);
    addTableHandle(tHandle);
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointShapeSeriesNameToTableStringMap.setDefault(new Object[]{tHandle, pointShape});
        } else {
            pointShapeSeriesNameToTableStringMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                new Object[]{ tHandle, pointShape});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToSelectableDataSetStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToSelectableDataSetStringMap() {
        return pointShapeSeriesNameToSelectableDataSetStringMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointShape(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String pointShape, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointShapeSeriesNameToSelectableDataSetStringMap.setDefault(new Object[]{sds, pointShape});
        } else {
            pointShapeSeriesNameToSelectableDataSetStringMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                new Object[]{ sds, pointShape});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object> pointSizeSeriesNameToTArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object> pointSizeSeriesNameToTArrayMap() {
        return pointSizeSeriesNameToTArrayMap;
    }
    @Override public <T extends java.lang.Number> MultiXYErrorBarSeriesSwappable pointSize(final T[] pointSizes, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameToTArrayMap.setDefault(pointSizes);
        } else {
            pointSizeSeriesNameToTArrayMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointSizes);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, double[]> pointSizeSeriesNameTodoubleArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, double[]> pointSizeSeriesNameTodoubleArrayMap() {
        return pointSizeSeriesNameTodoubleArrayMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointSize(final double[] pointSizes, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameTodoubleArrayMap.setDefault(pointSizes);
        } else {
            pointSizeSeriesNameTodoubleArrayMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointSizes);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, int[]> pointSizeSeriesNameTointArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, int[]> pointSizeSeriesNameTointArrayMap() {
        return pointSizeSeriesNameTointArrayMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointSize(final int[] pointSizes, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameTointArrayMap.setDefault(pointSizes);
        } else {
            pointSizeSeriesNameTointArrayMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointSizes);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, long[]> pointSizeSeriesNameTolongArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, long[]> pointSizeSeriesNameTolongArrayMap() {
        return pointSizeSeriesNameTolongArrayMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointSize(final long[] pointSizes, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameTolongArrayMap.setDefault(pointSizes);
        } else {
            pointSizeSeriesNameTolongArrayMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointSizes);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Number> pointSizeSeriesNameToNumberMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Number> pointSizeSeriesNameToNumberMap() {
        return pointSizeSeriesNameToNumberMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointSize(final java.lang.Number pointSize, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameToNumberMap.setDefault(pointSize);
        } else {
            pointSizeSeriesNameToNumberMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointSize);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.plot.datasets.data.IndexableData> pointSizeSeriesNameToIndexableDataMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.plot.datasets.data.IndexableData> pointSizeSeriesNameToIndexableDataMap() {
        return pointSizeSeriesNameToIndexableDataMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointSize(final io.deephaven.plot.datasets.data.IndexableData<java.lang.Double> pointSizes, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameToIndexableDataMap.setDefault(pointSizes);
        } else {
            pointSizeSeriesNameToIndexableDataMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                pointSizes);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToTableStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToTableStringMap() {
        return pointSizeSeriesNameToTableStringMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointSize(final io.deephaven.engine.table.Table t, final java.lang.String pointSizes, final Object... multiSeriesKey) {
    final io.deephaven.plot.util.tables.TableHandle tHandle = new io.deephaven.plot.util.tables.TableHandle(t, pointSizes);
    addTableHandle(tHandle);
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameToTableStringMap.setDefault(new Object[]{tHandle, pointSizes});
        } else {
            pointSizeSeriesNameToTableStringMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                new Object[]{ tHandle, pointSizes});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToSelectableDataSetStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToSelectableDataSetStringMap() {
        return pointSizeSeriesNameToSelectableDataSetStringMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointSize(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String pointSize, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameToSelectableDataSetStringMap.setDefault(new Object[]{sds, pointSize});
        } else {
            pointSizeSeriesNameToSelectableDataSetStringMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                new Object[]{ sds, pointSize});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> pointsVisibleSeriesNameToBooleanMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> pointsVisibleSeriesNameToBooleanMap() {
        return pointsVisibleSeriesNameToBooleanMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable pointsVisible(final java.lang.Boolean visible, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointsVisibleSeriesNameToBooleanMap.setDefault(visible);
        } else {
            pointsVisibleSeriesNameToBooleanMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                visible);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> seriesColorSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> seriesColorSeriesNameToStringMap() {
        return seriesColorSeriesNameToStringMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable seriesColor(final java.lang.String color, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            seriesColorSeriesNameToStringMap.setDefault(color);
        } else {
            seriesColorSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> seriesColorSeriesNameTointMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> seriesColorSeriesNameTointMap() {
        return seriesColorSeriesNameTointMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable seriesColor(final int color, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            seriesColorSeriesNameTointMap.setDefault(color);
        } else {
            seriesColorSeriesNameTointMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> seriesColorSeriesNameToPaintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> seriesColorSeriesNameToPaintMap() {
        return seriesColorSeriesNameToPaintMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable seriesColor(final io.deephaven.gui.color.Paint color, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            seriesColorSeriesNameToPaintMap.setDefault(color);
        } else {
            seriesColorSeriesNameToPaintMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> toolTipPatternSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> toolTipPatternSeriesNameToStringMap() {
        return toolTipPatternSeriesNameToStringMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable toolTipPattern(final java.lang.String toolTipPattern, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            toolTipPatternSeriesNameToStringMap.setDefault(toolTipPattern);
        } else {
            toolTipPatternSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                toolTipPattern);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> xToolTipPatternSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> xToolTipPatternSeriesNameToStringMap() {
        return xToolTipPatternSeriesNameToStringMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable xToolTipPattern(final java.lang.String xToolTipPattern, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            xToolTipPatternSeriesNameToStringMap.setDefault(xToolTipPattern);
        } else {
            xToolTipPatternSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                xToolTipPattern);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> yToolTipPatternSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> yToolTipPatternSeriesNameToStringMap() {
        return yToolTipPatternSeriesNameToStringMap;
    }
    @Override public MultiXYErrorBarSeriesSwappable yToolTipPattern(final java.lang.String yToolTipPattern, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            yToolTipPatternSeriesNameToStringMap.setDefault(yToolTipPattern);
        } else {
            yToolTipPatternSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey.length == 1 ? multiSeriesKey[0] : new io.deephaven.datastructures.util.SmartKey(multiSeriesKey)), 
                yToolTipPattern);
        }

        return this;
    }



    @SuppressWarnings("unchecked") 
    private <T extends io.deephaven.gui.color.Paint, T0 extends java.lang.Number> void $$initializeSeries$$(XYErrorBarDataSeriesInternal series) {
        String name = series.name().toString();
        java.util.function.Consumer<java.lang.Object[]> consumer0 = series::pointLabel;
        pointLabelSeriesNameToObjectArrayMap.runIfKeyExistsCast(consumer0, name);
        java.util.function.Consumer<java.lang.String> consumer1 = series::pointLabelFormat;
        pointLabelFormatSeriesNameToStringMap.runIfKeyExistsCast(consumer1, name);
        java.util.function.Consumer<io.deephaven.plot.datasets.data.IndexableData> consumer2 = series::pointShape;
        pointShapeSeriesNameToIndexableDataMap.runIfKeyExistsCast(consumer2, name);
        java.util.function.Consumer<java.lang.String[]> consumer3 = series::pointShape;
        pointShapeSeriesNameToStringArrayMap.runIfKeyExistsCast(consumer3, name);
        java.lang.Object[]         objectArray = pointColorSeriesNameToTableStringMap.get(name);
        if(objectArray != null) {series.pointColor(((io.deephaven.plot.util.tables.TableHandle) objectArray[0]).getTable(), (java.lang.String) objectArray[1]);}

        objectArray = pointSizeSeriesNameToSelectableDataSetStringMap.get(name);
        if(objectArray != null) {series.pointSize((io.deephaven.plot.filters.SelectableDataSet) objectArray[0], (java.lang.String) objectArray[1]);}

        java.util.function.Consumer<java.lang.Boolean> consumer4 = series::pointsVisible;
        pointsVisibleSeriesNameToBooleanMap.runIfKeyExistsCast(consumer4, name);
        java.util.function.Consumer<io.deephaven.plot.datasets.data.IndexableData> consumer5 = series::pointColor;
        pointColorSeriesNameToIndexableDataMap.runIfKeyExistsCast(consumer5, name);
        objectArray = pointShapeSeriesNameToSelectableDataSetStringMap.get(name);
        if(objectArray != null) {series.pointShape((io.deephaven.plot.filters.SelectableDataSet) objectArray[0], (java.lang.String) objectArray[1]);}

        objectArray = pointLabelSeriesNameToTableStringMap.get(name);
        if(objectArray != null) {series.pointLabel(((io.deephaven.plot.util.tables.TableHandle) objectArray[0]).getTable(), (java.lang.String) objectArray[1]);}

        java.util.function.Consumer<java.lang.String> consumer6 = series::pointColor;
        pointColorSeriesNameToStringMap.runIfKeyExistsCast(consumer6, name);
        java.util.function.Consumer<io.deephaven.gui.shape.Shape> consumer7 = series::pointShape;
        pointShapeSeriesNameToShapeMap.runIfKeyExistsCast(consumer7, name);
        java.util.function.Consumer<java.lang.Integer> consumer8 = series::lineColor;
        lineColorSeriesNameTointMap.runIfKeyExistsCast(consumer8, name);
        java.util.function.Consumer<java.lang.Boolean> consumer9 = series::linesVisible;
        linesVisibleSeriesNameToBooleanMap.runIfKeyExistsCast(consumer9, name);
        objectArray = pointLabelSeriesNameToSelectableDataSetStringMap.get(name);
        if(objectArray != null) {series.pointLabel((io.deephaven.plot.filters.SelectableDataSet) objectArray[0], (java.lang.String) objectArray[1]);}

        java.util.function.Consumer<java.lang.String> consumer10 = series::lineColor;
        lineColorSeriesNameToStringMap.runIfKeyExistsCast(consumer10, name);
        java.util.function.Consumer<io.deephaven.gui.color.Paint> consumer11 = series::seriesColor;
        seriesColorSeriesNameToPaintMap.runIfKeyExistsCast(consumer11, name);
        java.util.function.Consumer<long[]> consumer12 = series::pointSize;
        pointSizeSeriesNameTolongArrayMap.runIfKeyExistsCast(consumer12, name);
        java.util.function.Consumer<io.deephaven.gui.shape.Shape[]> consumer13 = series::pointShape;
        pointShapeSeriesNameToShapeArrayMap.runIfKeyExistsCast(consumer13, name);
        java.util.function.Consumer<java.lang.String> consumer14 = series::yToolTipPattern;
        yToolTipPatternSeriesNameToStringMap.runIfKeyExistsCast(consumer14, name);
        java.util.function.Consumer<java.lang.Integer> consumer15 = series::seriesColor;
        seriesColorSeriesNameTointMap.runIfKeyExistsCast(consumer15, name);
        java.util.function.Consumer<int[]> consumer16 = series::pointColor;
        pointColorSeriesNameTointArrayMap.runIfKeyExistsCast(consumer16, name);
        java.util.function.Consumer<java.lang.Object> consumer17 = series::pointLabel;
        pointLabelSeriesNameToObjectMap.runIfKeyExistsCast(consumer17, name);
        java.util.function.Consumer<io.deephaven.gui.color.Paint> consumer18 = series::errorBarColor;
        errorBarColorSeriesNameToPaintMap.runIfKeyExistsCast(consumer18, name);
        objectArray = pointColorSeriesNameToSelectableDataSetStringMap.get(name);
        if(objectArray != null) {series.pointColor((io.deephaven.plot.filters.SelectableDataSet) objectArray[0], (java.lang.String) objectArray[1]);}

        java.util.function.Consumer<io.deephaven.gui.color.Paint> consumer19 = series::lineColor;
        lineColorSeriesNameToPaintMap.runIfKeyExistsCast(consumer19, name);
        java.util.function.Consumer<java.lang.String> consumer20 = series::seriesColor;
        seriesColorSeriesNameToStringMap.runIfKeyExistsCast(consumer20, name);
        java.util.function.Consumer<java.lang.Integer[]> consumer21 = series::pointColor;
        pointColorSeriesNameToIntegerArrayMap.runIfKeyExistsCast(consumer21, name);
        java.util.function.Consumer<io.deephaven.plot.LineStyle> consumer22 = series::lineStyle;
        lineStyleSeriesNameToLineStyleMap.runIfKeyExistsCast(consumer22, name);
        java.util.function.Consumer<double[]> consumer23 = series::pointSize;
        pointSizeSeriesNameTodoubleArrayMap.runIfKeyExistsCast(consumer23, name);
        java.util.function.Consumer<java.lang.Boolean> consumer24 = series::gradientVisible;
        gradientVisibleSeriesNameTobooleanMap.runIfKeyExistsCast(consumer24, name);
        java.util.function.Consumer<io.deephaven.gui.color.Paint> consumer25 = series::pointColor;
        pointColorSeriesNameToPaintMap.runIfKeyExistsCast(consumer25, name);
        java.util.function.Consumer<io.deephaven.plot.datasets.data.IndexableData> consumer26 = series::pointColorInteger;
        pointColorIntegerSeriesNameToIndexableDataMap.runIfKeyExistsCast(consumer26, name);
        java.util.function.Consumer<int[]> consumer27 = series::pointSize;
        pointSizeSeriesNameTointArrayMap.runIfKeyExistsCast(consumer27, name);
        java.util.function.Consumer<java.lang.String[]> consumer28 = series::pointColor;
        pointColorSeriesNameToStringArrayMap.runIfKeyExistsCast(consumer28, name);
        java.util.function.Consumer<T0[]> consumer29 = series::pointSize;
        pointSizeSeriesNameToTArrayMap.runIfKeyExistsCast(consumer29, name);
        java.util.function.Consumer<io.deephaven.plot.datasets.data.IndexableData> consumer30 = series::pointLabel;
        pointLabelSeriesNameToIndexableDataMap.runIfKeyExistsCast(consumer30, name);
        objectArray = pointShapeSeriesNameToTableStringMap.get(name);
        if(objectArray != null) {series.pointShape(((io.deephaven.plot.util.tables.TableHandle) objectArray[0]).getTable(), (java.lang.String) objectArray[1]);}

        java.util.function.Consumer<java.lang.Number> consumer31 = series::pointSize;
        pointSizeSeriesNameToNumberMap.runIfKeyExistsCast(consumer31, name);
        java.util.function.Consumer<java.lang.String> consumer32 = series::errorBarColor;
        errorBarColorSeriesNameToStringMap.runIfKeyExistsCast(consumer32, name);
        java.util.function.Consumer<java.lang.Integer> consumer33 = series::pointColor;
        pointColorSeriesNameTointMap.runIfKeyExistsCast(consumer33, name);
        java.util.function.Consumer<io.deephaven.plot.datasets.data.IndexableData> consumer34 = series::pointSize;
        pointSizeSeriesNameToIndexableDataMap.runIfKeyExistsCast(consumer34, name);
        java.util.function.Consumer<java.lang.String> consumer35 = series::xToolTipPattern;
        xToolTipPatternSeriesNameToStringMap.runIfKeyExistsCast(consumer35, name);
        java.util.function.Consumer<java.lang.Integer> consumer36 = series::errorBarColor;
        errorBarColorSeriesNameTointMap.runIfKeyExistsCast(consumer36, name);
        java.util.function.Consumer<io.deephaven.gui.color.Paint[]> consumer37 = series::pointColor;
        pointColorSeriesNameToPaintArrayMap.runIfKeyExistsCast(consumer37, name);
        objectArray = pointSizeSeriesNameToTableStringMap.get(name);
        if(objectArray != null) {series.pointSize(((io.deephaven.plot.util.tables.TableHandle) objectArray[0]).getTable(), (java.lang.String) objectArray[1]);}

        java.util.function.Consumer<java.lang.String> consumer38 = series::pointShape;
        pointShapeSeriesNameToStringMap.runIfKeyExistsCast(consumer38, name);
        java.util.function.Consumer<java.lang.String> consumer39 = series::toolTipPattern;
        toolTipPatternSeriesNameToStringMap.runIfKeyExistsCast(consumer39, name);

    }
    @Override
    public MultiXYErrorBarSeriesSwappable copy(AxesImpl axes) {
        final MultiXYErrorBarSeriesSwappable __s__ = new MultiXYErrorBarSeriesSwappable(this, axes);
                __s__.pointLabelSeriesNameToObjectArrayMap = pointLabelSeriesNameToObjectArrayMap.copy();
        __s__.pointLabelFormatSeriesNameToStringMap = pointLabelFormatSeriesNameToStringMap.copy();
        __s__.pointShapeSeriesNameToIndexableDataMap = pointShapeSeriesNameToIndexableDataMap.copy();
        __s__.pointShapeSeriesNameToStringArrayMap = pointShapeSeriesNameToStringArrayMap.copy();
        __s__.pointColorSeriesNameToTableStringMap = pointColorSeriesNameToTableStringMap.copy();
        __s__.pointSizeSeriesNameToSelectableDataSetStringMap = pointSizeSeriesNameToSelectableDataSetStringMap.copy();
        __s__.pointsVisibleSeriesNameToBooleanMap = pointsVisibleSeriesNameToBooleanMap.copy();
        __s__.pointColorSeriesNameToIndexableDataMap = pointColorSeriesNameToIndexableDataMap.copy();
        __s__.pointShapeSeriesNameToSelectableDataSetStringMap = pointShapeSeriesNameToSelectableDataSetStringMap.copy();
        __s__.pointLabelSeriesNameToTableStringMap = pointLabelSeriesNameToTableStringMap.copy();
        __s__.pointColorSeriesNameToStringMap = pointColorSeriesNameToStringMap.copy();
        __s__.pointShapeSeriesNameToShapeMap = pointShapeSeriesNameToShapeMap.copy();
        __s__.lineColorSeriesNameTointMap = lineColorSeriesNameTointMap.copy();
        __s__.linesVisibleSeriesNameToBooleanMap = linesVisibleSeriesNameToBooleanMap.copy();
        __s__.pointLabelSeriesNameToSelectableDataSetStringMap = pointLabelSeriesNameToSelectableDataSetStringMap.copy();
        __s__.lineColorSeriesNameToStringMap = lineColorSeriesNameToStringMap.copy();
        __s__.seriesColorSeriesNameToPaintMap = seriesColorSeriesNameToPaintMap.copy();
        __s__.pointSizeSeriesNameTolongArrayMap = pointSizeSeriesNameTolongArrayMap.copy();
        __s__.pointShapeSeriesNameToShapeArrayMap = pointShapeSeriesNameToShapeArrayMap.copy();
        __s__.yToolTipPatternSeriesNameToStringMap = yToolTipPatternSeriesNameToStringMap.copy();
        __s__.seriesColorSeriesNameTointMap = seriesColorSeriesNameTointMap.copy();
        __s__.pointColorSeriesNameTointArrayMap = pointColorSeriesNameTointArrayMap.copy();
        __s__.pointLabelSeriesNameToObjectMap = pointLabelSeriesNameToObjectMap.copy();
        __s__.errorBarColorSeriesNameToPaintMap = errorBarColorSeriesNameToPaintMap.copy();
        __s__.pointColorSeriesNameToSelectableDataSetStringMap = pointColorSeriesNameToSelectableDataSetStringMap.copy();
        __s__.lineColorSeriesNameToPaintMap = lineColorSeriesNameToPaintMap.copy();
        __s__.seriesColorSeriesNameToStringMap = seriesColorSeriesNameToStringMap.copy();
        __s__.pointColorSeriesNameToIntegerArrayMap = pointColorSeriesNameToIntegerArrayMap.copy();
        __s__.lineStyleSeriesNameToLineStyleMap = lineStyleSeriesNameToLineStyleMap.copy();
        __s__.pointSizeSeriesNameTodoubleArrayMap = pointSizeSeriesNameTodoubleArrayMap.copy();
        __s__.gradientVisibleSeriesNameTobooleanMap = gradientVisibleSeriesNameTobooleanMap.copy();
        __s__.pointColorSeriesNameToPaintMap = pointColorSeriesNameToPaintMap.copy();
        __s__.pointColorIntegerSeriesNameToIndexableDataMap = pointColorIntegerSeriesNameToIndexableDataMap.copy();
        __s__.pointSizeSeriesNameTointArrayMap = pointSizeSeriesNameTointArrayMap.copy();
        __s__.pointColorSeriesNameToStringArrayMap = pointColorSeriesNameToStringArrayMap.copy();
        __s__.pointSizeSeriesNameToTArrayMap = pointSizeSeriesNameToTArrayMap.copy();
        __s__.pointLabelSeriesNameToIndexableDataMap = pointLabelSeriesNameToIndexableDataMap.copy();
        __s__.pointShapeSeriesNameToTableStringMap = pointShapeSeriesNameToTableStringMap.copy();
        __s__.pointSizeSeriesNameToNumberMap = pointSizeSeriesNameToNumberMap.copy();
        __s__.errorBarColorSeriesNameToStringMap = errorBarColorSeriesNameToStringMap.copy();
        __s__.pointColorSeriesNameTointMap = pointColorSeriesNameTointMap.copy();
        __s__.pointSizeSeriesNameToIndexableDataMap = pointSizeSeriesNameToIndexableDataMap.copy();
        __s__.xToolTipPatternSeriesNameToStringMap = xToolTipPatternSeriesNameToStringMap.copy();
        __s__.errorBarColorSeriesNameTointMap = errorBarColorSeriesNameTointMap.copy();
        __s__.pointColorSeriesNameToPaintArrayMap = pointColorSeriesNameToPaintArrayMap.copy();
        __s__.pointSizeSeriesNameToTableStringMap = pointSizeSeriesNameToTableStringMap.copy();
        __s__.pointShapeSeriesNameToStringMap = pointShapeSeriesNameToStringMap.copy();
        __s__.toolTipPatternSeriesNameToStringMap = toolTipPatternSeriesNameToStringMap.copy();
        return __s__;
    }
}