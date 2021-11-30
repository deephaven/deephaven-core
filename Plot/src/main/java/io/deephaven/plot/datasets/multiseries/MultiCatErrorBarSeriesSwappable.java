/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.multiseries;

import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.datasets.DynamicSeriesNamer;
import io.deephaven.plot.datasets.category.AbstractTableBasedCategoryDataSeries;
import io.deephaven.plot.datasets.category.CategoryTableDataSeriesInternal;
import io.deephaven.plot.datasets.categoryerrorbar.CategoryErrorBarDataSeriesKernel;
import io.deephaven.plot.datasets.categoryerrorbar.CategoryErrorBarDataSeriesInternal;
import io.deephaven.plot.filters.SelectableDataSetSwappableTable;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.plot.util.tables.SwappableTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;

import java.util.Collection;

/**
 * MultiSeries suitable for category error bar plots on a swappable data set.
 */
public class MultiCatErrorBarSeriesSwappable extends AbstractSwappableMultiSeries<CategoryErrorBarDataSeriesInternal> {

    private static final long serialVersionUID = 3180012797112976859L;
    private final String categories;
    private final String values;
    private final String yLow;
    private final String yHigh;

    /**
     * /** Creates a MultiCatErrorBarSeries instance.
     *
     * @param axes axes on which this multiseries will be plotted
     * @param id data series id
     * @param name series name
     * @param swappableTable selectable dataset
     * @param categories column in {@code t} holding discrete data
     * @param values column in {@code t} holding numeric data
     * @param byColumns column(s) in {@code t} that holds the grouping data
     */
    public MultiCatErrorBarSeriesSwappable(final AxesImpl axes,
            final int id,
            final Comparable name,
            final SwappableTable swappableTable,
            final String categories,
            final String values,
            final String yLow,
            final String yHigh,
            final String[] byColumns) {
        super(axes, id, name, swappableTable, categories, values, byColumns);
        ArgumentValidations.assertIsNumericOrTimeOrCharOrComparableInstance(swappableTable.getTableDefinition(),
                categories, "Invalid data type in category column: column=" + categories, getPlotInfo());
        ArgumentValidations.assertIsNumericOrTime(swappableTable.getTableDefinition(), values,
                "Invalid data type in category column: column=" + values, getPlotInfo());
        ArgumentValidations.assertIsNumericOrTime(swappableTable.getTableDefinition(), yLow,
                "Invalid data type in category column: column=" + yLow, getPlotInfo());
        ArgumentValidations.assertIsNumericOrTime(swappableTable.getTableDefinition(), yHigh,
                "Invalid data type in category column: column=" + yHigh, getPlotInfo());
        this.categories = categories;
        this.values = values;
        this.yLow = yLow;
        this.yHigh = yHigh;
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    private MultiCatErrorBarSeriesSwappable(final MultiCatErrorBarSeriesSwappable series, final AxesImpl axes) {
        super(series, axes);

        this.categories = series.categories;
        this.values = series.values;
        this.yLow = series.yLow;
        this.yHigh = series.yHigh;
    }

    @Override
    public CategoryErrorBarDataSeriesInternal createSeries(String seriesName, final BaseTable t,
            final DynamicSeriesNamer seriesNamer) {
        seriesName = makeSeriesName(seriesName, seriesNamer);
        final NonserializableCategoryDataSeriesTableMap series =
                new NonserializableCategoryDataSeriesTableMap(axes(), seriesName, t, categories, values, yLow, yHigh);
        series.addSwappableTable(getSwappableTable());
        return series;
    }

    private static class NonserializableCategoryDataSeriesTableMap extends AbstractTableBasedCategoryDataSeries
            implements CategoryErrorBarDataSeriesInternal, CategoryTableDataSeriesInternal {
        private final Table table;

        private final String categoryCol;
        private final String values;
        private final String yLow;
        private final String yHigh;

        private final CategoryErrorBarDataSeriesKernel kernel;

        /**
         * Creates a new CategoryDataSeriesTableMap instance.
         *
         * @param axes {@link AxesImpl} on which this dataset is being plotted
         * @param name series name
         * @param table holds the underlying table
         * @param categoryCol column in the underlying table containing the categorical data
         * @param values column in the underlying table containing the numerical data
         * @param <T> type of the categorical data
         * @throws RequirementFailure {@code chart}, {@code table}, {@code categories}, and {@code values} must not be
         *         null
         * @throws RuntimeException {@code categories} column must be either time, char/{@link Character},
         *         {@link Comparable}, or numeric {@code values} column must be numeric
         */
        <T extends Comparable> NonserializableCategoryDataSeriesTableMap(final AxesImpl axes,
                final Comparable name,
                final BaseTable table,
                final String categoryCol,
                final String values,
                final String yLow,
                final String yHigh) {
            super(axes, -1, name);
            ArgumentValidations.assertNotNull(axes, "axes", getPlotInfo());
            ArgumentValidations.assertNotNull(table, "table", getPlotInfo());
            ArgumentValidations.assertIsNumericOrTimeOrCharOrComparableInstance(table, categoryCol,
                    "Invalid data type in category column: column=" + categoryCol, getPlotInfo());
            ArgumentValidations.assertIsNumericOrTime(table, values,
                    "Invalid data type in data column: column=" + values, getPlotInfo());
            ArgumentValidations.assertIsNumericOrTime(table, yLow, "Invalid data type in data column: column=" + yLow,
                    getPlotInfo());
            ArgumentValidations.assertIsNumericOrTime(table, yHigh, "Invalid data type in data column: column=" + yHigh,
                    getPlotInfo());

            this.table = table;
            this.categoryCol = categoryCol;
            this.values = values;
            this.yLow = yLow;
            this.yHigh = yHigh;
            this.kernel = new CategoryErrorBarDataSeriesKernel(categoryCol, values, yLow, yHigh, getPlotInfo());
        }

        @Override
        public NonserializableCategoryDataSeriesTableMap copy(AxesImpl axes) {
            throw new UnsupportedOperationException(
                    "Copy constructors are not supported on NonserializableCategoryDataSeriesTableMap");
        }

        @Override
        public int size() {
            return kernel.size();
        }

        @Override
        public Collection<Comparable> categories() {
            return kernel.categories();
        }

        @Override
        public Number getValue(final Comparable category) {
            return kernel.getValue(category);
        }

        @Override
        public long getCategoryLocation(final Comparable category) {
            return kernel.getCategoryKey(category);
        }

        @Override
        public Number getStartY(final Comparable category) {
            return kernel.getStartY(category);
        }

        @Override
        public Number getEndY(final Comparable category) {
            return kernel.getEndY(category);
        }

        @Override
        protected Table getTable() {
            return table;
        }

        @Override
        protected String getCategoryCol() {
            return categoryCol;
        }

        @Override
        protected String getValueCol() {
            return values;
        }

        private void writeObject(java.io.ObjectOutputStream stream) {
            throw new UnsupportedOperationException("This can not be serialized!");
        }
    }

    ////////////////////////////// CODE BELOW HERE IS GENERATED -- DO NOT EDIT BY HAND //////////////////////////////
    ////////////////////////////// TO REGENERATE RUN GenerateMultiSeries //////////////////////////////
    ////////////////////////////// AND THEN RUN GenerateFigureImmutable //////////////////////////////

    @Override public void initializeSeries(CategoryErrorBarDataSeriesInternal series) {
        $$initializeSeries$$(series);
    }

    @Override public <COLOR extends io.deephaven.gui.color.Paint> MultiCatErrorBarSeriesSwappable pointColor(final groovy.lang.Closure<COLOR> colors, final Object... keys) {
        return pointColor(new io.deephaven.plot.util.functions.ClosureFunction<>(colors), keys);
    }



    @Override public <COLOR extends io.deephaven.gui.color.Paint> MultiCatErrorBarSeriesSwappable pointColor(final java.util.function.Function<java.lang.Comparable, COLOR> colors, final Object... keys) {
        final String newColumn = io.deephaven.plot.datasets.ColumnNameConstants.POINT_COLOR + this.hashCode();
        applyFunction(colors, newColumn, getX(), io.deephaven.gui.color.Paint.class);
        chart().figure().registerFigureFunction(new io.deephaven.plot.util.functions.FigureImplFunction(f -> f.pointColor(new SelectableDataSetSwappableTable(getSwappableTable()), getX(), newColumn, keys), this));
        return this;
    }



    @Override public <T extends io.deephaven.gui.color.Paint> MultiCatErrorBarSeriesSwappable pointColorByY(final groovy.lang.Closure<T> colors, final Object... keys) {
        return pointColorByY(new io.deephaven.plot.util.functions.ClosureFunction<>(colors), keys);
    }



    @Override public <T extends io.deephaven.gui.color.Paint> MultiCatErrorBarSeriesSwappable pointColorByY(final java.util.function.Function<java.lang.Double, T> colors, final Object... keys) {
        final String newColumn = io.deephaven.plot.datasets.ColumnNameConstants.POINT_COLOR + this.hashCode();
        applyFunction(colors, newColumn, getY(), io.deephaven.gui.color.Paint.class);
        chart().figure().registerFigureFunction(new io.deephaven.plot.util.functions.FigureImplFunction(f -> f.pointColor(new SelectableDataSetSwappableTable(getSwappableTable()), getX(), newColumn, keys), this));
        return this;
    }



    @Override public <COLOR extends java.lang.Integer> MultiCatErrorBarSeriesSwappable pointColorInteger(final groovy.lang.Closure<COLOR> colors, final Object... keys) {
        return pointColorInteger(new io.deephaven.plot.util.functions.ClosureFunction<>(colors), keys);
    }



    @Override public <COLOR extends java.lang.Integer> MultiCatErrorBarSeriesSwappable pointColorInteger(final java.util.function.Function<java.lang.Comparable, COLOR> colors, final Object... keys) {
        final String newColumn = io.deephaven.plot.datasets.ColumnNameConstants.POINT_COLOR + this.hashCode();
        applyFunction(colors, newColumn, getX(), java.lang.Integer.class);
        chart().figure().registerFigureFunction(new io.deephaven.plot.util.functions.FigureImplFunction(f -> f.pointColor(new SelectableDataSetSwappableTable(getSwappableTable()), getX(), newColumn, keys), this));
        return this;
    }



    @Override public <LABEL> MultiCatErrorBarSeriesSwappable pointLabel(final groovy.lang.Closure<LABEL> labels, final Object... keys) {
        return pointLabel(new io.deephaven.plot.util.functions.ClosureFunction<>(labels), keys);
    }



    @Override public <LABEL> MultiCatErrorBarSeriesSwappable pointLabel(final java.util.function.Function<java.lang.Comparable, LABEL> labels, final Object... keys) {
        final String newColumn = io.deephaven.plot.datasets.ColumnNameConstants.POINT_LABEL + this.hashCode();
        applyFunction(labels, newColumn, getX(), java.lang.Object.class);
        chart().figure().registerFigureFunction(new io.deephaven.plot.util.functions.FigureImplFunction(f -> f.pointLabel(new SelectableDataSetSwappableTable(getSwappableTable()), getX(), newColumn, keys), this));
        return this;
    }



    @Override public MultiCatErrorBarSeriesSwappable pointShape(final groovy.lang.Closure<java.lang.String> shapes, final Object... keys) {
        return pointShape(new io.deephaven.plot.util.functions.ClosureFunction<>(shapes), keys);
    }



    @Override public MultiCatErrorBarSeriesSwappable pointShape(final java.util.function.Function<java.lang.Comparable, java.lang.String> shapes, final Object... keys) {
        final String newColumn = io.deephaven.plot.datasets.ColumnNameConstants.POINT_SHAPE + this.hashCode();
        applyFunction(shapes, newColumn, getX(), java.lang.String.class);
        chart().figure().registerFigureFunction(new io.deephaven.plot.util.functions.FigureImplFunction(f -> f.pointShape(new SelectableDataSetSwappableTable(getSwappableTable()), getX(), newColumn, keys), this));
        return this;
    }



    @Override public <NUMBER extends java.lang.Number> MultiCatErrorBarSeriesSwappable pointSize(final groovy.lang.Closure<NUMBER> factors, final Object... keys) {
        return pointSize(new io.deephaven.plot.util.functions.ClosureFunction<>(factors), keys);
    }



    @Override public <NUMBER extends java.lang.Number> MultiCatErrorBarSeriesSwappable pointSize(final java.util.function.Function<java.lang.Comparable, NUMBER> factors, final Object... keys) {
        final String newColumn = io.deephaven.plot.datasets.ColumnNameConstants.POINT_SIZE + this.hashCode();
        applyFunction(factors, newColumn, getX(), java.lang.Number.class);
        chart().figure().registerFigureFunction(new io.deephaven.plot.util.functions.FigureImplFunction(f -> f.pointSize(new SelectableDataSetSwappableTable(getSwappableTable()), getX(), newColumn, keys), this));
        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> errorBarColorSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> errorBarColorSeriesNameToStringMap() {
        return errorBarColorSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable errorBarColor(final java.lang.String color, final Object... keys) {
        if(keys == null || keys.length == 0) {
            errorBarColorSeriesNameToStringMap.setDefault(color);
        } else {
            errorBarColorSeriesNameToStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> errorBarColorSeriesNameTointMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> errorBarColorSeriesNameTointMap() {
        return errorBarColorSeriesNameTointMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable errorBarColor(final int color, final Object... keys) {
        if(keys == null || keys.length == 0) {
            errorBarColorSeriesNameTointMap.setDefault(color);
        } else {
            errorBarColorSeriesNameTointMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> errorBarColorSeriesNameToPaintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> errorBarColorSeriesNameToPaintMap() {
        return errorBarColorSeriesNameToPaintMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable errorBarColor(final io.deephaven.gui.color.Paint color, final Object... keys) {
        if(keys == null || keys.length == 0) {
            errorBarColorSeriesNameToPaintMap.setDefault(color);
        } else {
            errorBarColorSeriesNameToPaintMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> gradientVisibleSeriesNameTobooleanMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> gradientVisibleSeriesNameTobooleanMap() {
        return gradientVisibleSeriesNameTobooleanMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable gradientVisible(final boolean visible, final Object... keys) {
        if(keys == null || keys.length == 0) {
            gradientVisibleSeriesNameTobooleanMap.setDefault(visible);
        } else {
            gradientVisibleSeriesNameTobooleanMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                visible);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> groupSeriesNameTointMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> groupSeriesNameTointMap() {
        return groupSeriesNameTointMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable group(final int group, final Object... keys) {
        if(keys == null || keys.length == 0) {
            groupSeriesNameTointMap.setDefault(group);
        } else {
            groupSeriesNameTointMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                group);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> lineColorSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> lineColorSeriesNameToStringMap() {
        return lineColorSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable lineColor(final java.lang.String color, final Object... keys) {
        if(keys == null || keys.length == 0) {
            lineColorSeriesNameToStringMap.setDefault(color);
        } else {
            lineColorSeriesNameToStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> lineColorSeriesNameTointMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> lineColorSeriesNameTointMap() {
        return lineColorSeriesNameTointMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable lineColor(final int color, final Object... keys) {
        if(keys == null || keys.length == 0) {
            lineColorSeriesNameTointMap.setDefault(color);
        } else {
            lineColorSeriesNameTointMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> lineColorSeriesNameToPaintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> lineColorSeriesNameToPaintMap() {
        return lineColorSeriesNameToPaintMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable lineColor(final io.deephaven.gui.color.Paint color, final Object... keys) {
        if(keys == null || keys.length == 0) {
            lineColorSeriesNameToPaintMap.setDefault(color);
        } else {
            lineColorSeriesNameToPaintMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.plot.LineStyle> lineStyleSeriesNameToLineStyleMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.plot.LineStyle> lineStyleSeriesNameToLineStyleMap() {
        return lineStyleSeriesNameToLineStyleMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable lineStyle(final io.deephaven.plot.LineStyle style, final Object... keys) {
        if(keys == null || keys.length == 0) {
            lineStyleSeriesNameToLineStyleMap.setDefault(style);
        } else {
            lineStyleSeriesNameToLineStyleMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                style);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> linesVisibleSeriesNameToBooleanMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> linesVisibleSeriesNameToBooleanMap() {
        return linesVisibleSeriesNameToBooleanMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable linesVisible(final java.lang.Boolean visible, final Object... keys) {
        if(keys == null || keys.length == 0) {
            linesVisibleSeriesNameToBooleanMap.setDefault(visible);
        } else {
            linesVisibleSeriesNameToBooleanMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                visible);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> piePercentLabelFormatSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> piePercentLabelFormatSeriesNameToStringMap() {
        return piePercentLabelFormatSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable piePercentLabelFormat(final java.lang.String format, final Object... keys) {
        if(keys == null || keys.length == 0) {
            piePercentLabelFormatSeriesNameToStringMap.setDefault(format);
        } else {
            piePercentLabelFormatSeriesNameToStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                format);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointColorSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointColorSeriesNameToStringMap() {
        return pointColorSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointColor(final java.lang.String color, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointColorSeriesNameToStringMap.setDefault(color);
        } else {
            pointColorSeriesNameToStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> pointColorSeriesNameTointMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> pointColorSeriesNameTointMap() {
        return pointColorSeriesNameTointMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointColor(final int color, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointColorSeriesNameTointMap.setDefault(color);
        } else {
            pointColorSeriesNameTointMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> pointColorSeriesNameToPaintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> pointColorSeriesNameToPaintMap() {
        return pointColorSeriesNameToPaintMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointColor(final io.deephaven.gui.color.Paint color, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointColorSeriesNameToPaintMap.setDefault(color);
        } else {
            pointColorSeriesNameToPaintMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointColorSeriesNameToMapMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointColorSeriesNameToMapMap() {
        return pointColorSeriesNameToMapMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable, COLOR extends io.deephaven.gui.color.Paint> MultiCatErrorBarSeriesSwappable pointColor(final java.util.Map<CATEGORY, COLOR> colors, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointColorSeriesNameToMapMap.setDefault(colors);
        } else {
            pointColorSeriesNameToMapMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                colors);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToComparableStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToComparableStringMap() {
        return pointColorSeriesNameToComparableStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointColor(final java.lang.Comparable category, final java.lang.String color, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointColorSeriesNameToComparableStringMap.setDefault(new Object[]{category, color});
        } else {
            pointColorSeriesNameToComparableStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ category, color});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToComparableintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToComparableintMap() {
        return pointColorSeriesNameToComparableintMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointColor(final java.lang.Comparable category, final int color, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointColorSeriesNameToComparableintMap.setDefault(new Object[]{category, color});
        } else {
            pointColorSeriesNameToComparableintMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ category, color});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToComparablePaintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToComparablePaintMap() {
        return pointColorSeriesNameToComparablePaintMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointColor(final java.lang.Comparable category, final io.deephaven.gui.color.Paint color, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointColorSeriesNameToComparablePaintMap.setDefault(new Object[]{category, color});
        } else {
            pointColorSeriesNameToComparablePaintMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ category, color});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToTableStringStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToTableStringStringMap() {
        return pointColorSeriesNameToTableStringStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointColor(final io.deephaven.engine.table.Table t, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys) {
    final io.deephaven.plot.util.tables.TableHandle tHandle = new io.deephaven.plot.util.tables.TableHandle(t, keyColumn, valueColumn);
    addTableHandle(tHandle);
        if(keys == null || keys.length == 0) {
            pointColorSeriesNameToTableStringStringMap.setDefault(new Object[]{tHandle, keyColumn, valueColumn});
        } else {
            pointColorSeriesNameToTableStringStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ tHandle, keyColumn, valueColumn});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToSelectableDataSetStringStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToSelectableDataSetStringStringMap() {
        return pointColorSeriesNameToSelectableDataSetStringStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointColor(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointColorSeriesNameToSelectableDataSetStringStringMap.setDefault(new Object[]{sds, keyColumn, valueColumn});
        } else {
            pointColorSeriesNameToSelectableDataSetStringStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ sds, keyColumn, valueColumn});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointColorByYSeriesNameToMapMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointColorByYSeriesNameToMapMap() {
        return pointColorByYSeriesNameToMapMap;
    }
    @Override public <T extends io.deephaven.gui.color.Paint> MultiCatErrorBarSeriesSwappable pointColorByY(final java.util.Map<java.lang.Double, T> colors, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointColorByYSeriesNameToMapMap.setDefault(colors);
        } else {
            pointColorByYSeriesNameToMapMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                colors);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointColorIntegerSeriesNameToMapMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointColorIntegerSeriesNameToMapMap() {
        return pointColorIntegerSeriesNameToMapMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable, COLOR extends java.lang.Integer> MultiCatErrorBarSeriesSwappable pointColorInteger(final java.util.Map<CATEGORY, COLOR> colors, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointColorIntegerSeriesNameToMapMap.setDefault(colors);
        } else {
            pointColorIntegerSeriesNameToMapMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                colors);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object> pointLabelSeriesNameToObjectMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object> pointLabelSeriesNameToObjectMap() {
        return pointLabelSeriesNameToObjectMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointLabel(final java.lang.Object label, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointLabelSeriesNameToObjectMap.setDefault(label);
        } else {
            pointLabelSeriesNameToObjectMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                label);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointLabelSeriesNameToMapMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointLabelSeriesNameToMapMap() {
        return pointLabelSeriesNameToMapMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable, LABEL> MultiCatErrorBarSeriesSwappable pointLabel(final java.util.Map<CATEGORY, LABEL> labels, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointLabelSeriesNameToMapMap.setDefault(labels);
        } else {
            pointLabelSeriesNameToMapMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                labels);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToComparableObjectMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToComparableObjectMap() {
        return pointLabelSeriesNameToComparableObjectMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointLabel(final java.lang.Comparable category, final java.lang.Object label, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointLabelSeriesNameToComparableObjectMap.setDefault(new Object[]{category, label});
        } else {
            pointLabelSeriesNameToComparableObjectMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ category, label});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToTableStringStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToTableStringStringMap() {
        return pointLabelSeriesNameToTableStringStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointLabel(final io.deephaven.engine.table.Table t, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys) {
    final io.deephaven.plot.util.tables.TableHandle tHandle = new io.deephaven.plot.util.tables.TableHandle(t, keyColumn, valueColumn);
    addTableHandle(tHandle);
        if(keys == null || keys.length == 0) {
            pointLabelSeriesNameToTableStringStringMap.setDefault(new Object[]{tHandle, keyColumn, valueColumn});
        } else {
            pointLabelSeriesNameToTableStringStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ tHandle, keyColumn, valueColumn});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToSelectableDataSetStringStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToSelectableDataSetStringStringMap() {
        return pointLabelSeriesNameToSelectableDataSetStringStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointLabel(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointLabelSeriesNameToSelectableDataSetStringStringMap.setDefault(new Object[]{sds, keyColumn, valueColumn});
        } else {
            pointLabelSeriesNameToSelectableDataSetStringStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ sds, keyColumn, valueColumn});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointLabelFormatSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointLabelFormatSeriesNameToStringMap() {
        return pointLabelFormatSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointLabelFormat(final java.lang.String format, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointLabelFormatSeriesNameToStringMap.setDefault(format);
        } else {
            pointLabelFormatSeriesNameToStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                format);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointShapeSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointShapeSeriesNameToStringMap() {
        return pointShapeSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointShape(final java.lang.String shape, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointShapeSeriesNameToStringMap.setDefault(shape);
        } else {
            pointShapeSeriesNameToStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                shape);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.shape.Shape> pointShapeSeriesNameToShapeMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.shape.Shape> pointShapeSeriesNameToShapeMap() {
        return pointShapeSeriesNameToShapeMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointShape(final io.deephaven.gui.shape.Shape shape, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointShapeSeriesNameToShapeMap.setDefault(shape);
        } else {
            pointShapeSeriesNameToShapeMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                shape);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointShapeSeriesNameToMapMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointShapeSeriesNameToMapMap() {
        return pointShapeSeriesNameToMapMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable> MultiCatErrorBarSeriesSwappable pointShape(final java.util.Map<CATEGORY, java.lang.String> shapes, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointShapeSeriesNameToMapMap.setDefault(shapes);
        } else {
            pointShapeSeriesNameToMapMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                shapes);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToComparableStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToComparableStringMap() {
        return pointShapeSeriesNameToComparableStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointShape(final java.lang.Comparable category, final java.lang.String shape, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointShapeSeriesNameToComparableStringMap.setDefault(new Object[]{category, shape});
        } else {
            pointShapeSeriesNameToComparableStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ category, shape});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToComparableShapeMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToComparableShapeMap() {
        return pointShapeSeriesNameToComparableShapeMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointShape(final java.lang.Comparable category, final io.deephaven.gui.shape.Shape shape, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointShapeSeriesNameToComparableShapeMap.setDefault(new Object[]{category, shape});
        } else {
            pointShapeSeriesNameToComparableShapeMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ category, shape});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToTableStringStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToTableStringStringMap() {
        return pointShapeSeriesNameToTableStringStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointShape(final io.deephaven.engine.table.Table t, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys) {
    final io.deephaven.plot.util.tables.TableHandle tHandle = new io.deephaven.plot.util.tables.TableHandle(t, keyColumn, valueColumn);
    addTableHandle(tHandle);
        if(keys == null || keys.length == 0) {
            pointShapeSeriesNameToTableStringStringMap.setDefault(new Object[]{tHandle, keyColumn, valueColumn});
        } else {
            pointShapeSeriesNameToTableStringStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ tHandle, keyColumn, valueColumn});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToSelectableDataSetStringStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToSelectableDataSetStringStringMap() {
        return pointShapeSeriesNameToSelectableDataSetStringStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointShape(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointShapeSeriesNameToSelectableDataSetStringStringMap.setDefault(new Object[]{sds, keyColumn, valueColumn});
        } else {
            pointShapeSeriesNameToSelectableDataSetStringStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ sds, keyColumn, valueColumn});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Number> pointSizeSeriesNameToNumberMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Number> pointSizeSeriesNameToNumberMap() {
        return pointSizeSeriesNameToNumberMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointSize(final java.lang.Number factor, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointSizeSeriesNameToNumberMap.setDefault(factor);
        } else {
            pointSizeSeriesNameToNumberMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                factor);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointSizeSeriesNameToMapMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointSizeSeriesNameToMapMap() {
        return pointSizeSeriesNameToMapMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable, NUMBER extends java.lang.Number> MultiCatErrorBarSeriesSwappable pointSize(final java.util.Map<CATEGORY, NUMBER> factors, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointSizeSeriesNameToMapMap.setDefault(factors);
        } else {
            pointSizeSeriesNameToMapMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                factors);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToCATEGORYArrayNUMBERArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToCATEGORYArrayNUMBERArrayMap() {
        return pointSizeSeriesNameToCATEGORYArrayNUMBERArrayMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable, NUMBER extends java.lang.Number> MultiCatErrorBarSeriesSwappable pointSize(final CATEGORY[] categories, final NUMBER[] factors, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointSizeSeriesNameToCATEGORYArrayNUMBERArrayMap.setDefault(new Object[]{categories, factors});
        } else {
            pointSizeSeriesNameToCATEGORYArrayNUMBERArrayMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ categories, factors});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToCATEGORYArraydoubleArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToCATEGORYArraydoubleArrayMap() {
        return pointSizeSeriesNameToCATEGORYArraydoubleArrayMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable> MultiCatErrorBarSeriesSwappable pointSize(final CATEGORY[] categories, final double[] factors, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointSizeSeriesNameToCATEGORYArraydoubleArrayMap.setDefault(new Object[]{categories, factors});
        } else {
            pointSizeSeriesNameToCATEGORYArraydoubleArrayMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ categories, factors});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToCATEGORYArrayintArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToCATEGORYArrayintArrayMap() {
        return pointSizeSeriesNameToCATEGORYArrayintArrayMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable> MultiCatErrorBarSeriesSwappable pointSize(final CATEGORY[] categories, final int[] factors, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointSizeSeriesNameToCATEGORYArrayintArrayMap.setDefault(new Object[]{categories, factors});
        } else {
            pointSizeSeriesNameToCATEGORYArrayintArrayMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ categories, factors});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToCATEGORYArraylongArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToCATEGORYArraylongArrayMap() {
        return pointSizeSeriesNameToCATEGORYArraylongArrayMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable> MultiCatErrorBarSeriesSwappable pointSize(final CATEGORY[] categories, final long[] factors, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointSizeSeriesNameToCATEGORYArraylongArrayMap.setDefault(new Object[]{categories, factors});
        } else {
            pointSizeSeriesNameToCATEGORYArraylongArrayMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ categories, factors});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToComparableNumberMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToComparableNumberMap() {
        return pointSizeSeriesNameToComparableNumberMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointSize(final java.lang.Comparable category, final java.lang.Number factor, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointSizeSeriesNameToComparableNumberMap.setDefault(new Object[]{category, factor});
        } else {
            pointSizeSeriesNameToComparableNumberMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ category, factor});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToComparabledoubleMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToComparabledoubleMap() {
        return pointSizeSeriesNameToComparabledoubleMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointSize(final java.lang.Comparable category, final double factor, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointSizeSeriesNameToComparabledoubleMap.setDefault(new Object[]{category, factor});
        } else {
            pointSizeSeriesNameToComparabledoubleMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ category, factor});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToComparableintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToComparableintMap() {
        return pointSizeSeriesNameToComparableintMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointSize(final java.lang.Comparable category, final int factor, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointSizeSeriesNameToComparableintMap.setDefault(new Object[]{category, factor});
        } else {
            pointSizeSeriesNameToComparableintMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ category, factor});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToComparablelongMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToComparablelongMap() {
        return pointSizeSeriesNameToComparablelongMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointSize(final java.lang.Comparable category, final long factor, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointSizeSeriesNameToComparablelongMap.setDefault(new Object[]{category, factor});
        } else {
            pointSizeSeriesNameToComparablelongMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ category, factor});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToTableStringStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToTableStringStringMap() {
        return pointSizeSeriesNameToTableStringStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointSize(final io.deephaven.engine.table.Table t, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys) {
    final io.deephaven.plot.util.tables.TableHandle tHandle = new io.deephaven.plot.util.tables.TableHandle(t, keyColumn, valueColumn);
    addTableHandle(tHandle);
        if(keys == null || keys.length == 0) {
            pointSizeSeriesNameToTableStringStringMap.setDefault(new Object[]{tHandle, keyColumn, valueColumn});
        } else {
            pointSizeSeriesNameToTableStringStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ tHandle, keyColumn, valueColumn});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToSelectableDataSetStringStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToSelectableDataSetStringStringMap() {
        return pointSizeSeriesNameToSelectableDataSetStringStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointSize(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String keyColumn, final java.lang.String valueColumn, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointSizeSeriesNameToSelectableDataSetStringStringMap.setDefault(new Object[]{sds, keyColumn, valueColumn});
        } else {
            pointSizeSeriesNameToSelectableDataSetStringStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                new Object[]{ sds, keyColumn, valueColumn});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> pointsVisibleSeriesNameToBooleanMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> pointsVisibleSeriesNameToBooleanMap() {
        return pointsVisibleSeriesNameToBooleanMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable pointsVisible(final java.lang.Boolean visible, final Object... keys) {
        if(keys == null || keys.length == 0) {
            pointsVisibleSeriesNameToBooleanMap.setDefault(visible);
        } else {
            pointsVisibleSeriesNameToBooleanMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                visible);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> seriesColorSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> seriesColorSeriesNameToStringMap() {
        return seriesColorSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable seriesColor(final java.lang.String color, final Object... keys) {
        if(keys == null || keys.length == 0) {
            seriesColorSeriesNameToStringMap.setDefault(color);
        } else {
            seriesColorSeriesNameToStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> seriesColorSeriesNameTointMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> seriesColorSeriesNameTointMap() {
        return seriesColorSeriesNameTointMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable seriesColor(final int color, final Object... keys) {
        if(keys == null || keys.length == 0) {
            seriesColorSeriesNameTointMap.setDefault(color);
        } else {
            seriesColorSeriesNameTointMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> seriesColorSeriesNameToPaintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> seriesColorSeriesNameToPaintMap() {
        return seriesColorSeriesNameToPaintMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable seriesColor(final io.deephaven.gui.color.Paint color, final Object... keys) {
        if(keys == null || keys.length == 0) {
            seriesColorSeriesNameToPaintMap.setDefault(color);
        } else {
            seriesColorSeriesNameToPaintMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> toolTipPatternSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> toolTipPatternSeriesNameToStringMap() {
        return toolTipPatternSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable toolTipPattern(final java.lang.String format, final Object... keys) {
        if(keys == null || keys.length == 0) {
            toolTipPatternSeriesNameToStringMap.setDefault(format);
        } else {
            toolTipPatternSeriesNameToStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                format);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> xToolTipPatternSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> xToolTipPatternSeriesNameToStringMap() {
        return xToolTipPatternSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable xToolTipPattern(final java.lang.String format, final Object... keys) {
        if(keys == null || keys.length == 0) {
            xToolTipPatternSeriesNameToStringMap.setDefault(format);
        } else {
            xToolTipPatternSeriesNameToStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                format);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> yToolTipPatternSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> yToolTipPatternSeriesNameToStringMap() {
        return yToolTipPatternSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeriesSwappable yToolTipPattern(final java.lang.String format, final Object... keys) {
        if(keys == null || keys.length == 0) {
            yToolTipPatternSeriesNameToStringMap.setDefault(format);
        } else {
            yToolTipPatternSeriesNameToStringMap.put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), 
                format);
        }

        return this;
    }



    @SuppressWarnings("unchecked") 
    private <CATEGORY extends java.lang.Comparable, COLOR extends io.deephaven.gui.color.Paint, T extends io.deephaven.gui.color.Paint, LABEL, NUMBER extends java.lang.Number, COLOR0 extends java.lang.Integer> void $$initializeSeries$$(CategoryErrorBarDataSeriesInternal series) {
        String name = series.name().toString();
        java.util.function.Consumer<java.util.Map> consumer0 = series::pointColor;
        pointColorSeriesNameToMapMap.runIfKeyExistsCast(consumer0, name);
        java.util.function.Consumer<java.util.Map> consumer1 = series::pointColorByY;
        pointColorByYSeriesNameToMapMap.runIfKeyExistsCast(consumer1, name);
        java.lang.Object[]         objectArray = pointSizeSeriesNameToComparableNumberMap.get(name);
        if(objectArray != null) {series.pointSize((java.lang.Comparable) objectArray[0], (java.lang.Number) objectArray[1]);}

        objectArray = pointColorSeriesNameToTableStringStringMap.get(name);
        if(objectArray != null) {series.pointColor(((io.deephaven.plot.util.tables.TableHandle) objectArray[0]).getTable(), (java.lang.String) objectArray[1], (java.lang.String) objectArray[2]);}

        java.util.function.Consumer<java.lang.Boolean> consumer2 = series::pointsVisible;
        pointsVisibleSeriesNameToBooleanMap.runIfKeyExistsCast(consumer2, name);
        java.util.function.Consumer<io.deephaven.gui.shape.Shape> consumer3 = series::pointShape;
        pointShapeSeriesNameToShapeMap.runIfKeyExistsCast(consumer3, name);
        java.util.function.Consumer<java.lang.Integer> consumer4 = series::lineColor;
        lineColorSeriesNameTointMap.runIfKeyExistsCast(consumer4, name);
        objectArray = pointShapeSeriesNameToComparableShapeMap.get(name);
        if(objectArray != null) {series.pointShape((java.lang.Comparable) objectArray[0], (io.deephaven.gui.shape.Shape) objectArray[1]);}

        java.util.function.Consumer<io.deephaven.gui.color.Paint> consumer5 = series::seriesColor;
        seriesColorSeriesNameToPaintMap.runIfKeyExistsCast(consumer5, name);
        objectArray = pointColorSeriesNameToComparableStringMap.get(name);
        if(objectArray != null) {series.pointColor((java.lang.Comparable) objectArray[0], (java.lang.String) objectArray[1]);}

        objectArray = pointColorSeriesNameToComparablePaintMap.get(name);
        if(objectArray != null) {series.pointColor((java.lang.Comparable) objectArray[0], (io.deephaven.gui.color.Paint) objectArray[1]);}

        java.util.function.Consumer<java.lang.String> consumer6 = series::yToolTipPattern;
        yToolTipPatternSeriesNameToStringMap.runIfKeyExistsCast(consumer6, name);
        java.util.function.Consumer<java.lang.Integer> consumer7 = series::seriesColor;
        seriesColorSeriesNameTointMap.runIfKeyExistsCast(consumer7, name);
        objectArray = pointSizeSeriesNameToComparabledoubleMap.get(name);
        if(objectArray != null) {series.pointSize((java.lang.Comparable) objectArray[0], (double) objectArray[1]);}

        java.util.function.Consumer<java.lang.Object> consumer8 = series::pointLabel;
        pointLabelSeriesNameToObjectMap.runIfKeyExistsCast(consumer8, name);
        java.util.function.Consumer<io.deephaven.gui.color.Paint> consumer9 = series::errorBarColor;
        errorBarColorSeriesNameToPaintMap.runIfKeyExistsCast(consumer9, name);
        objectArray = pointSizeSeriesNameToComparablelongMap.get(name);
        if(objectArray != null) {series.pointSize((java.lang.Comparable) objectArray[0], (long) objectArray[1]);}

        java.util.function.Consumer<java.lang.String> consumer10 = series::seriesColor;
        seriesColorSeriesNameToStringMap.runIfKeyExistsCast(consumer10, name);
        objectArray = pointShapeSeriesNameToComparableStringMap.get(name);
        if(objectArray != null) {series.pointShape((java.lang.Comparable) objectArray[0], (java.lang.String) objectArray[1]);}

        objectArray = pointLabelSeriesNameToTableStringStringMap.get(name);
        if(objectArray != null) {series.pointLabel(((io.deephaven.plot.util.tables.TableHandle) objectArray[0]).getTable(), (java.lang.String) objectArray[1], (java.lang.String) objectArray[2]);}

        java.util.function.Consumer<java.util.Map> consumer11 = series::pointShape;
        pointShapeSeriesNameToMapMap.runIfKeyExistsCast(consumer11, name);
        java.util.function.Consumer<java.lang.Boolean> consumer12 = series::gradientVisible;
        gradientVisibleSeriesNameTobooleanMap.runIfKeyExistsCast(consumer12, name);
        java.util.function.Consumer<io.deephaven.gui.color.Paint> consumer13 = series::pointColor;
        pointColorSeriesNameToPaintMap.runIfKeyExistsCast(consumer13, name);
        objectArray = pointShapeSeriesNameToTableStringStringMap.get(name);
        if(objectArray != null) {series.pointShape(((io.deephaven.plot.util.tables.TableHandle) objectArray[0]).getTable(), (java.lang.String) objectArray[1], (java.lang.String) objectArray[2]);}

        java.util.function.Consumer<java.lang.String> consumer14 = series::piePercentLabelFormat;
        piePercentLabelFormatSeriesNameToStringMap.runIfKeyExistsCast(consumer14, name);
        objectArray = pointLabelSeriesNameToSelectableDataSetStringStringMap.get(name);
        if(objectArray != null) {series.pointLabel((io.deephaven.plot.filters.SelectableDataSet) objectArray[0], (java.lang.String) objectArray[1], (java.lang.String) objectArray[2]);}

        java.util.function.Consumer<java.lang.String> consumer15 = series::errorBarColor;
        errorBarColorSeriesNameToStringMap.runIfKeyExistsCast(consumer15, name);
        objectArray = pointColorSeriesNameToSelectableDataSetStringStringMap.get(name);
        if(objectArray != null) {series.pointColor((io.deephaven.plot.filters.SelectableDataSet) objectArray[0], (java.lang.String) objectArray[1], (java.lang.String) objectArray[2]);}

        java.util.function.Consumer<java.lang.String> consumer16 = series::xToolTipPattern;
        xToolTipPatternSeriesNameToStringMap.runIfKeyExistsCast(consumer16, name);
        java.util.function.Consumer<java.lang.Integer> consumer17 = series::errorBarColor;
        errorBarColorSeriesNameTointMap.runIfKeyExistsCast(consumer17, name);
        objectArray = pointLabelSeriesNameToComparableObjectMap.get(name);
        if(objectArray != null) {series.pointLabel((java.lang.Comparable) objectArray[0], objectArray[1]);}

        java.util.function.Consumer<java.lang.String> consumer18 = series::toolTipPattern;
        toolTipPatternSeriesNameToStringMap.runIfKeyExistsCast(consumer18, name);
        java.util.function.Consumer<java.lang.String> consumer19 = series::pointLabelFormat;
        pointLabelFormatSeriesNameToStringMap.runIfKeyExistsCast(consumer19, name);
        objectArray = pointShapeSeriesNameToSelectableDataSetStringStringMap.get(name);
        if(objectArray != null) {series.pointShape((io.deephaven.plot.filters.SelectableDataSet) objectArray[0], (java.lang.String) objectArray[1], (java.lang.String) objectArray[2]);}

        java.util.function.Consumer<java.lang.String> consumer20 = series::pointColor;
        pointColorSeriesNameToStringMap.runIfKeyExistsCast(consumer20, name);
        java.util.function.Consumer<java.lang.Boolean> consumer21 = series::linesVisible;
        linesVisibleSeriesNameToBooleanMap.runIfKeyExistsCast(consumer21, name);
        java.util.function.Consumer<java.lang.String> consumer22 = series::lineColor;
        lineColorSeriesNameToStringMap.runIfKeyExistsCast(consumer22, name);
        objectArray = pointSizeSeriesNameToCATEGORYArraydoubleArrayMap.get(name);
        if(objectArray != null) {series.pointSize((CATEGORY[]) objectArray[0], (double[]) objectArray[1]);}

        objectArray = pointSizeSeriesNameToTableStringStringMap.get(name);
        if(objectArray != null) {series.pointSize(((io.deephaven.plot.util.tables.TableHandle) objectArray[0]).getTable(), (java.lang.String) objectArray[1], (java.lang.String) objectArray[2]);}

        java.util.function.Consumer<java.util.Map> consumer23 = series::pointLabel;
        pointLabelSeriesNameToMapMap.runIfKeyExistsCast(consumer23, name);
        java.util.function.Consumer<io.deephaven.gui.color.Paint> consumer24 = series::lineColor;
        lineColorSeriesNameToPaintMap.runIfKeyExistsCast(consumer24, name);
        objectArray = pointSizeSeriesNameToComparableintMap.get(name);
        if(objectArray != null) {series.pointSize((java.lang.Comparable) objectArray[0], (int) objectArray[1]);}

        objectArray = pointSizeSeriesNameToCATEGORYArraylongArrayMap.get(name);
        if(objectArray != null) {series.pointSize((CATEGORY[]) objectArray[0], (long[]) objectArray[1]);}

        java.util.function.Consumer<java.lang.Integer> consumer25 = series::group;
        groupSeriesNameTointMap.runIfKeyExistsCast(consumer25, name);
        java.util.function.Consumer<io.deephaven.plot.LineStyle> consumer26 = series::lineStyle;
        lineStyleSeriesNameToLineStyleMap.runIfKeyExistsCast(consumer26, name);
        java.util.function.Consumer<java.util.Map> consumer27 = series::pointSize;
        pointSizeSeriesNameToMapMap.runIfKeyExistsCast(consumer27, name);
        objectArray = pointSizeSeriesNameToCATEGORYArrayintArrayMap.get(name);
        if(objectArray != null) {series.pointSize((CATEGORY[]) objectArray[0], (int[]) objectArray[1]);}

        java.util.function.Consumer<java.util.Map<CATEGORY, COLOR0>> consumer28 = series::pointColorInteger;
        pointColorIntegerSeriesNameToMapMap.runIfKeyExistsCast(consumer28, name);
        java.util.function.Consumer<java.lang.Number> consumer29 = series::pointSize;
        pointSizeSeriesNameToNumberMap.runIfKeyExistsCast(consumer29, name);
        objectArray = pointSizeSeriesNameToCATEGORYArrayNUMBERArrayMap.get(name);
        if(objectArray != null) {series.pointSize((CATEGORY[]) objectArray[0], (NUMBER[]) objectArray[1]);}

        java.util.function.Consumer<java.lang.Integer> consumer30 = series::pointColor;
        pointColorSeriesNameTointMap.runIfKeyExistsCast(consumer30, name);
        objectArray = pointColorSeriesNameToComparableintMap.get(name);
        if(objectArray != null) {series.pointColor((java.lang.Comparable) objectArray[0], (int) objectArray[1]);}

        java.util.function.Consumer<java.lang.String> consumer31 = series::pointShape;
        pointShapeSeriesNameToStringMap.runIfKeyExistsCast(consumer31, name);
        objectArray = pointSizeSeriesNameToSelectableDataSetStringStringMap.get(name);
        if(objectArray != null) {series.pointSize((io.deephaven.plot.filters.SelectableDataSet) objectArray[0], (java.lang.String) objectArray[1], (java.lang.String) objectArray[2]);}


    }
    @Override
    public MultiCatErrorBarSeriesSwappable copy(AxesImpl axes) {
        final MultiCatErrorBarSeriesSwappable __s__ = new MultiCatErrorBarSeriesSwappable(this, axes);
                __s__.pointColorSeriesNameToMapMap = pointColorSeriesNameToMapMap.copy();
        __s__.pointColorByYSeriesNameToMapMap = pointColorByYSeriesNameToMapMap.copy();
        __s__.pointSizeSeriesNameToComparableNumberMap = pointSizeSeriesNameToComparableNumberMap.copy();
        __s__.pointColorSeriesNameToTableStringStringMap = pointColorSeriesNameToTableStringStringMap.copy();
        __s__.pointsVisibleSeriesNameToBooleanMap = pointsVisibleSeriesNameToBooleanMap.copy();
        __s__.pointShapeSeriesNameToShapeMap = pointShapeSeriesNameToShapeMap.copy();
        __s__.lineColorSeriesNameTointMap = lineColorSeriesNameTointMap.copy();
        __s__.pointShapeSeriesNameToComparableShapeMap = pointShapeSeriesNameToComparableShapeMap.copy();
        __s__.seriesColorSeriesNameToPaintMap = seriesColorSeriesNameToPaintMap.copy();
        __s__.pointColorSeriesNameToComparableStringMap = pointColorSeriesNameToComparableStringMap.copy();
        __s__.pointColorSeriesNameToComparablePaintMap = pointColorSeriesNameToComparablePaintMap.copy();
        __s__.yToolTipPatternSeriesNameToStringMap = yToolTipPatternSeriesNameToStringMap.copy();
        __s__.seriesColorSeriesNameTointMap = seriesColorSeriesNameTointMap.copy();
        __s__.pointSizeSeriesNameToComparabledoubleMap = pointSizeSeriesNameToComparabledoubleMap.copy();
        __s__.pointLabelSeriesNameToObjectMap = pointLabelSeriesNameToObjectMap.copy();
        __s__.errorBarColorSeriesNameToPaintMap = errorBarColorSeriesNameToPaintMap.copy();
        __s__.pointSizeSeriesNameToComparablelongMap = pointSizeSeriesNameToComparablelongMap.copy();
        __s__.seriesColorSeriesNameToStringMap = seriesColorSeriesNameToStringMap.copy();
        __s__.pointShapeSeriesNameToComparableStringMap = pointShapeSeriesNameToComparableStringMap.copy();
        __s__.pointLabelSeriesNameToTableStringStringMap = pointLabelSeriesNameToTableStringStringMap.copy();
        __s__.pointShapeSeriesNameToMapMap = pointShapeSeriesNameToMapMap.copy();
        __s__.gradientVisibleSeriesNameTobooleanMap = gradientVisibleSeriesNameTobooleanMap.copy();
        __s__.pointColorSeriesNameToPaintMap = pointColorSeriesNameToPaintMap.copy();
        __s__.pointShapeSeriesNameToTableStringStringMap = pointShapeSeriesNameToTableStringStringMap.copy();
        __s__.piePercentLabelFormatSeriesNameToStringMap = piePercentLabelFormatSeriesNameToStringMap.copy();
        __s__.pointLabelSeriesNameToSelectableDataSetStringStringMap = pointLabelSeriesNameToSelectableDataSetStringStringMap.copy();
        __s__.errorBarColorSeriesNameToStringMap = errorBarColorSeriesNameToStringMap.copy();
        __s__.pointColorSeriesNameToSelectableDataSetStringStringMap = pointColorSeriesNameToSelectableDataSetStringStringMap.copy();
        __s__.xToolTipPatternSeriesNameToStringMap = xToolTipPatternSeriesNameToStringMap.copy();
        __s__.errorBarColorSeriesNameTointMap = errorBarColorSeriesNameTointMap.copy();
        __s__.pointLabelSeriesNameToComparableObjectMap = pointLabelSeriesNameToComparableObjectMap.copy();
        __s__.toolTipPatternSeriesNameToStringMap = toolTipPatternSeriesNameToStringMap.copy();
        __s__.pointLabelFormatSeriesNameToStringMap = pointLabelFormatSeriesNameToStringMap.copy();
        __s__.pointShapeSeriesNameToSelectableDataSetStringStringMap = pointShapeSeriesNameToSelectableDataSetStringStringMap.copy();
        __s__.pointColorSeriesNameToStringMap = pointColorSeriesNameToStringMap.copy();
        __s__.linesVisibleSeriesNameToBooleanMap = linesVisibleSeriesNameToBooleanMap.copy();
        __s__.lineColorSeriesNameToStringMap = lineColorSeriesNameToStringMap.copy();
        __s__.pointSizeSeriesNameToCATEGORYArraydoubleArrayMap = pointSizeSeriesNameToCATEGORYArraydoubleArrayMap.copy();
        __s__.pointSizeSeriesNameToTableStringStringMap = pointSizeSeriesNameToTableStringStringMap.copy();
        __s__.pointLabelSeriesNameToMapMap = pointLabelSeriesNameToMapMap.copy();
        __s__.lineColorSeriesNameToPaintMap = lineColorSeriesNameToPaintMap.copy();
        __s__.pointSizeSeriesNameToComparableintMap = pointSizeSeriesNameToComparableintMap.copy();
        __s__.pointSizeSeriesNameToCATEGORYArraylongArrayMap = pointSizeSeriesNameToCATEGORYArraylongArrayMap.copy();
        __s__.groupSeriesNameTointMap = groupSeriesNameTointMap.copy();
        __s__.lineStyleSeriesNameToLineStyleMap = lineStyleSeriesNameToLineStyleMap.copy();
        __s__.pointSizeSeriesNameToMapMap = pointSizeSeriesNameToMapMap.copy();
        __s__.pointSizeSeriesNameToCATEGORYArrayintArrayMap = pointSizeSeriesNameToCATEGORYArrayintArrayMap.copy();
        __s__.pointColorIntegerSeriesNameToMapMap = pointColorIntegerSeriesNameToMapMap.copy();
        __s__.pointSizeSeriesNameToNumberMap = pointSizeSeriesNameToNumberMap.copy();
        __s__.pointSizeSeriesNameToCATEGORYArrayNUMBERArrayMap = pointSizeSeriesNameToCATEGORYArrayNUMBERArrayMap.copy();
        __s__.pointColorSeriesNameTointMap = pointColorSeriesNameTointMap.copy();
        __s__.pointColorSeriesNameToComparableintMap = pointColorSeriesNameToComparableintMap.copy();
        __s__.pointShapeSeriesNameToStringMap = pointShapeSeriesNameToStringMap.copy();
        __s__.pointSizeSeriesNameToSelectableDataSetStringStringMap = pointSizeSeriesNameToSelectableDataSetStringStringMap.copy();
        return __s__;
    }
}