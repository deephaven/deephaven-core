/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.datasets.multiseries;

import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.datasets.DynamicSeriesNamer;
import io.deephaven.plot.datasets.category.AbstractTableBasedCategoryDataSeries;
import io.deephaven.plot.datasets.category.CategoryTableDataSeriesInternal;
import io.deephaven.plot.datasets.categoryerrorbar.CategoryErrorBarDataSeriesKernel;
import io.deephaven.plot.datasets.categoryerrorbar.CategoryErrorBarDataSeriesInternal;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.plot.util.tables.TableBackedPartitionedTableHandle;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;

import java.util.Collection;

/**
 * A {@link AbstractMultiSeries} collection that holds and generates {@link CategoryErrorBarDataSeriesInternal}.
 */
public class MultiCatErrorBarSeries extends AbstractPartitionedTableHandleMultiSeries<CategoryErrorBarDataSeriesInternal> {

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
     * @param partitionedTableHandle table data
     * @param categories column in {@code t} holding discrete data
     * @param values column in {@code t} holding numeric data
     * @param byColumns column(s) in {@code t} that holds the grouping data
     */
    public MultiCatErrorBarSeries(final AxesImpl axes,
            final int id,
            final Comparable name,
            final TableBackedPartitionedTableHandle partitionedTableHandle,
            final String categories,
            final String values,
            final String yLow,
            final String yHigh,
            final String[] byColumns) {
        super(axes, id, name, partitionedTableHandle, categories, values, byColumns);
        ArgumentValidations.assertIsNumericOrTimeOrCharOrComparableInstance(partitionedTableHandle.getTableDefinition(),
                categories, "Invalid data type in category column: column=" + categories, getPlotInfo());
        ArgumentValidations.assertIsNumericOrTime(partitionedTableHandle.getTableDefinition(), values,
                "Invalid data type in category column: column=" + values, getPlotInfo());
        ArgumentValidations.assertIsNumericOrTime(partitionedTableHandle.getTableDefinition(), yLow,
                "Invalid data type in category column: column=" + yLow, getPlotInfo());
        ArgumentValidations.assertIsNumericOrTime(partitionedTableHandle.getTableDefinition(), yHigh,
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
    private MultiCatErrorBarSeries(final MultiCatErrorBarSeries series, final AxesImpl axes) {
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
        final NonserializableCategoryDataSeriesPartitionedTable series = new NonserializableCategoryDataSeriesPartitionedTable(axes(),
                seriesName,
                t,
                categories,
                values,
                yLow,
                yHigh);
        return series;
    }

    private static class NonserializableCategoryDataSeriesPartitionedTable extends AbstractTableBasedCategoryDataSeries
            implements CategoryErrorBarDataSeriesInternal, CategoryTableDataSeriesInternal {
        private final BaseTable table;

        private final String categoryCol;
        private final String values;
        private final String yLow;
        private final String yHigh;

        private final CategoryErrorBarDataSeriesKernel kernel;

        /**
         * Creates a new CategoryDataSeriesPartitionedTable instance.
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
        <T extends Comparable> NonserializableCategoryDataSeriesPartitionedTable(final AxesImpl axes,
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
        public NonserializableCategoryDataSeriesPartitionedTable copy(AxesImpl axes) {
            throw new UnsupportedOperationException(
                    "Copy constructors are not supported on NonserializableCategoryDataSeriesPartitionedTable");
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
        public long getCategoryLocation(Comparable category) {
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

    @Override public <COLOR extends io.deephaven.gui.color.Paint> MultiCatErrorBarSeries pointColor(final groovy.lang.Closure<COLOR> pointColor, final Object... multiSeriesKey) {
        return pointColor(new io.deephaven.plot.util.functions.ClosureFunction<>(pointColor), multiSeriesKey);
    }



    @Override public <COLOR extends io.deephaven.gui.color.Paint> MultiCatErrorBarSeries pointColor(final java.util.function.Function<java.lang.Comparable, COLOR> pointColor, final Object... multiSeriesKey) {
        final String newColumn = io.deephaven.plot.datasets.ColumnNameConstants.POINT_COLOR + this.hashCode();
        applyFunction(pointColor, newColumn, getX(), io.deephaven.gui.color.Paint.class);
        chart().figure().registerFigureFunction(new io.deephaven.plot.util.functions.FigureImplFunction(f -> f.pointColor(getPartitionedTableHandle().getTable(), getX(), newColumn, multiSeriesKey), this));
        return this;
    }



    @Override public <COLOR extends java.lang.Integer> MultiCatErrorBarSeries pointColorInteger(final groovy.lang.Closure<COLOR> colors, final Object... multiSeriesKey) {
        return pointColorInteger(new io.deephaven.plot.util.functions.ClosureFunction<>(colors), multiSeriesKey);
    }



    @Override public <COLOR extends java.lang.Integer> MultiCatErrorBarSeries pointColorInteger(final java.util.function.Function<java.lang.Comparable, COLOR> colors, final Object... multiSeriesKey) {
        final String newColumn = io.deephaven.plot.datasets.ColumnNameConstants.POINT_COLOR + this.hashCode();
        applyFunction(colors, newColumn, getX(), java.lang.Integer.class);
        chart().figure().registerFigureFunction(new io.deephaven.plot.util.functions.FigureImplFunction(f -> f.pointColor(getPartitionedTableHandle().getTable(), getX(), newColumn, multiSeriesKey), this));
        return this;
    }



    @Override public <LABEL> MultiCatErrorBarSeries pointLabel(final groovy.lang.Closure<LABEL> pointLabels, final Object... multiSeriesKey) {
        return pointLabel(new io.deephaven.plot.util.functions.ClosureFunction<>(pointLabels), multiSeriesKey);
    }



    @Override public <LABEL> MultiCatErrorBarSeries pointLabel(final java.util.function.Function<java.lang.Comparable, LABEL> pointLabels, final Object... multiSeriesKey) {
        final String newColumn = io.deephaven.plot.datasets.ColumnNameConstants.POINT_LABEL + this.hashCode();
        applyFunction(pointLabels, newColumn, getX(), java.lang.Object.class);
        chart().figure().registerFigureFunction(new io.deephaven.plot.util.functions.FigureImplFunction(f -> f.pointLabel(getPartitionedTableHandle().getTable(), getX(), newColumn, multiSeriesKey), this));
        return this;
    }



    @Override public MultiCatErrorBarSeries pointShape(final groovy.lang.Closure<java.lang.String> pointShapes, final Object... multiSeriesKey) {
        return pointShape(new io.deephaven.plot.util.functions.ClosureFunction<>(pointShapes), multiSeriesKey);
    }



    @Override public MultiCatErrorBarSeries pointShape(final java.util.function.Function<java.lang.Comparable, java.lang.String> pointShapes, final Object... multiSeriesKey) {
        final String newColumn = io.deephaven.plot.datasets.ColumnNameConstants.POINT_SHAPE + this.hashCode();
        applyFunction(pointShapes, newColumn, getX(), java.lang.String.class);
        chart().figure().registerFigureFunction(new io.deephaven.plot.util.functions.FigureImplFunction(f -> f.pointShape(getPartitionedTableHandle().getTable(), getX(), newColumn, multiSeriesKey), this));
        return this;
    }



    @Override public <NUMBER extends java.lang.Number> MultiCatErrorBarSeries pointSize(final groovy.lang.Closure<NUMBER> pointSizes, final Object... multiSeriesKey) {
        return pointSize(new io.deephaven.plot.util.functions.ClosureFunction<>(pointSizes), multiSeriesKey);
    }



    @Override public <NUMBER extends java.lang.Number> MultiCatErrorBarSeries pointSize(final java.util.function.Function<java.lang.Comparable, NUMBER> pointSizes, final Object... multiSeriesKey) {
        final String newColumn = io.deephaven.plot.datasets.ColumnNameConstants.POINT_SIZE + this.hashCode();
        applyFunction(pointSizes, newColumn, getX(), java.lang.Number.class);
        chart().figure().registerFigureFunction(new io.deephaven.plot.util.functions.FigureImplFunction(f -> f.pointSize(getPartitionedTableHandle().getTable(), getX(), newColumn, multiSeriesKey), this));
        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> errorBarColorSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> errorBarColorSeriesNameToStringMap() {
        return errorBarColorSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeries errorBarColor(final java.lang.String errorBarColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            errorBarColorSeriesNameToStringMap.setDefault(errorBarColor);
        } else {
            errorBarColorSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey), errorBarColor);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> errorBarColorSeriesNameTointMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> errorBarColorSeriesNameTointMap() {
        return errorBarColorSeriesNameTointMap;
    }
    @Override public MultiCatErrorBarSeries errorBarColor(final int errorBarColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            errorBarColorSeriesNameTointMap.setDefault(errorBarColor);
        } else {
            errorBarColorSeriesNameTointMap.put(namingFunction.apply(multiSeriesKey), errorBarColor);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> errorBarColorSeriesNameToPaintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> errorBarColorSeriesNameToPaintMap() {
        return errorBarColorSeriesNameToPaintMap;
    }
    @Override public MultiCatErrorBarSeries errorBarColor(final io.deephaven.gui.color.Paint errorBarColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            errorBarColorSeriesNameToPaintMap.setDefault(errorBarColor);
        } else {
            errorBarColorSeriesNameToPaintMap.put(namingFunction.apply(multiSeriesKey), errorBarColor);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> gradientVisibleSeriesNameTobooleanMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> gradientVisibleSeriesNameTobooleanMap() {
        return gradientVisibleSeriesNameTobooleanMap;
    }
    @Override public MultiCatErrorBarSeries gradientVisible(final boolean gradientVisible, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            gradientVisibleSeriesNameTobooleanMap.setDefault(gradientVisible);
        } else {
            gradientVisibleSeriesNameTobooleanMap.put(namingFunction.apply(multiSeriesKey), gradientVisible);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> groupSeriesNameTointMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> groupSeriesNameTointMap() {
        return groupSeriesNameTointMap;
    }
    @Override public MultiCatErrorBarSeries group(final int group, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            groupSeriesNameTointMap.setDefault(group);
        } else {
            groupSeriesNameTointMap.put(namingFunction.apply(multiSeriesKey), group);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> lineColorSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> lineColorSeriesNameToStringMap() {
        return lineColorSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeries lineColor(final java.lang.String color, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            lineColorSeriesNameToStringMap.setDefault(color);
        } else {
            lineColorSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey), color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> lineColorSeriesNameTointMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> lineColorSeriesNameTointMap() {
        return lineColorSeriesNameTointMap;
    }
    @Override public MultiCatErrorBarSeries lineColor(final int color, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            lineColorSeriesNameTointMap.setDefault(color);
        } else {
            lineColorSeriesNameTointMap.put(namingFunction.apply(multiSeriesKey), color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> lineColorSeriesNameToPaintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> lineColorSeriesNameToPaintMap() {
        return lineColorSeriesNameToPaintMap;
    }
    @Override public MultiCatErrorBarSeries lineColor(final io.deephaven.gui.color.Paint color, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            lineColorSeriesNameToPaintMap.setDefault(color);
        } else {
            lineColorSeriesNameToPaintMap.put(namingFunction.apply(multiSeriesKey), color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.plot.LineStyle> lineStyleSeriesNameToLineStyleMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.plot.LineStyle> lineStyleSeriesNameToLineStyleMap() {
        return lineStyleSeriesNameToLineStyleMap;
    }
    @Override public MultiCatErrorBarSeries lineStyle(final io.deephaven.plot.LineStyle lineStyle, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            lineStyleSeriesNameToLineStyleMap.setDefault(lineStyle);
        } else {
            lineStyleSeriesNameToLineStyleMap.put(namingFunction.apply(multiSeriesKey), lineStyle);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> linesVisibleSeriesNameToBooleanMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> linesVisibleSeriesNameToBooleanMap() {
        return linesVisibleSeriesNameToBooleanMap;
    }
    @Override public MultiCatErrorBarSeries linesVisible(final java.lang.Boolean visible, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            linesVisibleSeriesNameToBooleanMap.setDefault(visible);
        } else {
            linesVisibleSeriesNameToBooleanMap.put(namingFunction.apply(multiSeriesKey), visible);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> piePercentLabelFormatSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> piePercentLabelFormatSeriesNameToStringMap() {
        return piePercentLabelFormatSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeries piePercentLabelFormat(final java.lang.String pieLabelFormat, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            piePercentLabelFormatSeriesNameToStringMap.setDefault(pieLabelFormat);
        } else {
            piePercentLabelFormatSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey), pieLabelFormat);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointColorSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointColorSeriesNameToStringMap() {
        return pointColorSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeries pointColor(final java.lang.String pointColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameToStringMap.setDefault(pointColor);
        } else {
            pointColorSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey), pointColor);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> pointColorSeriesNameTointMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> pointColorSeriesNameTointMap() {
        return pointColorSeriesNameTointMap;
    }
    @Override public MultiCatErrorBarSeries pointColor(final int pointColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameTointMap.setDefault(pointColor);
        } else {
            pointColorSeriesNameTointMap.put(namingFunction.apply(multiSeriesKey), pointColor);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> pointColorSeriesNameToPaintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> pointColorSeriesNameToPaintMap() {
        return pointColorSeriesNameToPaintMap;
    }
    @Override public MultiCatErrorBarSeries pointColor(final io.deephaven.gui.color.Paint pointColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameToPaintMap.setDefault(pointColor);
        } else {
            pointColorSeriesNameToPaintMap.put(namingFunction.apply(multiSeriesKey), pointColor);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointColorSeriesNameToMapMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointColorSeriesNameToMapMap() {
        return pointColorSeriesNameToMapMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable, COLOR extends io.deephaven.gui.color.Paint> MultiCatErrorBarSeries pointColor(final java.util.Map<CATEGORY, COLOR> pointColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameToMapMap.setDefault(pointColor);
        } else {
            pointColorSeriesNameToMapMap.put(namingFunction.apply(multiSeriesKey), pointColor);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToComparableStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToComparableStringMap() {
        return pointColorSeriesNameToComparableStringMap;
    }
    @Override public MultiCatErrorBarSeries pointColor(final java.lang.Comparable category, final java.lang.String pointColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameToComparableStringMap.setDefault(new Object[]{category, pointColor});
        } else {
            pointColorSeriesNameToComparableStringMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ category, pointColor});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToComparableintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToComparableintMap() {
        return pointColorSeriesNameToComparableintMap;
    }
    @Override public MultiCatErrorBarSeries pointColor(final java.lang.Comparable category, final int pointColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameToComparableintMap.setDefault(new Object[]{category, pointColor});
        } else {
            pointColorSeriesNameToComparableintMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ category, pointColor});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToComparablePaintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToComparablePaintMap() {
        return pointColorSeriesNameToComparablePaintMap;
    }
    @Override public MultiCatErrorBarSeries pointColor(final java.lang.Comparable category, final io.deephaven.gui.color.Paint pointColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameToComparablePaintMap.setDefault(new Object[]{category, pointColor});
        } else {
            pointColorSeriesNameToComparablePaintMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ category, pointColor});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToTableStringStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToTableStringStringMap() {
        return pointColorSeriesNameToTableStringStringMap;
    }
    @Override public MultiCatErrorBarSeries pointColor(final io.deephaven.engine.table.Table t, final java.lang.String category, final java.lang.String pointColor, final Object... multiSeriesKey) {
    final io.deephaven.plot.util.tables.TableHandle tHandle = new io.deephaven.plot.util.tables.TableHandle(t, category, pointColor);
    addTableHandle(tHandle);
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameToTableStringStringMap.setDefault(new Object[]{tHandle, category, pointColor});
        } else {
            pointColorSeriesNameToTableStringStringMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ tHandle, category, pointColor});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToSelectableDataSetStringStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointColorSeriesNameToSelectableDataSetStringStringMap() {
        return pointColorSeriesNameToSelectableDataSetStringStringMap;
    }
    @Override public MultiCatErrorBarSeries pointColor(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String category, final java.lang.String pointColor, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorSeriesNameToSelectableDataSetStringStringMap.setDefault(new Object[]{sds, category, pointColor});
        } else {
            pointColorSeriesNameToSelectableDataSetStringStringMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ sds, category, pointColor});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointColorIntegerSeriesNameToMapMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointColorIntegerSeriesNameToMapMap() {
        return pointColorIntegerSeriesNameToMapMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable, COLOR extends java.lang.Integer> MultiCatErrorBarSeries pointColorInteger(final java.util.Map<CATEGORY, COLOR> colors, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointColorIntegerSeriesNameToMapMap.setDefault(colors);
        } else {
            pointColorIntegerSeriesNameToMapMap.put(namingFunction.apply(multiSeriesKey), colors);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object> pointLabelSeriesNameToObjectMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object> pointLabelSeriesNameToObjectMap() {
        return pointLabelSeriesNameToObjectMap;
    }
    @Override public MultiCatErrorBarSeries pointLabel(final java.lang.Object pointLabel, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointLabelSeriesNameToObjectMap.setDefault(pointLabel);
        } else {
            pointLabelSeriesNameToObjectMap.put(namingFunction.apply(multiSeriesKey), pointLabel);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointLabelSeriesNameToMapMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointLabelSeriesNameToMapMap() {
        return pointLabelSeriesNameToMapMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable, LABEL> MultiCatErrorBarSeries pointLabel(final java.util.Map<CATEGORY, LABEL> pointLabels, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointLabelSeriesNameToMapMap.setDefault(pointLabels);
        } else {
            pointLabelSeriesNameToMapMap.put(namingFunction.apply(multiSeriesKey), pointLabels);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToComparableObjectMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToComparableObjectMap() {
        return pointLabelSeriesNameToComparableObjectMap;
    }
    @Override public MultiCatErrorBarSeries pointLabel(final java.lang.Comparable category, final java.lang.Object pointLabel, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointLabelSeriesNameToComparableObjectMap.setDefault(new Object[]{category, pointLabel});
        } else {
            pointLabelSeriesNameToComparableObjectMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ category, pointLabel});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToTableStringStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToTableStringStringMap() {
        return pointLabelSeriesNameToTableStringStringMap;
    }
    @Override public MultiCatErrorBarSeries pointLabel(final io.deephaven.engine.table.Table t, final java.lang.String category, final java.lang.String pointLabel, final Object... multiSeriesKey) {
    final io.deephaven.plot.util.tables.TableHandle tHandle = new io.deephaven.plot.util.tables.TableHandle(t, category, pointLabel);
    addTableHandle(tHandle);
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointLabelSeriesNameToTableStringStringMap.setDefault(new Object[]{tHandle, category, pointLabel});
        } else {
            pointLabelSeriesNameToTableStringStringMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ tHandle, category, pointLabel});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToSelectableDataSetStringStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointLabelSeriesNameToSelectableDataSetStringStringMap() {
        return pointLabelSeriesNameToSelectableDataSetStringStringMap;
    }
    @Override public MultiCatErrorBarSeries pointLabel(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String category, final java.lang.String pointLabel, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointLabelSeriesNameToSelectableDataSetStringStringMap.setDefault(new Object[]{sds, category, pointLabel});
        } else {
            pointLabelSeriesNameToSelectableDataSetStringStringMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ sds, category, pointLabel});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointLabelFormatSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointLabelFormatSeriesNameToStringMap() {
        return pointLabelFormatSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeries pointLabelFormat(final java.lang.String pointLabelFormat, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointLabelFormatSeriesNameToStringMap.setDefault(pointLabelFormat);
        } else {
            pointLabelFormatSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey), pointLabelFormat);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointShapeSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> pointShapeSeriesNameToStringMap() {
        return pointShapeSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeries pointShape(final java.lang.String pointShape, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointShapeSeriesNameToStringMap.setDefault(pointShape);
        } else {
            pointShapeSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey), pointShape);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.shape.Shape> pointShapeSeriesNameToShapeMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.shape.Shape> pointShapeSeriesNameToShapeMap() {
        return pointShapeSeriesNameToShapeMap;
    }
    @Override public MultiCatErrorBarSeries pointShape(final io.deephaven.gui.shape.Shape pointShape, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointShapeSeriesNameToShapeMap.setDefault(pointShape);
        } else {
            pointShapeSeriesNameToShapeMap.put(namingFunction.apply(multiSeriesKey), pointShape);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointShapeSeriesNameToMapMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointShapeSeriesNameToMapMap() {
        return pointShapeSeriesNameToMapMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable> MultiCatErrorBarSeries pointShape(final java.util.Map<CATEGORY, java.lang.String> pointShapes, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointShapeSeriesNameToMapMap.setDefault(pointShapes);
        } else {
            pointShapeSeriesNameToMapMap.put(namingFunction.apply(multiSeriesKey), pointShapes);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToComparableStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToComparableStringMap() {
        return pointShapeSeriesNameToComparableStringMap;
    }
    @Override public MultiCatErrorBarSeries pointShape(final java.lang.Comparable category, final java.lang.String pointShape, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointShapeSeriesNameToComparableStringMap.setDefault(new Object[]{category, pointShape});
        } else {
            pointShapeSeriesNameToComparableStringMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ category, pointShape});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToComparableShapeMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToComparableShapeMap() {
        return pointShapeSeriesNameToComparableShapeMap;
    }
    @Override public MultiCatErrorBarSeries pointShape(final java.lang.Comparable category, final io.deephaven.gui.shape.Shape pointShape, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointShapeSeriesNameToComparableShapeMap.setDefault(new Object[]{category, pointShape});
        } else {
            pointShapeSeriesNameToComparableShapeMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ category, pointShape});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToTableStringStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToTableStringStringMap() {
        return pointShapeSeriesNameToTableStringStringMap;
    }
    @Override public MultiCatErrorBarSeries pointShape(final io.deephaven.engine.table.Table t, final java.lang.String category, final java.lang.String pointShape, final Object... multiSeriesKey) {
    final io.deephaven.plot.util.tables.TableHandle tHandle = new io.deephaven.plot.util.tables.TableHandle(t, category, pointShape);
    addTableHandle(tHandle);
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointShapeSeriesNameToTableStringStringMap.setDefault(new Object[]{tHandle, category, pointShape});
        } else {
            pointShapeSeriesNameToTableStringStringMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ tHandle, category, pointShape});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToSelectableDataSetStringStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointShapeSeriesNameToSelectableDataSetStringStringMap() {
        return pointShapeSeriesNameToSelectableDataSetStringStringMap;
    }
    @Override public MultiCatErrorBarSeries pointShape(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String category, final java.lang.String pointShape, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointShapeSeriesNameToSelectableDataSetStringStringMap.setDefault(new Object[]{sds, category, pointShape});
        } else {
            pointShapeSeriesNameToSelectableDataSetStringStringMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ sds, category, pointShape});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Number> pointSizeSeriesNameToNumberMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Number> pointSizeSeriesNameToNumberMap() {
        return pointSizeSeriesNameToNumberMap;
    }
    @Override public MultiCatErrorBarSeries pointSize(final java.lang.Number pointSize, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameToNumberMap.setDefault(pointSize);
        } else {
            pointSizeSeriesNameToNumberMap.put(namingFunction.apply(multiSeriesKey), pointSize);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointSizeSeriesNameToMapMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.util.Map> pointSizeSeriesNameToMapMap() {
        return pointSizeSeriesNameToMapMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable, NUMBER extends java.lang.Number> MultiCatErrorBarSeries pointSize(final java.util.Map<CATEGORY, NUMBER> pointSizes, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameToMapMap.setDefault(pointSizes);
        } else {
            pointSizeSeriesNameToMapMap.put(namingFunction.apply(multiSeriesKey), pointSizes);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToCATEGORYArrayNUMBERArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToCATEGORYArrayNUMBERArrayMap() {
        return pointSizeSeriesNameToCATEGORYArrayNUMBERArrayMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable, NUMBER extends java.lang.Number> MultiCatErrorBarSeries pointSize(final CATEGORY[] categories, final NUMBER[] pointSizes, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameToCATEGORYArrayNUMBERArrayMap.setDefault(new Object[]{categories, pointSizes});
        } else {
            pointSizeSeriesNameToCATEGORYArrayNUMBERArrayMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ categories, pointSizes});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToCATEGORYArraydoubleArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToCATEGORYArraydoubleArrayMap() {
        return pointSizeSeriesNameToCATEGORYArraydoubleArrayMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable> MultiCatErrorBarSeries pointSize(final CATEGORY[] categories, final double[] pointSizes, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameToCATEGORYArraydoubleArrayMap.setDefault(new Object[]{categories, pointSizes});
        } else {
            pointSizeSeriesNameToCATEGORYArraydoubleArrayMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ categories, pointSizes});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToCATEGORYArrayintArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToCATEGORYArrayintArrayMap() {
        return pointSizeSeriesNameToCATEGORYArrayintArrayMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable> MultiCatErrorBarSeries pointSize(final CATEGORY[] categories, final int[] pointSizes, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameToCATEGORYArrayintArrayMap.setDefault(new Object[]{categories, pointSizes});
        } else {
            pointSizeSeriesNameToCATEGORYArrayintArrayMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ categories, pointSizes});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToCATEGORYArraylongArrayMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToCATEGORYArraylongArrayMap() {
        return pointSizeSeriesNameToCATEGORYArraylongArrayMap;
    }
    @Override public <CATEGORY extends java.lang.Comparable> MultiCatErrorBarSeries pointSize(final CATEGORY[] categories, final long[] pointSizes, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameToCATEGORYArraylongArrayMap.setDefault(new Object[]{categories, pointSizes});
        } else {
            pointSizeSeriesNameToCATEGORYArraylongArrayMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ categories, pointSizes});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToComparableNumberMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToComparableNumberMap() {
        return pointSizeSeriesNameToComparableNumberMap;
    }
    @Override public MultiCatErrorBarSeries pointSize(final java.lang.Comparable category, final java.lang.Number pointSize, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameToComparableNumberMap.setDefault(new Object[]{category, pointSize});
        } else {
            pointSizeSeriesNameToComparableNumberMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ category, pointSize});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToComparabledoubleMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToComparabledoubleMap() {
        return pointSizeSeriesNameToComparabledoubleMap;
    }
    @Override public MultiCatErrorBarSeries pointSize(final java.lang.Comparable category, final double pointSize, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameToComparabledoubleMap.setDefault(new Object[]{category, pointSize});
        } else {
            pointSizeSeriesNameToComparabledoubleMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ category, pointSize});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToComparableintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToComparableintMap() {
        return pointSizeSeriesNameToComparableintMap;
    }
    @Override public MultiCatErrorBarSeries pointSize(final java.lang.Comparable category, final int pointSize, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameToComparableintMap.setDefault(new Object[]{category, pointSize});
        } else {
            pointSizeSeriesNameToComparableintMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ category, pointSize});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToComparablelongMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToComparablelongMap() {
        return pointSizeSeriesNameToComparablelongMap;
    }
    @Override public MultiCatErrorBarSeries pointSize(final java.lang.Comparable category, final long pointSize, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameToComparablelongMap.setDefault(new Object[]{category, pointSize});
        } else {
            pointSizeSeriesNameToComparablelongMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ category, pointSize});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToTableStringStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToTableStringStringMap() {
        return pointSizeSeriesNameToTableStringStringMap;
    }
    @Override public MultiCatErrorBarSeries pointSize(final io.deephaven.engine.table.Table t, final java.lang.String category, final java.lang.String pointSize, final Object... multiSeriesKey) {
    final io.deephaven.plot.util.tables.TableHandle tHandle = new io.deephaven.plot.util.tables.TableHandle(t, category, pointSize);
    addTableHandle(tHandle);
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameToTableStringStringMap.setDefault(new Object[]{tHandle, category, pointSize});
        } else {
            pointSizeSeriesNameToTableStringStringMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ tHandle, category, pointSize});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToSelectableDataSetStringStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Object[]> pointSizeSeriesNameToSelectableDataSetStringStringMap() {
        return pointSizeSeriesNameToSelectableDataSetStringStringMap;
    }
    @Override public MultiCatErrorBarSeries pointSize(final io.deephaven.plot.filters.SelectableDataSet sds, final java.lang.String category, final java.lang.String pointSize, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointSizeSeriesNameToSelectableDataSetStringStringMap.setDefault(new Object[]{sds, category, pointSize});
        } else {
            pointSizeSeriesNameToSelectableDataSetStringStringMap.put(namingFunction.apply(multiSeriesKey), 
                new Object[]{ sds, category, pointSize});
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> pointsVisibleSeriesNameToBooleanMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Boolean> pointsVisibleSeriesNameToBooleanMap() {
        return pointsVisibleSeriesNameToBooleanMap;
    }
    @Override public MultiCatErrorBarSeries pointsVisible(final java.lang.Boolean visible, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            pointsVisibleSeriesNameToBooleanMap.setDefault(visible);
        } else {
            pointsVisibleSeriesNameToBooleanMap.put(namingFunction.apply(multiSeriesKey), visible);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> seriesColorSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> seriesColorSeriesNameToStringMap() {
        return seriesColorSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeries seriesColor(final java.lang.String color, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            seriesColorSeriesNameToStringMap.setDefault(color);
        } else {
            seriesColorSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey), color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> seriesColorSeriesNameTointMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.Integer> seriesColorSeriesNameTointMap() {
        return seriesColorSeriesNameTointMap;
    }
    @Override public MultiCatErrorBarSeries seriesColor(final int color, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            seriesColorSeriesNameTointMap.setDefault(color);
        } else {
            seriesColorSeriesNameTointMap.put(namingFunction.apply(multiSeriesKey), color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> seriesColorSeriesNameToPaintMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> seriesColorSeriesNameToPaintMap() {
        return seriesColorSeriesNameToPaintMap;
    }
    @Override public MultiCatErrorBarSeries seriesColor(final io.deephaven.gui.color.Paint color, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            seriesColorSeriesNameToPaintMap.setDefault(color);
        } else {
            seriesColorSeriesNameToPaintMap.put(namingFunction.apply(multiSeriesKey), color);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> toolTipPatternSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> toolTipPatternSeriesNameToStringMap() {
        return toolTipPatternSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeries toolTipPattern(final java.lang.String toolTipPattern, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            toolTipPatternSeriesNameToStringMap.setDefault(toolTipPattern);
        } else {
            toolTipPatternSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey), toolTipPattern);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> xToolTipPatternSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> xToolTipPatternSeriesNameToStringMap() {
        return xToolTipPatternSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeries xToolTipPattern(final java.lang.String xToolTipPattern, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            xToolTipPatternSeriesNameToStringMap.setDefault(xToolTipPattern);
        } else {
            xToolTipPatternSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey), xToolTipPattern);
        }

        return this;
    }



    private io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> yToolTipPatternSeriesNameToStringMap = new io.deephaven.plot.util.PlotUtils.HashMapWithDefault<>();
    public io.deephaven.plot.util.PlotUtils.HashMapWithDefault<String, java.lang.String> yToolTipPatternSeriesNameToStringMap() {
        return yToolTipPatternSeriesNameToStringMap;
    }
    @Override public MultiCatErrorBarSeries yToolTipPattern(final java.lang.String yToolTipPattern, final Object... multiSeriesKey) {
        if(multiSeriesKey == null || multiSeriesKey.length == 0) {
            yToolTipPatternSeriesNameToStringMap.setDefault(yToolTipPattern);
        } else {
            yToolTipPatternSeriesNameToStringMap.put(namingFunction.apply(multiSeriesKey), yToolTipPattern);
        }

        return this;
    }



    @SuppressWarnings("unchecked") 
    private <CATEGORY extends java.lang.Comparable, COLOR extends io.deephaven.gui.color.Paint, LABEL, NUMBER extends java.lang.Number, COLOR0 extends java.lang.Integer> void $$initializeSeries$$(CategoryErrorBarDataSeriesInternal series) {
        String name = series.name().toString();
        java.util.function.Consumer<java.util.Map> consumer0 = series::pointColor;
        pointColorSeriesNameToMapMap.runIfKeyExistsCast(consumer0, name);
        java.lang.Object[]         objectArray = pointSizeSeriesNameToComparableNumberMap.get(name);
        if(objectArray != null) {series.pointSize((java.lang.Comparable) objectArray[0], (java.lang.Number) objectArray[1]);}

        objectArray = pointColorSeriesNameToTableStringStringMap.get(name);
        if(objectArray != null) {series.pointColor(((io.deephaven.plot.util.tables.TableHandle) objectArray[0]).getTable(), (java.lang.String) objectArray[1], (java.lang.String) objectArray[2]);}

        java.util.function.Consumer<java.lang.Boolean> consumer1 = series::pointsVisible;
        pointsVisibleSeriesNameToBooleanMap.runIfKeyExistsCast(consumer1, name);
        java.util.function.Consumer<io.deephaven.gui.shape.Shape> consumer2 = series::pointShape;
        pointShapeSeriesNameToShapeMap.runIfKeyExistsCast(consumer2, name);
        java.util.function.Consumer<java.lang.Integer> consumer3 = series::lineColor;
        lineColorSeriesNameTointMap.runIfKeyExistsCast(consumer3, name);
        objectArray = pointShapeSeriesNameToComparableShapeMap.get(name);
        if(objectArray != null) {series.pointShape((java.lang.Comparable) objectArray[0], (io.deephaven.gui.shape.Shape) objectArray[1]);}

        java.util.function.Consumer<io.deephaven.gui.color.Paint> consumer4 = series::seriesColor;
        seriesColorSeriesNameToPaintMap.runIfKeyExistsCast(consumer4, name);
        objectArray = pointColorSeriesNameToComparableStringMap.get(name);
        if(objectArray != null) {series.pointColor((java.lang.Comparable) objectArray[0], (java.lang.String) objectArray[1]);}

        objectArray = pointColorSeriesNameToComparablePaintMap.get(name);
        if(objectArray != null) {series.pointColor((java.lang.Comparable) objectArray[0], (io.deephaven.gui.color.Paint) objectArray[1]);}

        java.util.function.Consumer<java.lang.String> consumer5 = series::yToolTipPattern;
        yToolTipPatternSeriesNameToStringMap.runIfKeyExistsCast(consumer5, name);
        java.util.function.Consumer<java.lang.Integer> consumer6 = series::seriesColor;
        seriesColorSeriesNameTointMap.runIfKeyExistsCast(consumer6, name);
        objectArray = pointSizeSeriesNameToComparabledoubleMap.get(name);
        if(objectArray != null) {series.pointSize((java.lang.Comparable) objectArray[0], (double) objectArray[1]);}

        java.util.function.Consumer<java.lang.Object> consumer7 = series::pointLabel;
        pointLabelSeriesNameToObjectMap.runIfKeyExistsCast(consumer7, name);
        java.util.function.Consumer<io.deephaven.gui.color.Paint> consumer8 = series::errorBarColor;
        errorBarColorSeriesNameToPaintMap.runIfKeyExistsCast(consumer8, name);
        objectArray = pointSizeSeriesNameToComparablelongMap.get(name);
        if(objectArray != null) {series.pointSize((java.lang.Comparable) objectArray[0], (long) objectArray[1]);}

        java.util.function.Consumer<java.lang.String> consumer9 = series::seriesColor;
        seriesColorSeriesNameToStringMap.runIfKeyExistsCast(consumer9, name);
        objectArray = pointShapeSeriesNameToComparableStringMap.get(name);
        if(objectArray != null) {series.pointShape((java.lang.Comparable) objectArray[0], (java.lang.String) objectArray[1]);}

        objectArray = pointLabelSeriesNameToTableStringStringMap.get(name);
        if(objectArray != null) {series.pointLabel(((io.deephaven.plot.util.tables.TableHandle) objectArray[0]).getTable(), (java.lang.String) objectArray[1], (java.lang.String) objectArray[2]);}

        java.util.function.Consumer<java.util.Map> consumer10 = series::pointShape;
        pointShapeSeriesNameToMapMap.runIfKeyExistsCast(consumer10, name);
        java.util.function.Consumer<java.lang.Boolean> consumer11 = series::gradientVisible;
        gradientVisibleSeriesNameTobooleanMap.runIfKeyExistsCast(consumer11, name);
        java.util.function.Consumer<io.deephaven.gui.color.Paint> consumer12 = series::pointColor;
        pointColorSeriesNameToPaintMap.runIfKeyExistsCast(consumer12, name);
        objectArray = pointShapeSeriesNameToTableStringStringMap.get(name);
        if(objectArray != null) {series.pointShape(((io.deephaven.plot.util.tables.TableHandle) objectArray[0]).getTable(), (java.lang.String) objectArray[1], (java.lang.String) objectArray[2]);}

        java.util.function.Consumer<java.lang.String> consumer13 = series::piePercentLabelFormat;
        piePercentLabelFormatSeriesNameToStringMap.runIfKeyExistsCast(consumer13, name);
        objectArray = pointLabelSeriesNameToSelectableDataSetStringStringMap.get(name);
        if(objectArray != null) {series.pointLabel((io.deephaven.plot.filters.SelectableDataSet) objectArray[0], (java.lang.String) objectArray[1], (java.lang.String) objectArray[2]);}

        java.util.function.Consumer<java.lang.String> consumer14 = series::errorBarColor;
        errorBarColorSeriesNameToStringMap.runIfKeyExistsCast(consumer14, name);
        objectArray = pointColorSeriesNameToSelectableDataSetStringStringMap.get(name);
        if(objectArray != null) {series.pointColor((io.deephaven.plot.filters.SelectableDataSet) objectArray[0], (java.lang.String) objectArray[1], (java.lang.String) objectArray[2]);}

        java.util.function.Consumer<java.lang.String> consumer15 = series::xToolTipPattern;
        xToolTipPatternSeriesNameToStringMap.runIfKeyExistsCast(consumer15, name);
        java.util.function.Consumer<java.lang.Integer> consumer16 = series::errorBarColor;
        errorBarColorSeriesNameTointMap.runIfKeyExistsCast(consumer16, name);
        objectArray = pointLabelSeriesNameToComparableObjectMap.get(name);
        if(objectArray != null) {series.pointLabel((java.lang.Comparable) objectArray[0], objectArray[1]);}

        java.util.function.Consumer<java.lang.String> consumer17 = series::toolTipPattern;
        toolTipPatternSeriesNameToStringMap.runIfKeyExistsCast(consumer17, name);
        java.util.function.Consumer<java.lang.String> consumer18 = series::pointLabelFormat;
        pointLabelFormatSeriesNameToStringMap.runIfKeyExistsCast(consumer18, name);
        objectArray = pointShapeSeriesNameToSelectableDataSetStringStringMap.get(name);
        if(objectArray != null) {series.pointShape((io.deephaven.plot.filters.SelectableDataSet) objectArray[0], (java.lang.String) objectArray[1], (java.lang.String) objectArray[2]);}

        java.util.function.Consumer<java.lang.String> consumer19 = series::pointColor;
        pointColorSeriesNameToStringMap.runIfKeyExistsCast(consumer19, name);
        java.util.function.Consumer<java.lang.Boolean> consumer20 = series::linesVisible;
        linesVisibleSeriesNameToBooleanMap.runIfKeyExistsCast(consumer20, name);
        java.util.function.Consumer<java.lang.String> consumer21 = series::lineColor;
        lineColorSeriesNameToStringMap.runIfKeyExistsCast(consumer21, name);
        objectArray = pointSizeSeriesNameToCATEGORYArraydoubleArrayMap.get(name);
        if(objectArray != null) {series.pointSize((CATEGORY[]) objectArray[0], (double[]) objectArray[1]);}

        objectArray = pointSizeSeriesNameToTableStringStringMap.get(name);
        if(objectArray != null) {series.pointSize(((io.deephaven.plot.util.tables.TableHandle) objectArray[0]).getTable(), (java.lang.String) objectArray[1], (java.lang.String) objectArray[2]);}

        java.util.function.Consumer<java.util.Map> consumer22 = series::pointLabel;
        pointLabelSeriesNameToMapMap.runIfKeyExistsCast(consumer22, name);
        java.util.function.Consumer<io.deephaven.gui.color.Paint> consumer23 = series::lineColor;
        lineColorSeriesNameToPaintMap.runIfKeyExistsCast(consumer23, name);
        objectArray = pointSizeSeriesNameToComparableintMap.get(name);
        if(objectArray != null) {series.pointSize((java.lang.Comparable) objectArray[0], (int) objectArray[1]);}

        objectArray = pointSizeSeriesNameToCATEGORYArraylongArrayMap.get(name);
        if(objectArray != null) {series.pointSize((CATEGORY[]) objectArray[0], (long[]) objectArray[1]);}

        java.util.function.Consumer<java.lang.Integer> consumer24 = series::group;
        groupSeriesNameTointMap.runIfKeyExistsCast(consumer24, name);
        java.util.function.Consumer<io.deephaven.plot.LineStyle> consumer25 = series::lineStyle;
        lineStyleSeriesNameToLineStyleMap.runIfKeyExistsCast(consumer25, name);
        java.util.function.Consumer<java.util.Map> consumer26 = series::pointSize;
        pointSizeSeriesNameToMapMap.runIfKeyExistsCast(consumer26, name);
        objectArray = pointSizeSeriesNameToCATEGORYArrayintArrayMap.get(name);
        if(objectArray != null) {series.pointSize((CATEGORY[]) objectArray[0], (int[]) objectArray[1]);}

        java.util.function.Consumer<java.util.Map<CATEGORY, COLOR0>> consumer27 = series::pointColorInteger;
        pointColorIntegerSeriesNameToMapMap.runIfKeyExistsCast(consumer27, name);
        java.util.function.Consumer<java.lang.Number> consumer28 = series::pointSize;
        pointSizeSeriesNameToNumberMap.runIfKeyExistsCast(consumer28, name);
        objectArray = pointSizeSeriesNameToCATEGORYArrayNUMBERArrayMap.get(name);
        if(objectArray != null) {series.pointSize((CATEGORY[]) objectArray[0], (NUMBER[]) objectArray[1]);}

        java.util.function.Consumer<java.lang.Integer> consumer29 = series::pointColor;
        pointColorSeriesNameTointMap.runIfKeyExistsCast(consumer29, name);
        objectArray = pointColorSeriesNameToComparableintMap.get(name);
        if(objectArray != null) {series.pointColor((java.lang.Comparable) objectArray[0], (int) objectArray[1]);}

        java.util.function.Consumer<java.lang.String> consumer30 = series::pointShape;
        pointShapeSeriesNameToStringMap.runIfKeyExistsCast(consumer30, name);
        objectArray = pointSizeSeriesNameToSelectableDataSetStringStringMap.get(name);
        if(objectArray != null) {series.pointSize((io.deephaven.plot.filters.SelectableDataSet) objectArray[0], (java.lang.String) objectArray[1], (java.lang.String) objectArray[2]);}


    }
    @Override
    public MultiCatErrorBarSeries copy(AxesImpl axes) {
        final MultiCatErrorBarSeries __s__ = new MultiCatErrorBarSeries(this, axes);
                __s__.pointColorSeriesNameToMapMap = pointColorSeriesNameToMapMap.copy();
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