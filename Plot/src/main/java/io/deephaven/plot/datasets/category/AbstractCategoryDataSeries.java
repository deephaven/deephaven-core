/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.datasets.category;

import io.deephaven.base.verify.Require;
import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.ChartImpl;
import io.deephaven.plot.LineStyle;
import io.deephaven.plot.datasets.AbstractDataSeries;
import io.deephaven.plot.datasets.data.*;
import io.deephaven.plot.errors.*;
import io.deephaven.plot.filters.SelectableDataSet;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.plot.util.PlotUtils;
import io.deephaven.plot.util.functions.ClosureFunction;
import io.deephaven.plot.util.tables.ColumnHandlerFactory;
import io.deephaven.plot.util.tables.SwappableTable;
import io.deephaven.plot.util.tables.TableHandle;
import io.deephaven.engine.table.Table;
import io.deephaven.gui.color.Color;
import io.deephaven.gui.color.Paint;
import io.deephaven.gui.shape.NamedShape;
import io.deephaven.gui.shape.Shape;
import groovy.lang.Closure;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;

import static io.deephaven.plot.util.PlotUtils.intToColor;
import static io.deephaven.util.QueryConstants.*;

/**
 * Common class for {@link CategoryDataSeriesInternal}.
 * <p>
 * Please note that any method which uses a {@link SwappableTable} must call a lastBy()!
 */
public abstract class AbstractCategoryDataSeries extends AbstractDataSeries implements CategoryDataSeriesInternal {

    private static final long serialVersionUID = 6881532832713307316L;
    private final AssociativeDataWithDefault<Comparable, Paint> colors;
    private final AssociativeDataWithDefault<Comparable, String> labels;
    private final AssociativeDataWithDefault<Comparable, Double> sizes;
    private final AssociativeDataWithDefault<Comparable, Shape> shapes;
    private String piePercentLabelFormat = null;
    private int group = 1;


    public AbstractCategoryDataSeries(final AxesImpl axes, final int id, final Comparable name) {
        this(axes, id, name, null);
    }

    /**
     * Creates an AbstractCategoryDataSeries instance.
     *
     * @param axes axes on which this series will be plotted
     * @param id data series id
     * @param name series name
     * @param series series to copy
     *
     */
    public AbstractCategoryDataSeries(final AxesImpl axes, final int id, final Comparable name,
            final AbstractCategoryDataSeries series) {
        super(axes, id, name, series);

        colors = new AssociativeDataWithDefault<>(getPlotInfo());
        labels = new AssociativeDataWithDefault<>(getPlotInfo());
        sizes = new AssociativeDataWithDefault<>(getPlotInfo());
        shapes = new AssociativeDataWithDefault<>(getPlotInfo());

        if (series != null) {
            this.colors.set(series.colors);
            this.labels.set(series.labels);
            this.sizes.set(series.sizes);
            this.shapes.set(series.shapes);
            this.group = series.group;
            this.piePercentLabelFormat = series.piePercentLabelFormat;
        }
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    protected AbstractCategoryDataSeries(final AbstractCategoryDataSeries series, final AxesImpl axes) {
        super(series, axes);

        colors = new AssociativeDataWithDefault<>(series.getPlotInfo());
        labels = new AssociativeDataWithDefault<>(series.getPlotInfo());
        sizes = new AssociativeDataWithDefault<>(series.getPlotInfo());
        shapes = new AssociativeDataWithDefault<>(series.getPlotInfo());

        this.colors.set(series.colors);
        this.labels.set(series.labels);
        this.sizes.set(series.sizes);
        this.shapes.set(series.shapes);
        this.group = series.group;
        this.piePercentLabelFormat = series.piePercentLabelFormat;
    }

    ////////////////////////// internal //////////////////////////

    protected void colorsSetSpecific(final AssociativeData<Comparable, Paint> data) {
        colors.setSpecific(data);
    }

    protected void labelsSetSpecific(final AssociativeData<Comparable, String> data) {
        labels.setSpecific(data);
    }

    protected void sizesSetSpecific(final AssociativeData<Comparable, Double> data) {
        sizes.setSpecific(data);
    }

    protected void shapesSetSpecific(final AssociativeData<Comparable, Shape> data) {
        shapes.setSpecific(data);
    }

    @Override
    public Paint getColor(final Comparable category) {
        return colors.get(category);
    }

    @Override
    public Double getPointSize(final Comparable category) {
        return PlotUtils.numberToDouble(sizes.get(category));
    }

    @Override
    public Paint getSeriesColor() {
        return colors.getDefault();
    }

    @Override
    public String getLabel(final Comparable category) {
        return labels.get(category);
    }

    @Override
    public Shape getPointShape(final Comparable category) {
        return shapes.get(category);
    }

    @Override
    public int getGroup() {
        return group;
    }

    @Override
    public String getPiePercentLabelFormat() {
        return piePercentLabelFormat;
    }

    //////////////////////// data organization ////////////////////////


    @Override
    public AbstractCategoryDataSeries group(final int group) {
        this.group = group;
        return this;
    }


    ////////////////////////// visibility //////////////////////////


    @Override
    public AbstractCategoryDataSeries linesVisible(final Boolean visible) {
        setLinesVisible(visible);
        return this;
    }

    @Override
    public AbstractCategoryDataSeries pointsVisible(final Boolean visible) {
        setPointsVisible(visible);
        return this;
    }

    @Override
    public AbstractCategoryDataSeries gradientVisible(final boolean gradientVisible) {
        setGradientVisible(gradientVisible);
        return this;
    }


    ////////////////////////// color //////////////////////////


    /**
     * Sets the default {@link Paint} for this dataset.
     *
     * @param color color
     * @return this CategoryDataSeries
     */
    public AbstractCategoryDataSeries seriesColor(final Paint color) {
        lineColor(color);
        pointColor(color);
        return this;
    }

    /**
     * Sets the default {@link Paint} for this dataset.
     *
     * @param color index of the color in the series color palette
     * @return this CategoryDataSeries
     */
    public AbstractCategoryDataSeries seriesColor(final int color) {
        lineColor(color);
        pointColor(color);
        return this;
    }

    /**
     * Sets the default {@link Paint} for this dataset.
     *
     * @param color color
     * @return this CategoryDataSeries
     */
    public AbstractCategoryDataSeries seriesColor(final String color) {
        lineColor(color);
        pointColor(color);
        return this;
    }


    ////////////////////////// line color //////////////////////////


    @Override
    public AbstractCategoryDataSeries lineColor(final Paint color) {
        setLineColor(color);
        return this;
    }

    @Override
    public AbstractCategoryDataSeries lineColor(final int color) {
        setLineColor(color);
        return this;
    }

    @Override
    public AbstractCategoryDataSeries lineColor(final String color) {
        setLineColor(color);
        return this;
    }


    ////////////////////////// error bar color //////////////////////////


    @Override
    public AbstractCategoryDataSeries errorBarColor(final Paint errorBarColor) {
        setErrorBarColor(errorBarColor);
        return this;
    }

    @Override
    public AbstractCategoryDataSeries errorBarColor(final int errorBarColor) {
        setErrorBarColor(errorBarColor);
        return this;
    }

    @Override
    public AbstractCategoryDataSeries errorBarColor(final String errorBarColor) {
        setErrorBarColor(errorBarColor);
        return this;
    }


    ////////////////////////// line style //////////////////////////


    @Override
    public AbstractCategoryDataSeries lineStyle(final LineStyle lineStyle) {
        setLineStyle(lineStyle);
        return this;
    }

    ////////////////////////// point sizes //////////////////////////


    @Override
    public AbstractCategoryDataSeries pointSize(final int pointSize) {
        return pointSize(pointSize == NULL_INT ? null : pointSize);
    }

    @Override
    public AbstractCategoryDataSeries pointSize(final long pointSize) {
        return pointSize(pointSize == NULL_LONG ? null : pointSize);
    }

    @Override
    public AbstractCategoryDataSeries pointSize(final double pointSize) {
        sizes.setDefault(pointSize == NULL_DOUBLE || pointSize == Double.NaN ? null : pointSize);
        return this;
    }

    @Override
    public AbstractCategoryDataSeries pointSize(final Number pointSize) {
        sizes.setDefault(pointSize == null ? null : pointSize.doubleValue());
        return this;
    }

    @Override
    public AbstractCategoryDataSeries pointSize(final Comparable category, final int pointSize) {
        if (!sizes.isModifiable()) {
            sizes.setSpecific(new AssociativeDataHashMap<>(getPlotInfo()));
        }

        sizes.put(category, (double) pointSize);
        return this;
    }

    @Override
    public AbstractCategoryDataSeries pointSize(final Comparable category, final long pointSize) {
        if (!sizes.isModifiable()) {
            sizes.setSpecific(new AssociativeDataHashMap<>(getPlotInfo()));
        }

        sizes.put(category, (double) pointSize);
        return this;
    }

    @Override
    public AbstractCategoryDataSeries pointSize(final Comparable category, final double pointSize) {
        if (!sizes.isModifiable()) {
            sizes.setSpecific(new AssociativeDataHashMap<>(getPlotInfo()));
        }

        sizes.put(category, pointSize);
        return this;
    }

    @Override
    public AbstractCategoryDataSeries pointSize(final Comparable category, final Number pointSize) {
        if (!sizes.isModifiable()) {
            sizes.setSpecific(new AssociativeDataHashMap<>(getPlotInfo()));
        }

        sizes.put(category, pointSize == null ? null : pointSize.doubleValue());
        return this;
    }

    @Override
    public <CATEGORY extends Comparable> AbstractCategoryDataSeries pointSize(final CATEGORY[] categories,
            int[] pointSizes) {
        return pointSize(categories, PlotUtils.toDouble(pointSizes));
    }

    @Override
    public <CATEGORY extends Comparable> AbstractCategoryDataSeries pointSize(final CATEGORY[] categories,
            long[] pointSizes) {
        return pointSize(categories, PlotUtils.toDouble(pointSizes));
    }

    @Override
    public <CATEGORY extends Comparable, NUMBER extends Number> AbstractCategoryDataSeries pointSize(
            final CATEGORY[] categories, NUMBER[] pointSizes) {
        return pointSize(categories, PlotUtils.toDouble(pointSizes));
    }


    @Override
    public <CATEGORY extends Comparable> AbstractCategoryDataSeries pointSize(final CATEGORY[] categories,
            double[] pointSizes) {
        ArgumentValidations.assertNotNull(categories, "categories", getPlotInfo());
        ArgumentValidations.assertNotNull(pointSizes, "factors", getPlotInfo());
        Require.eq(categories.length, "categories.length", pointSizes.length, "factors.length");

        if (!sizes.isModifiable()) {
            sizes.setSpecific(new AssociativeDataHashMap<>(getPlotInfo()));
        }

        for (int i = 0; i < categories.length; i++) {
            sizes.put(categories[i], pointSizes[i]);
        }
        return this;
    }

    @Override
    public <CATEGORY extends Comparable, NUMBER extends Number> AbstractCategoryDataSeries pointSize(
            final Map<CATEGORY, NUMBER> pointSizes) {
        if (!this.sizes.isModifiable()) {
            this.sizes.setSpecific(new AssociativeDataHashMap<>(getPlotInfo()));
        }

        for (Map.Entry<CATEGORY, NUMBER> entry : pointSizes.entrySet()) {
            final Comparable k = entry.getKey();
            final NUMBER v = entry.getValue();
            this.sizes.put(k, v == null ? null : v.doubleValue());
        }

        return this;
    }

    @Override
    public AbstractCategoryDataSeries pointSize(Table t, String category, String pointSize) {
        ArgumentValidations.assertNotNull(t, "table", getPlotInfo());
        ArgumentValidations.assertNotNull(category, "keyColumn", getPlotInfo());
        ArgumentValidations.assertNotNull(pointSize, "valueColumn", getPlotInfo());

        final TableHandle tableHandle = new TableHandle(t, category, pointSize);
        addTableHandle(tableHandle);

        ArgumentValidations.assertInstance(t, category, Comparable.class,
                "key column is not a supported type (Comparable): keyColumn=" + category, getPlotInfo());

        sizes.setSpecific(new AssociativeDataTable<Comparable, Double, Number>(tableHandle, category, pointSize,
                Comparable.class, Number.class, getPlotInfo()) {
            @Override
            public Double convert(Number v) {
                return PlotUtils.numberToDouble(v);
            }
        });

        return this;
    }

    @Override
    public AbstractCategoryDataSeries pointSize(SelectableDataSet sds, String category, String pointSize) {
        ArgumentValidations.assertNotNull(sds, "sds", getPlotInfo());
        ArgumentValidations.assertNotNull(category, "keyColumn", getPlotInfo());
        ArgumentValidations.assertNotNull(pointSize, "valueColumn", getPlotInfo());

        final Function<Table, Table> tableTransform =
                (Function<Table, Table> & Serializable) table -> table.lastBy(category);
        final SwappableTable t = sds.getSwappableTable(name(), chart(), tableTransform, pointSize);
        addSwappableTable(t);

        ArgumentValidations.assertInstance(t.getTableDefinition(), category, Comparable.class,
                "key column is not a supported type (Comparable): keyColumn=" + category, getPlotInfo());

        sizes.setSpecific(new AssociativeDataSwappableTable<Comparable, Double, Number>(t, category, pointSize,
                Comparable.class, Number.class, getPlotInfo()) {
            @Override
            public Double convert(Number v) {
                return PlotUtils.numberToDouble(v);
            }
        });

        return this;
    }


    ////////////////////////// point colors //////////////////////////


    @Override
    public AbstractCategoryDataSeries pointColor(final Paint pointColor) {
        colors.setDefault(pointColor);
        return this;
    }

    @Override
    public AbstractCategoryDataSeries pointColor(final int pointColor) {
        return pointColor(intToColor(chart(), pointColor));
    }

    @Override
    public CategoryDataSeriesInternal pointColor(final String pointColor) {
        return pointColor(Color.color(pointColor));
    }

    @Override
    public AbstractCategoryDataSeries pointColor(final Comparable category, final Paint color) {
        if (!colors.isModifiable()) {
            colors.setSpecific(new AssociativeDataHashMap<>(getPlotInfo()));
        }

        colors.put(category, color);
        return this;
    }

    @Override
    public AbstractCategoryDataSeries pointColor(final Comparable category, final int pointColor) {
        return pointColor(category, PlotUtils.intToColor(chart(), pointColor));
    }

    @Override
    public CategoryDataSeriesInternal pointColor(final Comparable category, final String pointColor) {
        return pointColor(category, Color.color(pointColor));
    }

    @Override
    public <CATEGORY extends Comparable, COLOR extends Paint> AbstractCategoryDataSeries pointColor(
            final Map<CATEGORY, COLOR> pointColor) {
        if (!this.colors.isModifiable()) {
            this.colors.setSpecific(new AssociativeDataHashMap<>(getPlotInfo()));
        }

        this.colors.putAll(pointColor);
        return this;
    }

    @Override
    public <CATEGORY extends Comparable, COLOR extends Integer> AbstractCategoryDataSeries pointColorInteger(
            final Map<CATEGORY, COLOR> colors) {
        if (!this.colors.isModifiable()) {
            this.colors.setSpecific(new AssociativeDataHashMap<>(getPlotInfo()));
        }

        for (Map.Entry<CATEGORY, COLOR> c : colors.entrySet()) {
            this.colors.put(c.getKey(), intToColor(chart(), c.getValue()));
        }

        return this;
    }

    @Override
    public AbstractCategoryDataSeries pointColor(final Table t, final String category, final String pointColor) {
        ArgumentValidations.assertNotNull(t, "table", getPlotInfo());
        ArgumentValidations.assertNotNull(category, "keyColumn", getPlotInfo());
        ArgumentValidations.assertNotNull(pointColor, "valueColumn", getPlotInfo());
        ArgumentValidations.assertInstance(t, category, Comparable.class,
                "key column is not a supported type (Comparable): keyColumn=" + category, getPlotInfo());

        final TableHandle tableHandle = new TableHandle(t, category, pointColor);
        addTableHandle(tableHandle);
        final ColumnHandlerFactory.ColumnHandler valueColumnHandler =
                ColumnHandlerFactory.newNumericHandler(tableHandle, pointColor, getPlotInfo());

        if (valueColumnHandler.typeClassification().equals(ColumnHandlerFactory.TypeClassification.INTEGER) &&
                (valueColumnHandler.type() == int.class || valueColumnHandler.type() == Integer.class)) {
            this.colors.setSpecific(new AssociativeDataTableComparablePaint(tableHandle, category, pointColor,
                    chart(), getPlotInfo()));
        } else if (valueColumnHandler.typeClassification().equals(ColumnHandlerFactory.TypeClassification.PAINT)) {
            this.colors.setSpecific(new AssociativeDataTable<>(tableHandle, category, pointColor, Comparable.class,
                    Paint.class, getPlotInfo()));
        } else {
            throw new PlotUnsupportedOperationException("Column can not be converted into a color: column="
                    + pointColor + "\ttype=" + valueColumnHandler.type(), this);
        }

        return this;
    }

    @Override
    public AbstractCategoryDataSeries pointColor(final SelectableDataSet sds, final String category,
            final String pointColor) {
        ArgumentValidations.assertNotNull(sds, "sds", getPlotInfo());
        ArgumentValidations.assertNotNull(category, "keyColumn", getPlotInfo());
        ArgumentValidations.assertNotNull(pointColor, "valueColumn", getPlotInfo());
        ArgumentValidations.assertInstance(sds, category, Comparable.class,
                "key column is not a supported type (Comparable): keyColumn=" + category, getPlotInfo());

        final Class type = sds.getTableDefinition().getColumn(pointColor).getDataType();
        final boolean isInt = type.equals(int.class) || type.equals(Integer.class) || type.equals(short.class)
                || type.equals(Short.class);
        final boolean isPaint = Paint.class.isAssignableFrom(type);

        if (!isInt && !isPaint) {
            throw new PlotUnsupportedOperationException(
                    "Column can not be converted into a color: column=" + pointColor + "\ttype=" + type, this);
        }

        final Function<Table, Table> tableTransform =
                (Function<Table, Table> & Serializable) table -> table.lastBy(category);
        final SwappableTable t = sds.getSwappableTable(name(), chart(), tableTransform, category, pointColor);
        addSwappableTable(t);

        if (isInt) {
            this.colors.setSpecific(new AssociativeDataSwappableTableComparablePaint(t, category, pointColor, chart(),
                    getPlotInfo()));
        } else if (isPaint) {
            this.colors.setSpecific(new AssociativeDataSwappableTable<>(t, category, pointColor, Comparable.class,
                    Paint.class, getPlotInfo()));
        } else {
            throw new PlotIllegalStateException("Should never reach here", this);
        }

        return this;
    }


    ////////////////////////// point labels //////////////////////////


    @Override
    public AbstractCategoryDataSeries pointLabel(final Object pointLabel) {
        labels.setDefault(pointLabel == null ? null : pointLabel.toString());
        return this;
    }

    @Override
    public AbstractCategoryDataSeries pointLabel(final Comparable category, final Object pointLabel) {
        if (!labels.isModifiable()) {
            labels.setSpecific(new AssociativeDataHashMap<>(getPlotInfo()));
        }

        labels.put(category, pointLabel == null ? null : pointLabel.toString());
        return this;
    }

    @Override
    public <CATEGORY extends Comparable, LABEL> AbstractCategoryDataSeries pointLabel(
            final Map<CATEGORY, LABEL> pointLabels) {
        if (!this.labels.isModifiable()) {
            this.labels.setSpecific(new AssociativeDataHashMap<>(getPlotInfo()));
        }

        for (Map.Entry<CATEGORY, LABEL> l : pointLabels.entrySet()) {
            this.labels.put(l.getKey(), l.getValue() == null ? null : l.getValue().toString());
        }

        return this;
    }

    @Override
    public AbstractCategoryDataSeries pointLabel(final Table t, final String category, final String pointLabel) {
        final TableHandle tableHandle = new TableHandle(t, category, pointLabel);
        addTableHandle(tableHandle);
        ArgumentValidations.assertInstance(t, category, Comparable.class,
                "key column is not a supported type (Comparable): keyColumn=" + category, getPlotInfo());

        this.labels.setSpecific(new AssociativeDataTableLabel(tableHandle, category, pointLabel, getPlotInfo()));

        return this;
    }

    @Override
    public AbstractCategoryDataSeries pointLabelFormat(final String pointLabelFormat) {
        setPointLabelFormat(pointLabelFormat);
        return this;
    }

    @Override
    public AbstractCategoryDataSeries piePercentLabelFormat(final String pieLabelFormat) {
        setPiePercentLabelFormat(pieLabelFormat);
        return this;
    }

    @Override
    public AbstractCategoryDataSeries xToolTipPattern(final String xToolTipPattern) {
        setXToolTipPattern(xToolTipPattern);
        return this;
    }

    @Override
    public AbstractCategoryDataSeries yToolTipPattern(final String yToolTipPattern) {
        setYToolTipPattern(yToolTipPattern);
        return this;
    }

    @Override
    public AbstractCategoryDataSeries pointLabel(final SelectableDataSet sds, final String category,
            final String pointLabel) {
        ArgumentValidations.assertColumnsInTable(sds, getPlotInfo(), category, pointLabel);
        ArgumentValidations.assertInstance(sds, category, Comparable.class,
                "key column is not a supported type (Comparable): keyColumn=" + category, getPlotInfo());

        final Function<Table, Table> tableTransform =
                (Function<Table, Table> & Serializable) table -> table.lastBy(category);
        final SwappableTable t = sds.getSwappableTable(name(), chart(), tableTransform, category, pointLabel);
        addSwappableTable(t);
        this.labels.setSpecific(new AssociativeDataSwappableTableLabel(t, category, pointLabel, getPlotInfo()));

        return this;
    }

    @Override
    public AbstractCategoryDataSeries pointShape(final String pointShape) {
        return pointShape(NamedShape.getShape(pointShape));
    }

    @Override
    public AbstractCategoryDataSeries pointShape(final Shape pointShape) {
        shapes.setDefault(pointShape);
        return this;
    }

    @Override
    public CategoryDataSeries pointShape(final Comparable category, final String pointShape) {
        return pointShape(category, NamedShape.getShape(pointShape));
    }

    @Override
    public CategoryDataSeries pointShape(final Comparable category, final Shape pointShape) {
        if (!shapes.isModifiable()) {
            shapes.setSpecific(new AssociativeDataHashMap<>(getPlotInfo()));
        }

        shapes.put(category, pointShape);
        return this;
    }

    @Override
    public <CATEGORY extends Comparable> CategoryDataSeries pointShape(final Map<CATEGORY, String> pointShapes) {
        ArgumentValidations.assertNotNull(pointShapes, "shapes", getPlotInfo());

        if (!this.shapes.isModifiable()) {
            this.shapes.setSpecific(new AssociativeDataHashMap<>(getPlotInfo()));
        }

        for (Map.Entry<CATEGORY, String> l : pointShapes.entrySet()) {
            try {
                this.shapes.put(l.getKey(), NamedShape.getShape(l.getValue()));
            } catch (final IllegalArgumentException iae) {
                this.shapes.setSpecific(null);
                throw new PlotIllegalArgumentException("Not a valid shape: `" + l.getValue() + "` for category:"
                        + l.getKey() + "; valid shapes: " + NamedShape.getShapesString(), this);
            }
        }

        return this;
    }

    @Override
    public CategoryDataSeries pointShape(final Table t, final String category, final String pointShape) {
        ArgumentValidations.assertNotNull(t, "t", getPlotInfo());
        ArgumentValidations.assertNotNull(category, "keyColumn", getPlotInfo());
        ArgumentValidations.assertNotNull(pointShape, "valueColumn", getPlotInfo());

        ArgumentValidations.assertInstance(t, category, Comparable.class,
                "key column is not a supported type (Comparable): keyColumn=" + category, getPlotInfo());

        final Class columnType = ArgumentValidations.getColumnType(t, pointShape, getPlotInfo());
        if (String.class.isAssignableFrom(columnType)) {
            final TableHandle tableHandle = new TableHandle(t, category, pointShape);
            addTableHandle(tableHandle);
            this.shapes.setSpecific(
                    new AssociativeDataTablePointShapeString(tableHandle, category, pointShape, getPlotInfo()));
        } else if (Shape.class.isAssignableFrom(columnType)) {
            final TableHandle tableHandle = new TableHandle(t, category, pointShape);
            addTableHandle(tableHandle);
            this.shapes.setSpecific(
                    new AssociativeDataTablePointShapeObj(tableHandle, category, pointShape, getPlotInfo()));
        } else {
            throw new PlotRuntimeException(
                    "column is not a supported type (String or Shape): columnName=" + pointShape, this);
        }

        return this;
    }

    @Override
    public CategoryDataSeries pointShape(final SelectableDataSet sds, final String category,
            final String pointShape) {
        ArgumentValidations.assertNotNull(sds, "sds", getPlotInfo());
        ArgumentValidations.assertNotNull(category, "keyColumn", getPlotInfo());
        ArgumentValidations.assertNotNull(pointShape, "valueColumn", getPlotInfo());

        ArgumentValidations.assertColumnsInTable(sds, getPlotInfo(), category, pointShape);
        ArgumentValidations.assertInstance(sds, category, Comparable.class,
                "key column is not a supported type (Comparable): keyColumn=" + category, getPlotInfo());

        final SwappableTable swappableTable = sds.getSwappableTable(name(), chart(), category, pointShape);
        final Class columnType = ArgumentValidations.getColumnType(sds, pointShape, getPlotInfo());
        if (String.class.isAssignableFrom(columnType)) {
            addSwappableTable(swappableTable);
            this.shapes.setSpecific(new AssociativeDataSwappableTablePointShapeString(swappableTable, category,
                    pointShape, getPlotInfo()));
        } else if (Shape.class.isAssignableFrom(columnType)) {
            addSwappableTable(swappableTable);
            this.shapes.setSpecific(new AssociativeDataSwappableTablePointShapeObj(swappableTable, category,
                    pointShape, getPlotInfo()));
        } else {
            throw new PlotRuntimeException(
                    "column is not a supported type (String OR Shape): columnName=" + pointShape, this);
        }
        return this;
    }

    private static class AssociativeDataTablePointShapeString extends AssociativeDataTable<Comparable, Shape, String> {

        private static final long serialVersionUID = -782616004116345049L;

        AssociativeDataTablePointShapeString(final TableHandle handle, final String keyColumn, final String valueColumn,
                final PlotInfo plotInfo) {
            super(handle, keyColumn, valueColumn, Comparable.class, String.class, plotInfo);
        }

        @Override
        public Shape convert(final String v) {
            return NamedShape.getShape(v);
        }
    }

    private static class AssociativeDataTablePointShapeObj extends AssociativeDataTable<Comparable, Shape, Shape> {


        private static final long serialVersionUID = -2868764888409198544L;

        AssociativeDataTablePointShapeObj(final TableHandle handle, final String keyColumn, final String valueColumn,
                final PlotInfo plotInfo) {
            super(handle, keyColumn, valueColumn, Comparable.class, Shape.class, plotInfo);
        }

        @Override
        public Shape convert(final Shape v) {
            return v;
        }
    }

    private static class AssociativeDataSwappableTablePointShapeString
            extends AssociativeDataSwappableTable<Comparable, Shape, String> {

        private static final long serialVersionUID = 120758160744582475L;

        AssociativeDataSwappableTablePointShapeString(final SwappableTable t, final String keyColumn,
                final String valueColumn, final PlotInfo plotInfo) {
            super(t, keyColumn, valueColumn, Comparable.class, String.class, plotInfo);
        }

        @Override
        public Shape convert(final String v) {
            return NamedShape.getShape(v);
        }
    }

    private static class AssociativeDataSwappableTablePointShapeObj
            extends AssociativeDataSwappableTable<Comparable, Shape, Shape> {

        private static final long serialVersionUID = -682972849470492883L;

        AssociativeDataSwappableTablePointShapeObj(final SwappableTable t, final String keyColumn,
                final String valueColumn, final PlotInfo plotInfo) {
            super(t, keyColumn, valueColumn, Comparable.class, Shape.class, plotInfo);
        }

        @Override
        public Shape convert(final Shape v) {
            return v;
        }
    }

    private static class AssociativeDataPaintByYMap<COLOR extends Paint> extends AssociativeData<Comparable, Paint>
            implements Serializable {

        private static final long serialVersionUID = 1040533194319869777L;
        private final Map<Double, COLOR> colors;
        private final AbstractCategoryDataSeries dataSeries;

        private AssociativeDataPaintByYMap(final Map<Double, COLOR> colors,
                final AbstractCategoryDataSeries dataSeries) {
            super(dataSeries.getPlotInfo());
            this.colors = colors;
            this.dataSeries = dataSeries;
        }

        @Override
        public Paint get(Comparable key) {
            return colors.get(dataSeries.getValue(key).doubleValue());
        }

        @Override
        public boolean isModifiable() {
            return false;
        }

        @Override
        public void put(Comparable comparable, Paint paint) {
            throw new PlotUnsupportedOperationException("AssociativeDataPaintByY can not be modified", this);
        }

        @Override
        public <K extends Comparable, V extends Paint> void putAll(Map<K, V> values) {
            throw new PlotUnsupportedOperationException("AssociativeDataPaintByY can not be modified", this);
        }
    }

    private static class AssociativeDataTableComparablePaint extends AssociativeDataTable<Comparable, Paint, Integer> {

        private static final long serialVersionUID = 2872945661540856625L;
        private final ChartImpl chart;

        AssociativeDataTableComparablePaint(final TableHandle handle, final String keyColumn, final String valueColumn,
                final ChartImpl chart, final PlotInfo plotInfo) {
            super(handle, keyColumn, valueColumn, Comparable.class, Integer.class, plotInfo);
            this.chart = chart;
        }

        @Override
        public Paint convert(Integer v) {
            return intToColor(chart, v);
        }
    }

    private static class AssociativeDataSwappableTableComparablePaint
            extends AssociativeDataSwappableTable<Comparable, Paint, Integer> {

        private static final long serialVersionUID = -644994476705986379L;
        private final ChartImpl chart;

        AssociativeDataSwappableTableComparablePaint(final SwappableTable t, final String keyColumn,
                final String valueColumn, final ChartImpl chart, final PlotInfo plotInfo) {
            super(t, keyColumn, valueColumn, Comparable.class, Integer.class, plotInfo);
            this.chart = chart;
        }

        @Override
        public Paint convert(Integer v) {
            return intToColor(chart, v);
        }
    }

    private static class AssociativeDataTableLabel extends AssociativeDataTable<Comparable, String, Object> {
        private static final long serialVersionUID = -2209957632708434850L;

        AssociativeDataTableLabel(final TableHandle handle, final String keyColumn, final String valueColumn,
                final PlotInfo plotInfo) {
            super(handle, keyColumn, valueColumn, Comparable.class, Object.class, plotInfo);
        }

        @Override
        public String convert(Object v) {
            return v == null ? null : v.toString();
        }
    }

    private static class AssociativeDataSwappableTableLabel
            extends AssociativeDataSwappableTable<Comparable, String, Object> {
        private static final long serialVersionUID = -7266731699707547063L;

        // The SwappableTable needs to have a lastBy applied
        AssociativeDataSwappableTableLabel(final SwappableTable t, final String keyColumn, final String valueColumn,
                final PlotInfo plotInfo) {
            super(t, keyColumn, valueColumn, Comparable.class, Object.class, plotInfo);
        }

        @Override
        public String convert(Object v) {
            return v == null ? null : v.toString();
        }
    }


    ////////////////////// tool tips /////////////////////////////


    /**
     * Sets the pie percent label format for this dataset.
     *
     * @param format format
     */
    protected void setPiePercentLabelFormat(String format) {
        this.piePercentLabelFormat = format;
    }

    @Override
    public AbstractCategoryDataSeries toolTipPattern(final String toolTipPattern) {
        xToolTipPattern(toolTipPattern);
        yToolTipPattern(toolTipPattern);
        return this;
    }

}
