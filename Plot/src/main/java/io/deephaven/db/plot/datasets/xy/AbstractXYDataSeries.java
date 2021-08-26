/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.xy;


import io.deephaven.db.plot.AxesImpl;
import io.deephaven.db.plot.ChartImpl;
import io.deephaven.db.plot.LineStyle;
import io.deephaven.db.plot.datasets.AbstractDataSeries;
import io.deephaven.db.plot.datasets.DataSeries;
import io.deephaven.db.plot.datasets.data.*;
import io.deephaven.db.plot.errors.*;
import io.deephaven.db.plot.filters.SelectableDataSet;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.plot.util.PlotUtils;
import io.deephaven.db.plot.util.functions.ClosureFunction;
import io.deephaven.db.plot.util.tables.ColumnHandlerFactory;
import io.deephaven.db.plot.util.tables.SwappableTable;
import io.deephaven.db.plot.util.tables.TableHandle;
import io.deephaven.db.tables.Table;
import io.deephaven.gui.color.Color;
import io.deephaven.gui.color.Paint;
import io.deephaven.gui.shape.NamedShape;
import io.deephaven.gui.shape.Shape;
import groovy.lang.Closure;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Function;

import static io.deephaven.db.plot.util.PlotUtils.intToColor;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * Base class for the {@link XYDataSeriesInternal}.
 */
public abstract class AbstractXYDataSeries extends AbstractDataSeries implements XYDataSeriesInternal {

    private static final long serialVersionUID = 5353144043894861970L;
    private final IndexableDataWithDefault<String> shapeLabels;
    private final IndexableDataWithDefault<Double> shapeSizes;
    private final IndexableDataWithDefault<Paint> shapeColors;
    private final IndexableDataWithDefault<Shape> pointShapes;

    /**
     * Creates an AbstractXYDataSeries instance.
     *
     * @param axes axes on which the data will be plotted
     * @param id data series id
     * @param name series name
     */
    public AbstractXYDataSeries(final AxesImpl axes, final int id, final Comparable name,
            final AbstractXYDataSeries series) {
        super(axes, id, name, series);
        shapeLabels = new IndexableDataWithDefault<>(getPlotInfo());
        shapeSizes = new IndexableDataWithDefault<>(getPlotInfo());
        shapeColors = new IndexableDataWithDefault<>(getPlotInfo());
        pointShapes = new IndexableDataWithDefault<>(getPlotInfo());

        if (series != null) {
            this.shapeLabels.set(series.shapeLabels);
            this.shapeSizes.set(series.shapeSizes);
            this.shapeColors.set(series.shapeColors);
            this.pointShapes.set(series.pointShapes);
        }
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    protected AbstractXYDataSeries(final AbstractXYDataSeries series, final AxesImpl axes) {
        super(series, axes);

        shapeLabels = new IndexableDataWithDefault<>(series.getPlotInfo());
        shapeSizes = new IndexableDataWithDefault<>(series.getPlotInfo());
        shapeColors = new IndexableDataWithDefault<>(series.getPlotInfo());
        pointShapes = new IndexableDataWithDefault<>(series.getPlotInfo());

        this.shapeLabels.set(series.shapeLabels);
        this.shapeSizes.set(series.shapeSizes);
        this.shapeColors.set(series.shapeColors);
        this.pointShapes.set(series.pointShapes);
    }

    ////////////////////////// internal //////////////////////////

    protected void colorsSetSpecific(final IndexableData<Paint> data) {
        shapeColors.setSpecific(data, true);
    }

    @Override
    public Paint getSeriesColor() {
        return shapeColors.getDefaultValue();
    }

    @Override
    public String getPointLabel(int i) {
        return shapeLabels.get(i);
    }

    @Override
    public Shape getPointShape(int i) {
        return pointShapes.get(i);
    }

    @Override
    public Double getPointSize(int i) {
        return shapeSizes.get(i);
    }

    @Override
    public Paint getPointColor(int i) {
        return shapeColors.get(i);
    }



    ////////////////////////// visibility //////////////////////////


    @Override
    public AbstractXYDataSeries linesVisible(Boolean visible) {
        setLinesVisible(visible);
        return this;
    }

    @Override
    public AbstractXYDataSeries pointsVisible(Boolean visible) {
        setPointsVisible(visible);
        return this;
    }

    @Override
    public AbstractXYDataSeries gradientVisible(boolean visible) {
        setGradientVisible(visible);
        return this;
    }


    ////////////////////////// line color //////////////////////////


    @Override
    public AbstractXYDataSeries lineColor(final Paint color) {
        setLineColor(color);
        return this;
    }

    @Override
    public AbstractXYDataSeries lineColor(final int color) {
        setLineColor(color);
        return this;
    }

    @Override
    public AbstractXYDataSeries lineColor(final String color) {
        setLineColor(color);
        return this;
    }


    ////////////////////////// error bar color //////////////////////////


    @Override
    public AbstractXYDataSeries errorBarColor(final Paint color) {
        setErrorBarColor(color);
        return this;
    }

    @Override
    public AbstractXYDataSeries errorBarColor(final int color) {
        setErrorBarColor(color);
        return this;
    }

    @Override
    public AbstractXYDataSeries errorBarColor(final String color) {
        setErrorBarColor(color);
        return this;
    }


    ////////////////////////// line style //////////////////////////


    @Override
    public AbstractXYDataSeries lineStyle(final LineStyle style) {
        setLineStyle(style);
        return this;
    }


    ////////////////////////// color //////////////////////////


    public AbstractXYDataSeries seriesColor(final Paint color) {
        lineColor(color);
        pointColor(color);
        return this;
    }

    public AbstractXYDataSeries seriesColor(final int color) {
        lineColor(color);
        pointColor(color);
        return this;
    }

    public AbstractXYDataSeries seriesColor(final String color) {
        lineColor(color);
        pointColor(color);
        return this;
    }


    ////////////////////////// point sizes //////////////////////////


    @Override
    public XYDataSeriesInternal pointSize(final int factor) {
        return pointSize(factor == NULL_INT ? null : factor);
    }

    @Override
    public XYDataSeriesInternal pointSize(final long factor) {
        return pointSize(factor == NULL_LONG ? null : factor);
    }

    @Override
    public XYDataSeriesInternal pointSize(final double factor) {
        shapeSizes.setDefault(factor == NULL_DOUBLE || factor == Double.NaN ? null : factor);
        return this;
    }

    @Override
    public XYDataSeriesInternal pointSize(final Number factor) {
        shapeSizes.setDefault(factor == null ? null : factor.doubleValue());
        return this;
    }

    @Override
    public AbstractXYDataSeries pointSize(final IndexableData<Double> factors) {
        ArgumentValidations.assertNotNull(factors, "factors", getPlotInfo());
        shapeSizes.setSpecific(factors, true);
        return this;
    }

    @Override
    public AbstractXYDataSeries pointSize(final int... factors) {
        return pointSize(new IndexableDataDouble(factors, true, getPlotInfo()));
    }

    @Override
    public AbstractXYDataSeries pointSize(final long... factors) {
        return pointSize(new IndexableDataDouble(factors, true, getPlotInfo()));
    }

    @Override
    public AbstractXYDataSeries pointSize(final double... factors) {
        return pointSize(new IndexableDataDouble(factors, true, getPlotInfo()));
    }

    @Override
    public <T extends Number> AbstractXYDataSeries pointSize(final T[] factors) {
        return pointSize(new IndexableDataDouble(factors, true, getPlotInfo()));
    }

    @Override
    public AbstractXYDataSeries pointSize(final Table t, final String columnName) {
        ArgumentValidations.assertNotNull(t, "table", getPlotInfo());
        ArgumentValidations.assertNotNull(columnName, "columnName", getPlotInfo());

        final TableHandle tableHandle = new TableHandle(t, columnName);
        addTableHandle(tableHandle);
        final ColumnHandlerFactory.ColumnHandler columnHandler =
                ColumnHandlerFactory.newNumericHandler(tableHandle, columnName, getPlotInfo());

        if (columnHandler.typeClassification().isNumeric()) {
            shapeSizes.setSpecific(new IndexableDataTable<Double>(columnHandler, getPlotInfo()) {
                @Override
                public Double convert(Object v) {
                    return PlotUtils.numberToDouble((Number) v);
                }
            }, true);
        } else {
            throw new PlotUnsupportedOperationException("Column can not be converted into a size: column=" + columnName,
                    this);
        }

        return this;
    }

    @Override
    public AbstractXYDataSeries pointSize(final SelectableDataSet sds, final String columnName) {
        ArgumentValidations.assertNotNull(sds, "sds", getPlotInfo());
        ArgumentValidations.assertNotNull(columnName, "columnName", getPlotInfo());
        ArgumentValidations.assertColumnsInTable(sds, getPlotInfo(), columnName);
        ArgumentValidations.assertIsNumericOrTime(sds, columnName, getPlotInfo());

        final SwappableTable t = sds.getSwappableTable(name(), chart(), columnName);
        addSwappableTable(t);
        shapeSizes.setSpecific(new IndexableDataSwappableTableDouble(t, columnName, getPlotInfo()), true);

        return this;
    }


    ////////////////////////// point colors //////////////////////////


    @Override
    public AbstractXYDataSeries pointColor(final Paint color) {
        shapeColors.setDefault(color);
        return this;
    }

    @Override
    public AbstractXYDataSeries pointColor(final int color) {
        return pointColor(intToColor(chart(), color));
    }

    @Override
    public AbstractXYDataSeries pointColor(final String color) {
        return pointColor(Color.color(color));
    }

    @Override
    public <T extends Paint> AbstractXYDataSeries pointColor(final IndexableData<T> colors) {
        shapeColors.setSpecific(colors, true);
        return this;
    }


    @Override
    public AbstractXYDataSeries pointColor(final Paint... colors) {
        return pointColor(new IndexableDataArray<>(colors, getPlotInfo()));
    }

    @Override
    public AbstractXYDataSeries pointColorInteger(final IndexableData<Integer> colors) {
        return pointColor(new IndexableDataPaintInteger(colors, chart()));
    }

    @Override
    public AbstractXYDataSeries pointColor(final int... colors) {
        return pointColorInteger(new IndexableDataInteger(colors, getPlotInfo()));
    }

    @Override
    public AbstractXYDataSeries pointColor(final Integer... colors) {
        return pointColorInteger(new IndexableDataArray<>(colors, getPlotInfo()));
    }

    @Override
    public AbstractXYDataSeries pointColor(final String... colors) {
        return pointColor(Arrays.stream(colors).map(Color::color).toArray(Color[]::new));
    }

    @Override
    public AbstractXYDataSeries pointColor(final Table t, final String columnName) {
        ArgumentValidations.assertNotNull(t, "table", getPlotInfo());

        final TableHandle tableHandle = new TableHandle(t, columnName);
        addTableHandle(tableHandle);
        final ColumnHandlerFactory.ColumnHandler columnHandler =
                ColumnHandlerFactory.newNumericHandler(tableHandle, columnName, getPlotInfo());

        if (columnHandler.typeClassification() == ColumnHandlerFactory.TypeClassification.INTEGER &&
                (columnHandler.type() == int.class || columnHandler.type() == Integer.class)) {
            return pointColor(new IndexableDataTablePaint(columnHandler, chart()));
        } else if (columnHandler.typeClassification() == ColumnHandlerFactory.TypeClassification.PAINT) {
            return pointColor(new IndexableDataTable<>(columnHandler, getPlotInfo()));
        } else {
            throw new PlotUnsupportedOperationException(
                    "Column can not be converted into a color: column=" + columnName + "\ttype=" + columnHandler.type(),
                    this);
        }
    }

    @Override
    public AbstractXYDataSeries pointColor(final SelectableDataSet sds, final String columnName) {
        ArgumentValidations.assertColumnsInTable(sds, getPlotInfo(), columnName);
        final Class type = sds.getTableDefinition().getColumn(columnName).getDataType();
        final boolean isInt = type.equals(int.class) || type.equals(Integer.class) || type.equals(short.class)
                || type.equals(Short.class);
        final boolean isPaint = Paint.class.isAssignableFrom(type);

        if (!isInt && !isPaint) {
            throw new PlotUnsupportedOperationException(
                    "Column can not be converted into a color: column=" + columnName + "\ttype=" + type, this);
        }

        final SwappableTable t = sds.getSwappableTable(name(), chart(), columnName);
        addSwappableTable(t);

        if (isInt) {
            return pointColor(new IndexableDataSwappableTablePaint(t, columnName, chart()));
        } else if (isPaint) {
            return pointColor(new IndexableDataSwappableTable<Paint>(t, columnName, getPlotInfo()));
        } else {
            throw new PlotIllegalStateException("Should never reach here", this);
        }
    }

    @Override
    public <T extends Paint> AbstractXYDataSeries pointColorByY(Function<Double, T> colors) {
        ArgumentValidations.assertNotNull(colors, "colors", getPlotInfo());
        final Paint[] paints = new Paint[size()];

        for (int i = 0; i < size(); i++) {
            paints[i] = colors.apply(getY(i));
        }

        return pointColor(paints);
    }

    @Override
    public <T extends Paint> AbstractXYDataSeries pointColorByY(final Closure<T> colors) {
        return pointColorByY(new ClosureFunction<>(colors));
    }

    ////////////////////////// point labels //////////////////////////


    @Override
    public AbstractXYDataSeries pointLabel(final Object label) {
        shapeLabels.setDefault(label == null ? null : label.toString());
        return this;
    }

    @Override
    public AbstractXYDataSeries pointLabel(final IndexableData<?> labels) {
        shapeLabels.setSpecific(new IndexableDataString<>(labels), true);

        return this;
    }

    @Override
    public AbstractXYDataSeries pointLabel(final Object... labels) {
        return pointLabel(new IndexableDataArray<>(labels, getPlotInfo()));
    }

    @Override
    public AbstractXYDataSeries pointLabel(@NotNull Table t, @NotNull String columnName) {
        final TableHandle tableHandle = new TableHandle(t, columnName);
        addTableHandle(tableHandle);
        final ColumnHandlerFactory.ColumnHandler columnHandler =
                ColumnHandlerFactory.newObjectHandler(tableHandle, columnName, getPlotInfo());

        return pointLabel(new IndexableDataTableString(columnHandler, getPlotInfo()));
    }

    @Override
    public AbstractXYDataSeries pointLabel(@NotNull SelectableDataSet sds, @NotNull String columnName) {
        ArgumentValidations.assertColumnsInTable(sds, getPlotInfo(), columnName);

        final SwappableTable t = sds.getSwappableTable(name(), chart(), columnName);
        addSwappableTable(t);
        return pointLabel(new IndexableDataSwappableTableString(t, columnName, getPlotInfo()));
    }


    //////////////////// point shapes //////////////////////////


    @Override
    public DataSeries pointShape(final String shape) {
        return pointShape(NamedShape.getShape(shape));
    }

    @Override
    public DataSeries pointShape(final Shape shape) {
        pointShapes.setDefault(shape);
        return this;
    }

    @Override
    public XYDataSeries pointShape(final IndexableData<String> shapes) {
        ArgumentValidations.assertNotNull(shapes, "shapes", getPlotInfo());
        pointShapes.setSpecific(new IndexableDataPointShapeString(shapes), true);

        return this;
    }

    @Override
    public XYDataSeries pointShape(final String... shapes) {
        ArgumentValidations.assertNotNull(shapes, "shapes", getPlotInfo());
        // sanity check shapes
        int index = 0;
        for (final String shape : shapes) {
            try {
                NamedShape.getShape(shape);
            } catch (final IllegalArgumentException iae) {
                throw new PlotIllegalArgumentException("Not a valid shape: `" + shape + "` at index: " + index
                        + "; valid shapes: " + Arrays.toString(NamedShape.values()), this);
            }
            ++index;
        }

        return pointShape(new IndexableDataArray<>(shapes, getPlotInfo()));
    }

    @Override
    public XYDataSeries pointShape(final Shape... shapes) {
        ArgumentValidations.assertNotNull(shapes, "shapes", getPlotInfo());
        pointShapes.setSpecific(new IndexableDataPointShapeObject(new IndexableDataArray<>(shapes, getPlotInfo())),
                true);
        return this;
    }

    @Override
    public XYDataSeries pointShape(final Table t, final String columnName) {
        ArgumentValidations.assertNotNull(t, "t", getPlotInfo());
        ArgumentValidations.assertNotNull(columnName, "columnName", getPlotInfo());

        final Class columnType = ArgumentValidations.getColumnType(t, columnName, getPlotInfo());
        if (String.class.isAssignableFrom(columnType)) {
            final TableHandle tableHandle = new TableHandle(t, columnName);
            addTableHandle(tableHandle);
            final ColumnHandlerFactory.ColumnHandler columnHandler =
                    ColumnHandlerFactory.newObjectHandler(tableHandle, columnName, getPlotInfo());
            return pointShape(new IndexableDataTableString<>(columnHandler, getPlotInfo()));
        } else if (Shape.class.isAssignableFrom(columnType)) {
            final TableHandle tableHandle = new TableHandle(t, columnName);
            addTableHandle(tableHandle);
            final ColumnHandlerFactory.ColumnHandler columnHandler =
                    ColumnHandlerFactory.newObjectHandler(tableHandle, columnName, getPlotInfo());
            pointShapes.setSpecific(new IndexableDataTablePointShapeObject(columnHandler, getPlotInfo()), true);
            return this;
        } else {
            throw new PlotRuntimeException("column is not a supported type (String or Shape): columnName=" + columnName,
                    this);
        }
    }

    @Override
    public XYDataSeries pointShape(final SelectableDataSet sds, final String columnName) {
        ArgumentValidations.assertNotNull(sds, "sds", getPlotInfo());
        ArgumentValidations.assertNotNull(columnName, "columnName", getPlotInfo());
        final Class columnType = ArgumentValidations.getColumnType(sds, columnName, getPlotInfo());

        if (String.class.isAssignableFrom(columnType)) {
            final SwappableTable t = sds.getSwappableTable(name(), chart(), columnName);
            addSwappableTable(t);
            return pointShape(new IndexableDataSwappableTableString<>(t, columnName, getPlotInfo()));
        } else if (Shape.class.isAssignableFrom(columnType)) {
            final SwappableTable t = sds.getSwappableTable(name(), chart(), columnName);
            addSwappableTable(t);
            pointShapes.setSpecific(new IndexableDataSwappableTablePointShapeObject(t, columnName, getPlotInfo()),
                    true);
            return this;
        } else {
            throw new PlotRuntimeException("column is not a supported type (String or Shape): columnName=" + columnName,
                    this);
        }
    }

    @Override
    public AbstractXYDataSeries pointLabelFormat(final String format) {
        setPointLabelFormat(format);
        return this;
    }

    @Override
    public AbstractXYDataSeries xToolTipPattern(final String format) {
        setXToolTipPattern(format);
        return this;
    }

    @Override
    public AbstractXYDataSeries yToolTipPattern(final String format) {
        setYToolTipPattern(format);
        return this;
    }

    private static class IndexableDataString<T> extends IndexableData<String> implements Serializable {

        private static final long serialVersionUID = 4764967316583190069L;
        private IndexableData<T> labels;

        IndexableDataString(IndexableData<T> labels) {
            super(labels.getPlotInfo());
            this.labels = labels;
        }

        @Override
        public int size() {
            return labels.size();
        }

        @Override
        public String get(int i) {
            final T v = labels.get(i);
            return v == null ? null : v.toString();
        }
    }

    private static class IndexableDataTableString<T> extends IndexableDataTable<String> implements Serializable {

        private static final long serialVersionUID = 5039901915605865720L;
        private IndexableData<T> labels;

        IndexableDataTableString(ColumnHandlerFactory.ColumnHandler columnHandler, final PlotInfo plotInfo) {
            super(columnHandler, plotInfo);
            this.labels = labels;
        }

        @Override
        public String convert(Object v) {
            return v == null ? null : v.toString();
        }
    }

    private static class IndexableDataTablePointShapeObject extends IndexableDataTable<Shape> implements Serializable {

        private static final long serialVersionUID = -3933148551484191243L;

        IndexableDataTablePointShapeObject(ColumnHandlerFactory.ColumnHandler columnHandler, final PlotInfo plotInfo) {
            super(columnHandler, plotInfo);
        }

        @Override
        public Shape convert(Object v) {
            return v == null ? null : ((Shape) v);
        }
    }

    private static class IndexableDataSwappableTablePointShapeObject extends IndexableDataSwappableTable<Shape>
            implements Serializable {

        private static final long serialVersionUID = 5642048699432187943L;

        IndexableDataSwappableTablePointShapeObject(final SwappableTable swappableTable, final String column,
                PlotInfo plotInfo) {
            super(swappableTable, column, plotInfo);
        }

        @Override
        public Shape convert(Object v) {
            return v == null ? null : ((Shape) v);
        }
    }

    private static class IndexableDataSwappableTableString<T> extends IndexableDataSwappableTable<String>
            implements Serializable {

        private static final long serialVersionUID = 5039901915605865720L;

        IndexableDataSwappableTableString(final SwappableTable t, final String column, final PlotInfo plotInfo) {
            super(t, column, plotInfo);
        }

        @Override
        public String convert(Object v) {
            return v == null ? null : v.toString();
        }
    }

    private static class IndexableDataSwappableTablePaint extends IndexableDataSwappableTable<Paint>
            implements Serializable {
        private static final long serialVersionUID = 1809364632850960872L;
        private final ChartImpl chart;

        IndexableDataSwappableTablePaint(final SwappableTable t, final String column, final ChartImpl chart) {
            super(t, column, chart.getPlotInfo());
            this.chart = chart;
        }

        @Override
        public Paint convert(final Object v) {
            return intToColor(chart, (Integer) v);
        }
    }

    private static class IndexableDataPaintInteger extends IndexableData<Paint> implements Serializable {
        private static final long serialVersionUID = 6258408120262796309L;
        private final IndexableData<Integer> colors;
        private final ChartImpl chart;

        IndexableDataPaintInteger(IndexableData<Integer> colors, ChartImpl chart) {
            super(chart.getPlotInfo());
            this.colors = colors;
            this.chart = chart;
        }

        @Override
        public int size() {
            return colors.size();
        }

        @Override
        public Paint get(int i) {
            return intToColor(chart, colors.get(i));
        }
    }

    private static class IndexableDataTablePaint extends IndexableDataTable<Paint> implements Serializable {
        private static final long serialVersionUID = 1809364632850960872L;
        private final ChartImpl chart;

        IndexableDataTablePaint(ColumnHandlerFactory.ColumnHandler columnHandler, ChartImpl chart) {
            super(columnHandler, chart.getPlotInfo());
            this.chart = chart;
        }

        @Override
        public Paint convert(final Object v) {
            return intToColor(chart, (Integer) v);
        }
    }

    private static class IndexableDataPointShapeString extends IndexableData<Shape> {
        private static final long serialVersionUID = 2933177669306019523L;
        private final IndexableData<String> shapes;

        public IndexableDataPointShapeString(final IndexableData<String> shapes) {
            super(shapes.getPlotInfo());
            this.shapes = shapes;
        }

        @Override
        public int size() {
            return shapes.size();
        }

        @Override
        public Shape get(final int index) {
            return NamedShape.getShape(shapes.get(index));
        }
    }

    private static class IndexableDataPointShapeObject extends IndexableData<Shape> {
        private static final long serialVersionUID = 975960184959064132L;
        private final IndexableData<Shape> shapes;

        public IndexableDataPointShapeObject(final IndexableData<Shape> shapes) {
            super(shapes.getPlotInfo());
            this.shapes = shapes;
        }

        @Override
        public int size() {
            return shapes.size();
        }

        @Override
        public Shape get(final int index) {
            return shapes.get(index);
        }
    }

    ////////////////////// tool tips /////////////////////////////


    @Override
    public AbstractXYDataSeries toolTipPattern(final String format) {
        xToolTipPattern(format);
        yToolTipPattern(format);
        return this;
    }

}
