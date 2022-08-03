/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.datasets.xy;


import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.ChartImpl;
import io.deephaven.plot.LineStyle;
import io.deephaven.plot.datasets.AbstractDataSeries;
import io.deephaven.plot.datasets.DataSeries;
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
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Function;

import static io.deephaven.plot.util.PlotUtils.intToColor;
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
    public AbstractXYDataSeries gradientVisible(boolean gradientVisible) {
        setGradientVisible(gradientVisible);
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
    public AbstractXYDataSeries errorBarColor(final Paint errorBarColor) {
        setErrorBarColor(errorBarColor);
        return this;
    }

    @Override
    public AbstractXYDataSeries errorBarColor(final int errorBarColor) {
        setErrorBarColor(errorBarColor);
        return this;
    }

    @Override
    public AbstractXYDataSeries errorBarColor(final String errorBarColor) {
        setErrorBarColor(errorBarColor);
        return this;
    }


    ////////////////////////// line style //////////////////////////


    @Override
    public AbstractXYDataSeries lineStyle(final LineStyle lineStyle) {
        setLineStyle(lineStyle);
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
    public XYDataSeriesInternal pointSize(final int pointSize) {
        return pointSize(pointSize == NULL_INT ? null : pointSize);
    }

    @Override
    public XYDataSeriesInternal pointSize(final long pointSize) {
        return pointSize(pointSize == NULL_LONG ? null : pointSize);
    }

    @Override
    public XYDataSeriesInternal pointSize(final double pointSize) {
        shapeSizes.setDefault(pointSize == NULL_DOUBLE || pointSize == Double.NaN ? null : pointSize);
        return this;
    }

    @Override
    public XYDataSeriesInternal pointSize(final Number pointSize) {
        shapeSizes.setDefault(pointSize == null ? null : pointSize.doubleValue());
        return this;
    }

    @Override
    public AbstractXYDataSeries pointSize(final IndexableData<Double> pointSizes) {
        ArgumentValidations.assertNotNull(pointSizes, "factors", getPlotInfo());
        shapeSizes.setSpecific(pointSizes, true);
        return this;
    }

    @Override
    public AbstractXYDataSeries pointSize(final int... pointSizes) {
        return pointSize(new IndexableDataDouble(pointSizes, true, getPlotInfo()));
    }

    @Override
    public AbstractXYDataSeries pointSize(final long... pointSizes) {
        return pointSize(new IndexableDataDouble(pointSizes, true, getPlotInfo()));
    }

    @Override
    public AbstractXYDataSeries pointSize(final double... pointSizes) {
        return pointSize(new IndexableDataDouble(pointSizes, true, getPlotInfo()));
    }

    @Override
    public <T extends Number> AbstractXYDataSeries pointSize(final T[] pointSizes) {
        return pointSize(new IndexableDataDouble(pointSizes, true, getPlotInfo()));
    }

    @Override
    public AbstractXYDataSeries pointSize(final Table t, final String pointSizes) {
        ArgumentValidations.assertNotNull(t, "table", getPlotInfo());
        ArgumentValidations.assertNotNull(pointSizes, "columnName", getPlotInfo());

        final TableHandle tableHandle = new TableHandle(t, pointSizes);
        addTableHandle(tableHandle);
        final ColumnHandlerFactory.ColumnHandler columnHandler =
                ColumnHandlerFactory.newNumericHandler(tableHandle, pointSizes, getPlotInfo());

        if (columnHandler.typeClassification().isNumeric()) {
            shapeSizes.setSpecific(new IndexableDataTable<Double>(columnHandler, getPlotInfo()) {
                @Override
                public Double convert(Object v) {
                    return PlotUtils.numberToDouble((Number) v);
                }
            }, true);
        } else {
            throw new PlotUnsupportedOperationException("Column can not be converted into a size: column=" + pointSizes,
                    this);
        }

        return this;
    }

    @Override
    public AbstractXYDataSeries pointSize(final SelectableDataSet sds, final String pointSize) {
        ArgumentValidations.assertNotNull(sds, "sds", getPlotInfo());
        ArgumentValidations.assertNotNull(pointSize, "columnName", getPlotInfo());
        ArgumentValidations.assertColumnsInTable(sds, getPlotInfo(), pointSize);
        ArgumentValidations.assertIsNumericOrTime(sds, pointSize, getPlotInfo());

        final SwappableTable t = sds.getSwappableTable(name(), chart(), pointSize);
        addSwappableTable(t);
        shapeSizes.setSpecific(new IndexableDataSwappableTableDouble(t, pointSize, getPlotInfo()), true);

        return this;
    }


    ////////////////////////// point colors //////////////////////////


    @Override
    public AbstractXYDataSeries pointColor(final Paint pointColor) {
        shapeColors.setDefault(pointColor);
        return this;
    }

    @Override
    public AbstractXYDataSeries pointColor(final int pointColor) {
        return pointColor(intToColor(chart(), pointColor));
    }

    @Override
    public AbstractXYDataSeries pointColor(final String pointColor) {
        return pointColor(Color.color(pointColor));
    }

    @Override
    public <T extends Paint> AbstractXYDataSeries pointColor(final IndexableData<T> pointColor) {
        shapeColors.setSpecific(pointColor, true);
        return this;
    }


    @Override
    public AbstractXYDataSeries pointColor(final Paint... pointColor) {
        return pointColor(new IndexableDataArray<>(pointColor, getPlotInfo()));
    }

    @Override
    public AbstractXYDataSeries pointColorInteger(final IndexableData<Integer> colors) {
        return pointColor(new IndexableDataPaintInteger(colors, chart()));
    }

    @Override
    public AbstractXYDataSeries pointColor(final int... pointColors) {
        return pointColorInteger(new IndexableDataInteger(pointColors, getPlotInfo()));
    }

    @Override
    public AbstractXYDataSeries pointColor(final Integer... pointColors) {
        return pointColorInteger(new IndexableDataArray<>(pointColors, getPlotInfo()));
    }

    @Override
    public AbstractXYDataSeries pointColor(final String... pointColors) {
        return pointColor(Arrays.stream(pointColors).map(Color::color).toArray(Color[]::new));
    }

    @Override
    public AbstractXYDataSeries pointColor(final Table t, final String pointColors) {
        ArgumentValidations.assertNotNull(t, "table", getPlotInfo());

        final TableHandle tableHandle = new TableHandle(t, pointColors);
        addTableHandle(tableHandle);
        final ColumnHandlerFactory.ColumnHandler columnHandler =
                ColumnHandlerFactory.newNumericHandler(tableHandle, pointColors, getPlotInfo());

        if (columnHandler.typeClassification() == ColumnHandlerFactory.TypeClassification.INTEGER &&
                (columnHandler.type() == int.class || columnHandler.type() == Integer.class)) {
            return pointColor(new IndexableDataTablePaint(columnHandler, chart()));
        } else if (columnHandler.typeClassification() == ColumnHandlerFactory.TypeClassification.PAINT) {
            return pointColor(new IndexableDataTable<>(columnHandler, getPlotInfo()));
        } else {
            throw new PlotUnsupportedOperationException(
                    "Column can not be converted into a color: column=" + pointColors + "\ttype="
                            + columnHandler.type(),
                    this);
        }
    }

    @Override
    public AbstractXYDataSeries pointColor(final SelectableDataSet sds, final String pointColors) {
        ArgumentValidations.assertColumnsInTable(sds, getPlotInfo(), pointColors);
        final Class type = sds.getTableDefinition().getColumn(pointColors).getDataType();
        final boolean isInt = type.equals(int.class) || type.equals(Integer.class) || type.equals(short.class)
                || type.equals(Short.class);
        final boolean isPaint = Paint.class.isAssignableFrom(type);

        if (!isInt && !isPaint) {
            throw new PlotUnsupportedOperationException(
                    "Column can not be converted into a color: column=" + pointColors + "\ttype=" + type, this);
        }

        final SwappableTable t = sds.getSwappableTable(name(), chart(), pointColors);
        addSwappableTable(t);

        if (isInt) {
            return pointColor(new IndexableDataSwappableTablePaint(t, pointColors, chart()));
        } else if (isPaint) {
            return pointColor(new IndexableDataSwappableTable<Paint>(t, pointColors, getPlotInfo()));
        } else {
            throw new PlotIllegalStateException("Should never reach here", this);
        }
    }


    ////////////////////////// point labels //////////////////////////


    @Override
    public AbstractXYDataSeries pointLabel(final Object pointLabel) {
        shapeLabels.setDefault(pointLabel == null ? null : pointLabel.toString());
        return this;
    }

    @Override
    public AbstractXYDataSeries pointLabel(final IndexableData<?> pointLabels) {
        shapeLabels.setSpecific(new IndexableDataString<>(pointLabels), true);

        return this;
    }

    @Override
    public AbstractXYDataSeries pointLabel(final Object... pointLabels) {
        return pointLabel(new IndexableDataArray<>(pointLabels, getPlotInfo()));
    }

    @Override
    public AbstractXYDataSeries pointLabel(@NotNull Table t, @NotNull String pointLabel) {
        final TableHandle tableHandle = new TableHandle(t, pointLabel);
        addTableHandle(tableHandle);
        final ColumnHandlerFactory.ColumnHandler columnHandler =
                ColumnHandlerFactory.newObjectHandler(tableHandle, pointLabel, getPlotInfo());

        return pointLabel(new IndexableDataTableString(columnHandler, getPlotInfo()));
    }

    @Override
    public AbstractXYDataSeries pointLabel(@NotNull SelectableDataSet sds, @NotNull String pointLabel) {
        ArgumentValidations.assertColumnsInTable(sds, getPlotInfo(), pointLabel);

        final SwappableTable t = sds.getSwappableTable(name(), chart(), pointLabel);
        addSwappableTable(t);
        return pointLabel(new IndexableDataSwappableTableString(t, pointLabel, getPlotInfo()));
    }


    //////////////////// point shapes //////////////////////////


    @Override
    public DataSeries pointShape(final String pointShape) {
        return pointShape(NamedShape.getShape(pointShape));
    }

    @Override
    public DataSeries pointShape(final Shape pointShape) {
        pointShapes.setDefault(pointShape);
        return this;
    }

    @Override
    public XYDataSeries pointShape(final IndexableData<String> pointShapes) {
        ArgumentValidations.assertNotNull(pointShapes, "shapes", getPlotInfo());
        this.pointShapes.setSpecific(new IndexableDataPointShapeString(pointShapes), true);

        return this;
    }

    @Override
    public XYDataSeries pointShape(final String... pointShapes) {
        ArgumentValidations.assertNotNull(pointShapes, "shapes", getPlotInfo());
        // sanity check shapes
        int index = 0;
        for (final String shape : pointShapes) {
            try {
                NamedShape.getShape(shape);
            } catch (final IllegalArgumentException iae) {
                throw new PlotIllegalArgumentException("Not a valid shape: `" + shape + "` at index: " + index
                        + "; valid shapes: " + Arrays.toString(NamedShape.values()), this);
            }
            ++index;
        }

        return pointShape(new IndexableDataArray<>(pointShapes, getPlotInfo()));
    }

    @Override
    public XYDataSeries pointShape(final Shape... pointShapes) {
        ArgumentValidations.assertNotNull(pointShapes, "shapes", getPlotInfo());
        this.pointShapes.setSpecific(
                new IndexableDataPointShapeObject(new IndexableDataArray<>(pointShapes, getPlotInfo())),
                true);
        return this;
    }

    @Override
    public XYDataSeries pointShape(final Table t, final String pointShape) {
        ArgumentValidations.assertNotNull(t, "t", getPlotInfo());
        ArgumentValidations.assertNotNull(pointShape, "columnName", getPlotInfo());

        final Class columnType = ArgumentValidations.getColumnType(t, pointShape, getPlotInfo());
        if (String.class.isAssignableFrom(columnType)) {
            final TableHandle tableHandle = new TableHandle(t, pointShape);
            addTableHandle(tableHandle);
            final ColumnHandlerFactory.ColumnHandler columnHandler =
                    ColumnHandlerFactory.newObjectHandler(tableHandle, pointShape, getPlotInfo());
            return pointShape(new IndexableDataTableString<>(columnHandler, getPlotInfo()));
        } else if (Shape.class.isAssignableFrom(columnType)) {
            final TableHandle tableHandle = new TableHandle(t, pointShape);
            addTableHandle(tableHandle);
            final ColumnHandlerFactory.ColumnHandler columnHandler =
                    ColumnHandlerFactory.newObjectHandler(tableHandle, pointShape, getPlotInfo());
            pointShapes.setSpecific(new IndexableDataTablePointShapeObject(columnHandler, getPlotInfo()), true);
            return this;
        } else {
            throw new PlotRuntimeException("column is not a supported type (String or Shape): columnName=" + pointShape,
                    this);
        }
    }

    @Override
    public XYDataSeries pointShape(final SelectableDataSet sds, final String pointShape) {
        ArgumentValidations.assertNotNull(sds, "sds", getPlotInfo());
        ArgumentValidations.assertNotNull(pointShape, "columnName", getPlotInfo());
        final Class columnType = ArgumentValidations.getColumnType(sds, pointShape, getPlotInfo());

        if (String.class.isAssignableFrom(columnType)) {
            final SwappableTable t = sds.getSwappableTable(name(), chart(), pointShape);
            addSwappableTable(t);
            return pointShape(new IndexableDataSwappableTableString<>(t, pointShape, getPlotInfo()));
        } else if (Shape.class.isAssignableFrom(columnType)) {
            final SwappableTable t = sds.getSwappableTable(name(), chart(), pointShape);
            addSwappableTable(t);
            pointShapes.setSpecific(new IndexableDataSwappableTablePointShapeObject(t, pointShape, getPlotInfo()),
                    true);
            return this;
        } else {
            throw new PlotRuntimeException("column is not a supported type (String or Shape): columnName=" + pointShape,
                    this);
        }
    }

    @Override
    public AbstractXYDataSeries pointLabelFormat(final String pointLabelFormat) {
        setPointLabelFormat(pointLabelFormat);
        return this;
    }

    @Override
    public AbstractXYDataSeries xToolTipPattern(final String xToolTipPattern) {
        setXToolTipPattern(xToolTipPattern);
        return this;
    }

    @Override
    public AbstractXYDataSeries yToolTipPattern(final String yToolTipPattern) {
        setYToolTipPattern(yToolTipPattern);
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
    public AbstractXYDataSeries toolTipPattern(final String toolTipPattern) {
        xToolTipPattern(toolTipPattern);
        yToolTipPattern(toolTipPattern);
        return this;
    }

}
