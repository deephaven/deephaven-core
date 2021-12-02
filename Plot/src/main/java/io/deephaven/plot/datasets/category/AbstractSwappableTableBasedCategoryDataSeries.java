/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.category;

import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.datasets.ColumnNameConstants;
import io.deephaven.plot.datasets.data.AssociativeDataSwappableTable;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.plot.util.functions.FigureImplFunction;
import io.deephaven.plot.util.tables.SwappableTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.lang.QueryLibrary;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.gui.color.Paint;
import io.deephaven.gui.shape.NamedShape;
import io.deephaven.gui.shape.Shape;

import java.util.Objects;
import java.util.function.Function;

import static io.deephaven.plot.util.PlotUtils.intToColor;

public abstract class AbstractSwappableTableBasedCategoryDataSeries extends AbstractCategoryDataSeries {

    public AbstractSwappableTableBasedCategoryDataSeries(AxesImpl axes, int id, Comparable name) {
        super(axes, id, name);
    }

    public AbstractSwappableTableBasedCategoryDataSeries(final AxesImpl axes, final int id, final Comparable name,
            final AbstractCategoryDataSeries series) {
        super(axes, id, name, series);
    }

    public AbstractSwappableTableBasedCategoryDataSeries(final AbstractCategoryDataSeries series, final AxesImpl axes) {
        super(series, axes);
    }

    @Override
    public CategoryDataSeries pointShape(final Function<Comparable, String> shapes) {
        final String colName = ColumnNameConstants.POINT_SHAPE + this.hashCode();
        chart().figure().registerTableMapFunction(getSwappableTable().getTableMapHandle(),
                constructTableMapFromFunctionOnCategoryCol(shapes, String.class, colName));
        getSwappableTable().getTableMapHandle().addColumn(colName);
        chart().figure().registerFigureFunction(new FigureImplFunction(figImpl -> {
            ((AbstractSwappableTableBasedCategoryDataSeries) figImpl.getFigure().getCharts()
                    .getChart(chart().row(), chart().column()).axes(axes().id()).series(id()))
                            .shapesSetSpecific(
                                    new AssociativeDataSwappableTable<Comparable, Shape, String>(getSwappableTable(),
                                            getCategoryCol(), colName, Comparable.class, String.class, getPlotInfo()) {
                                        @Override
                                        public Shape convert(String v) {
                                            return NamedShape.getShape(v);
                                        }
                                    });
            return figImpl;
        }, this));
        return this;
    }

    @Override
    public <NUMBER extends Number> CategoryDataSeries pointSize(final Function<Comparable, NUMBER> factors) {
        final String colName = ColumnNameConstants.POINT_SIZE + this.hashCode();
        chart().figure().registerTableMapFunction(getSwappableTable().getTableMapHandle(),
                constructTableMapFromFunctionOnCategoryCol(factors, Number.class, colName));
        getSwappableTable().getTableMapHandle().addColumn(colName);
        chart().figure().registerFigureFunction(new FigureImplFunction(figImpl -> {
            ((AbstractSwappableTableBasedCategoryDataSeries) figImpl.getFigure().getCharts()
                    .getChart(chart().row(), chart().column()).axes(axes().id()).series(id()))
                            .sizesSetSpecific(new AssociativeDataSwappableTable<>(getSwappableTable(), getCategoryCol(),
                                    colName, Comparable.class, Number.class, getPlotInfo()));
            return figImpl;
        }, this));
        return this;
    }

    @Override
    public <COLOR extends Paint> CategoryDataSeries pointColor(final Function<Comparable, COLOR> colors) {
        final String colName = ColumnNameConstants.POINT_COLOR + this.hashCode();
        chart().figure().registerTableMapFunction(getSwappableTable().getTableMapHandle(),
                constructTableMapFromFunctionOnCategoryCol(colors, Paint.class, colName));
        getSwappableTable().getTableMapHandle().addColumn(colName);
        chart().figure().registerFigureFunction(new FigureImplFunction(figImpl -> {
            ((AbstractSwappableTableBasedCategoryDataSeries) figImpl.getFigure().getCharts()
                    .getChart(chart().row(), chart().column()).axes(axes().id()).series(id()))
                            .colorsSetSpecific(new AssociativeDataSwappableTable<>(getSwappableTable(),
                                    getCategoryCol(), colName, Comparable.class, Paint.class, getPlotInfo()));
            return figImpl;
        }, this));
        return this;
    }

    @Override
    public <COLOR extends Integer> CategoryDataSeries pointColorInteger(final Function<Comparable, COLOR> colors) {
        final String colName = ColumnNameConstants.POINT_COLOR + this.hashCode();
        chart().figure().registerTableMapFunction(getSwappableTable().getTableMapHandle(),
                constructTableMapFromFunctionOnCategoryCol(colors, Integer.class, colName));
        getSwappableTable().getTableMapHandle().addColumn(colName);
        chart().figure().registerFigureFunction(new FigureImplFunction(figImpl -> {
            ((AbstractSwappableTableBasedCategoryDataSeries) figImpl.getFigure().getCharts()
                    .getChart(chart().row(), chart().column()).axes(axes().id()).series(id()))
                            .colorsSetSpecific(
                                    new AssociativeDataSwappableTable<Comparable, Paint, Integer>(getSwappableTable(),
                                            getCategoryCol(), colName, Comparable.class, Integer.class, getPlotInfo()) {
                                        @Override
                                        public Paint convert(Integer v) {
                                            return intToColor(chart(), v);
                                        }
                                    });
            return figImpl;
        }, this));
        return this;
    }

    @Override
    public <T extends Paint> CategoryDataSeries pointColorByY(Function<Double, T> colors) {
        final String colName = ColumnNameConstants.POINT_COLOR + this.hashCode();
        chart().figure().registerTableMapFunction(getSwappableTable().getTableMapHandle(),
                constructTableMapFromFunctionOnCategoryCol(colors, Paint.class, colName));
        getSwappableTable().getTableMapHandle().addColumn(colName);
        chart().figure().registerFigureFunction(new FigureImplFunction(figImpl -> {
            ((AbstractSwappableTableBasedCategoryDataSeries) figImpl.getFigure().getCharts()
                    .getChart(chart().row(), chart().column()).axes(axes().id()).series(id()))
                            .colorsSetSpecific(new AssociativeDataSwappableTable<>(getSwappableTable(),
                                    getCategoryCol(), colName, Comparable.class, Paint.class, getPlotInfo()));
            return figImpl;
        }, this));
        return this;
    }

    @Override
    public <LABEL> CategoryDataSeries pointLabel(final Function<Comparable, LABEL> labels) {
        final String colName = ColumnNameConstants.POINT_LABEL + this.hashCode();
        chart().figure().registerTableMapFunction(getSwappableTable().getTableMapHandle(),
                constructTableMapFromFunctionOnCategoryCol(labels, Object.class, colName));
        getSwappableTable().getTableMapHandle().addColumn(colName);
        chart().figure().registerFigureFunction(new FigureImplFunction(figImpl -> {
            ((AbstractSwappableTableBasedCategoryDataSeries) figImpl.getFigure().getCharts()
                    .getChart(chart().row(), chart().column()).axes(axes().id()).series(id()))
                            .labelsSetSpecific(
                                    new AssociativeDataSwappableTable<Comparable, String, Object>(getSwappableTable(),
                                            getCategoryCol(), colName, Comparable.class, Object.class, getPlotInfo()) {
                                        @Override
                                        public String convert(final Object o) {
                                            return Objects.toString(o);
                                        }
                                    });
            return figImpl;
        }, this));
        return this;
    }

    private <S, T> Function<Table, Table> constructTableMapFromFunctionOnCategoryCol(final Function<S, T> function,
            final Class resultClass, final String columnName) {
        return constructTableMapFromFunction(function, resultClass, getCategoryCol(), columnName);
    }

    protected <S, T> Function<Table, Table> constructTableMapFromFunction(final Function<S, T> function,
            final Class resultClass, final String onColumn, final String columnName) {
        ArgumentValidations.assertNotNull(function, "function", getPlotInfo());
        final String queryFunction = columnName + "Function";
        return t -> {
            QueryScope.addParam(queryFunction, function);
            QueryLibrary.importClass(resultClass);
            return t.update(columnName + " = (" + resultClass.getSimpleName() + ") " + queryFunction + ".apply("
                    + onColumn + ")");
        };
    }

    protected abstract SwappableTable getSwappableTable();

    protected abstract String getCategoryCol();

    protected abstract String getNumericCol();
}
