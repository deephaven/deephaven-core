/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.category;

import io.deephaven.db.plot.AxesImpl;
import io.deephaven.db.plot.datasets.ColumnNameConstants;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.plot.util.functions.FigureImplFunction;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.libs.QueryLibrary;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.gui.color.Paint;

import java.util.function.Function;

public abstract class AbstractTableBasedCategoryDataSeries extends AbstractCategoryDataSeries {

    public AbstractTableBasedCategoryDataSeries(AxesImpl axes, int id, Comparable name) {
        super(axes, id, name);
    }

    public AbstractTableBasedCategoryDataSeries(final AxesImpl axes, final int id,
        final Comparable name, final AbstractCategoryDataSeries series) {
        super(axes, id, name, series);
    }

    public AbstractTableBasedCategoryDataSeries(final AbstractCategoryDataSeries series,
        final AxesImpl axes) {
        super(series, axes);
    }

    @Override
    public CategoryDataSeries pointShape(final Function<Comparable, String> shapes) {
        final String colName = ColumnNameConstants.POINT_SHAPE + this.hashCode();
        chart().figure().registerTableFunction(getTable(),
            t -> constructTableFromFunctionOnCategoryCol(t, shapes, String.class, colName));
        chart().figure().registerFigureFunction(
            new FigureImplFunction(f -> f.pointShape(getTable(), getCategoryCol(), colName), this));
        return this;
    }

    @Override
    public <NUMBER extends Number> CategoryDataSeries pointSize(
        final Function<Comparable, NUMBER> factors) {
        final String colName = ColumnNameConstants.POINT_SIZE + this.hashCode();
        chart().figure().registerTableFunction(getTable(),
            t -> constructTableFromFunctionOnCategoryCol(t, factors, Number.class, colName));
        chart().figure().registerFigureFunction(
            new FigureImplFunction(f -> f.pointSize(getTable(), getCategoryCol(), colName), this));
        return this;
    }

    @Override
    public <COLOR extends Paint> CategoryDataSeries pointColor(
        final Function<Comparable, COLOR> colors) {
        final String colName = ColumnNameConstants.POINT_COLOR + this.hashCode();
        chart().figure().registerTableFunction(getTable(),
            t -> constructTableFromFunctionOnCategoryCol(t, colors, Paint.class, colName));
        chart().figure().registerFigureFunction(
            new FigureImplFunction(f -> f.pointColor(getTable(), getCategoryCol(), colName), this));
        return this;
    }

    @Override
    public <COLOR extends Integer> CategoryDataSeries pointColorInteger(
        final Function<Comparable, COLOR> colors) {
        final String colName = ColumnNameConstants.POINT_COLOR + this.hashCode();
        chart().figure().registerTableFunction(getTable(),
            t -> constructTableFromFunctionOnCategoryCol(t, colors, Integer.class, colName));
        chart().figure().registerFigureFunction(
            new FigureImplFunction(f -> f.pointColor(getTable(), getCategoryCol(), colName), this));
        return this;
    }

    @Override
    public <T extends Paint> CategoryDataSeries pointColorByY(Function<Double, T> colors) {
        final String colName = ColumnNameConstants.POINT_COLOR + this.hashCode();
        chart().figure().registerTableFunction(getTable(),
            t -> constructTableFromFunctionOnNumericalCol(t, colors, Paint.class, colName));
        chart().figure().registerFigureFunction(
            new FigureImplFunction(f -> f.pointColor(getTable(), getCategoryCol(), colName), this));
        return this;
    }

    @Override
    public <LABEL> CategoryDataSeries pointLabel(final Function<Comparable, LABEL> labels) {
        final String colName = ColumnNameConstants.POINT_LABEL + this.hashCode();
        chart().figure().registerTableFunction(getTable(),
            t -> constructTableFromFunctionOnCategoryCol(t, labels, Object.class, colName));
        chart().figure().registerFigureFunction(
            new FigureImplFunction(f -> f.pointLabel(getTable(), getCategoryCol(), colName), this));
        return this;
    }

    private <S, T> Table constructTableFromFunctionOnCategoryCol(final Table t,
        final Function<S, T> function, final Class resultClass, final String columnName) {
        return constructTableFromFunction(t, function, resultClass, getCategoryCol(), columnName);
    }

    private <S, T> Table constructTableFromFunctionOnNumericalCol(final Table t,
        final Function<S, T> function, final Class resultClass, final String columnName) {
        return constructTableFromFunction(t, function, resultClass, getValueCol(), columnName);
    }

    protected <S, T> Table constructTableFromFunction(final Table t, final Function<S, T> function,
        final Class resultClass, final String onColumn, final String columnName) {
        ArgumentValidations.assertNotNull(function, "function", getPlotInfo());
        final String queryFunction = columnName + "Function";
        QueryScope.addParam(queryFunction, function);
        QueryLibrary.importClass(resultClass);
        return t.update(columnName + " = (" + resultClass.getSimpleName() + ") " + queryFunction
            + ".apply(" + onColumn + ")");
    }

    protected abstract Table getTable();

    protected abstract String getCategoryCol();

    protected abstract String getValueCol();
}
