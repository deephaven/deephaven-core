/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.xy;

import io.deephaven.db.plot.AxesImpl;
import io.deephaven.db.plot.SeriesInternal;
import io.deephaven.db.plot.datasets.ColumnNameConstants;
import io.deephaven.db.plot.datasets.data.IndexableNumericDataTable;
import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.plot.util.functions.FigureImplFunction;
import io.deephaven.db.plot.util.tables.TableHandle;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.libs.QueryLibrary;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.gui.color.Paint;

import java.util.function.Function;

public class XYDataSeriesTableArray extends XYDataSeriesArray implements SeriesInternal {

    private final TableHandle tableHandle;
    private final String x;
    private final String y;

    public XYDataSeriesTableArray(final AxesImpl axes, final int id, final Comparable name,
            final TableHandle tableHandle, final String x, final String y) {
        super(axes, id, name, new IndexableNumericDataTable(tableHandle, x, new PlotInfo(axes, name)),
                new IndexableNumericDataTable(tableHandle, y, new PlotInfo(axes, name)));

        this.tableHandle = tableHandle;
        this.x = x;
        this.y = y;
    }

    @Override
    public <T extends Paint> AbstractXYDataSeries pointColorByY(Function<Double, T> colors) {
        final String colName = ColumnNameConstants.POINT_COLOR + this.hashCode();
        chart().figure().registerTableFunction(tableHandle.getTable(),
                t -> constructTableFromFunction(t, colors, Paint.class, y, colName));
        chart().figure().registerFigureFunction(
                new FigureImplFunction(f -> f.pointColor(tableHandle.getTable(), colName), this));
        return this;
    }

    private XYDataSeriesTableArray(final XYDataSeriesTableArray series, final AxesImpl axes) {
        super(series, axes);
        this.tableHandle = series.tableHandle;
        this.x = series.x;
        this.y = series.y;
    }

    @Override
    public XYDataSeriesTableArray copy(final AxesImpl axes) {
        return new XYDataSeriesTableArray(this, axes);
    }

    private <S, T> Table constructTableFromFunction(final Table t, final Function<S, T> function,
            final Class resultClass, final String onColumn, final String columnName) {
        ArgumentValidations.assertNotNull(function, "function", getPlotInfo());
        final String queryFunction = columnName + "Function";
        QueryScope.addParam(queryFunction, function);
        QueryLibrary.importClass(resultClass);
        return t.update(
                columnName + " = (" + resultClass.getSimpleName() + ") " + queryFunction + ".apply(" + onColumn + ")");
    }
}
