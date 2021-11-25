package io.deephaven.plot.datasets.multiseries;

import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.datasets.DataSeriesInternal;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.plot.util.tables.TableBackedTableMapHandle;
import io.deephaven.engine.table.lang.QueryLibrary;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.table.TableMap;

import java.util.Arrays;
import java.util.Map;

public abstract class AbstractTableMapHandleMultiSeries<SERIES extends DataSeriesInternal>
        extends AbstractMultiSeries<SERIES> {
    private static final long serialVersionUID = 1L;

    private final TableBackedTableMapHandle tableMapHandle;
    private final String x;
    private final String y;

    /**
     * Creates a MultiSeries instance.
     *
     * @param axes axes on which this {@link MultiSeries} will be plotted
     * @param id data series id
     * @param name series name
     * @param tableMapHandle table handle
     * @param x the x-axis data column in {@code tableMapHandle}
     * @param y the y-axis data column in {@code tableMapHandle}
     * @param byColumns columns forming the keys of the table map
     */
    AbstractTableMapHandleMultiSeries(final AxesImpl axes, final int id, final Comparable name,
            final TableBackedTableMapHandle tableMapHandle, final String x, final String y, final String[] byColumns) {
        super(axes, id, name, byColumns);
        this.tableMapHandle = tableMapHandle;
        this.x = x;
        this.y = y;
        addTableMapHandle(tableMapHandle);
        applyNamingFunction();
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    AbstractTableMapHandleMultiSeries(final AbstractTableMapHandleMultiSeries series, final AxesImpl axes) {
        super(series, axes);
        this.tableMapHandle = series.tableMapHandle;
        this.namingFunction = series.namingFunction;
        this.x = series.x;
        this.y = series.y;
        addTableMapHandle(tableMapHandle);
    }

    @Override
    public String getX() {
        return x;
    }

    public String getY() {
        return y;
    }

    @Override
    public TableMap getTableMap() {
        if (tableMap == null) {
            synchronized (tableMapLock) {
                if (tableMap != null) {
                    return tableMap;
                }

                tableMap = tableMapHandle.getTableMap();
            }
        }

        return tableMap;
    }

    public TableBackedTableMapHandle getTableMapHandle() {
        return tableMapHandle;
    }

    @Override
    public void applyTransform(final String columnName, final String update, final Class[] classesToImport,
            final Map<String, Object> params, boolean columnTypesPreserved) {
        ArgumentValidations.assertNull(tableMap, "tableMap must be null", getPlotInfo());
        Arrays.stream(classesToImport).forEach(QueryLibrary::importClass);
        params.forEach(QueryScope::addParam);
        tableMapHandle.addColumn(columnName);

        chart().figure().registerTableFunction(tableMapHandle.getTable(), t -> {
            return t.update(update);
        });
    }
}
