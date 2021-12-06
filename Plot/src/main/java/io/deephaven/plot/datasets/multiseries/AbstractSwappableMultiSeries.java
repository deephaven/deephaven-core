package io.deephaven.plot.datasets.multiseries;

import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.datasets.DataSeriesInternal;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.plot.util.tables.SwappableTable;
import io.deephaven.plot.util.tables.TableHandle;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.lang.QueryLibrary;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.table.TableMap;

import java.util.*;
import java.util.function.Function;

public abstract class AbstractSwappableMultiSeries<SERIES extends DataSeriesInternal>
        extends AbstractMultiSeries<SERIES> {
    private static final long serialVersionUID = 3034015389062108370L;

    private Table localTable;
    private final SwappableTable swappableTable;
    private final String x;
    private final String y;
    private final List<TableHandle> tableHandlesList = new ArrayList<>();

    /**
     * Creates a MultiSeries instance.
     *
     * @param axes axes on which this {@link MultiSeries} will be plotted
     * @param id data series id
     * @param name series name
     * @param swappableTable table handle
     * @param x the x-axis data column in {@code swappableTable}
     * @param y the y-axis data column in {@code swappableTable}
     * @param byColumns columns forming the keys of the table map
     */
    AbstractSwappableMultiSeries(final AxesImpl axes, final int id, final Comparable name,
            final SwappableTable swappableTable, final String x, final String y, final String[] byColumns) {
        super(axes, id, name, byColumns);

        this.swappableTable = swappableTable;
        this.x = x;
        this.y = y;
        addSwappableTable(swappableTable);
        applyNamingFunction();
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    AbstractSwappableMultiSeries(final AbstractSwappableMultiSeries series, final AxesImpl axes) {
        super(series, axes);
        this.namingFunction = series.namingFunction;
        this.swappableTable = series.swappableTable;
        this.initialized = series.initialized;
        this.x = series.x;
        this.y = series.y;
        this.setDynamicSeriesNamer(series.getDynamicSeriesNamer());
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
        if (localTable == null) {
            return EMPTY_TABLE_MAP;
        }

        if (tableMap == null) {
            synchronized (tableMapLock) {
                if (tableMap != null) {
                    return tableMap;
                }
                tableMap = localTable.partitionBy(byColumns);
            }
        }

        return this.tableMap;
    }

    @Override
    public void addTableHandle(final TableHandle tableHandle) {
        tableHandlesList.add(tableHandle);
        super.addTableHandle(tableHandle);
    }

    public SwappableTable getSwappableTable() {
        return swappableTable;
    }

    @Override
    protected void applyFunction(final java.util.function.Function function, final String columnName,
            final String functionInput, final Class resultClass) {
        ArgumentValidations.assertNotNull(function, "function", getPlotInfo());
        final String queryFunction = columnName + "Function";
        final Map<String, Object> params = new HashMap<>();
        params.put(queryFunction, function);

        final String update = columnName + " = (" + resultClass.getSimpleName() + ") " + queryFunction + ".apply("
                + functionInput + ")";

        applyTransform(columnName, update, new Class[] {resultClass}, params, true);
    }

    @Override
    public void applyTransform(final String columnName, final String update, final Class[] classesToImport,
            final Map<String, Object> params, boolean columnTypesPreserved) {
        ArgumentValidations.assertNull(tableMap, "tableMap must be null", getPlotInfo());
        swappableTable.addColumn(columnName);
        final Function<Table, Table> tableTransform = t -> {
            Arrays.stream(classesToImport).forEach(QueryLibrary::importClass);
            params.forEach(QueryScope::addParam);
            return t.update(update);
        };
        chart().figure().registerTableMapFunction(swappableTable.getTableMapHandle(), tableTransform);
    }
}
