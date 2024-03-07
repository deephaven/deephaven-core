//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.datasets.multiseries;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.datasets.DataSeriesInternal;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.plot.util.tables.TableBackedPartitionedTableHandle;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.PartitionedTable;

import java.util.Arrays;
import java.util.Map;

public abstract class AbstractPartitionedTableHandleMultiSeries<SERIES extends DataSeriesInternal>
        extends AbstractMultiSeries<SERIES> {
    private static final long serialVersionUID = 1L;

    private final TableBackedPartitionedTableHandle partitionedTableHandle;
    private final String x;
    private final String y;

    /**
     * Creates a MultiSeries instance.
     *
     * @param axes axes on which this {@link MultiSeries} will be plotted
     * @param id data series id
     * @param name series name
     * @param partitionedTableHandle table handle
     * @param x the x-axis data column in {@code partitionedTableHandle}
     * @param y the y-axis data column in {@code partitionedTableHandle}
     * @param byColumns columns forming the keys of the partitioned table
     */
    AbstractPartitionedTableHandleMultiSeries(final AxesImpl axes, final int id, final Comparable name,
            final TableBackedPartitionedTableHandle partitionedTableHandle, final String x, final String y,
            final String[] byColumns) {
        super(axes, id, name, byColumns);
        this.partitionedTableHandle = partitionedTableHandle;
        this.x = x;
        this.y = y;
        addPartitionedTableHandle(partitionedTableHandle);
        applyNamingFunction();
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    AbstractPartitionedTableHandleMultiSeries(final AbstractPartitionedTableHandleMultiSeries series,
            final AxesImpl axes) {
        super(series, axes);
        this.partitionedTableHandle = series.partitionedTableHandle;
        this.namingFunction = series.namingFunction;
        this.x = series.x;
        this.y = series.y;
        addPartitionedTableHandle(partitionedTableHandle);
    }

    @Override
    public String getX() {
        return x;
    }

    public String getY() {
        return y;
    }

    @Override
    public PartitionedTable getPartitionedTable() {
        if (partitionedTable == null) {
            synchronized (partitionedTableLock) {
                if (partitionedTable != null) {
                    return partitionedTable;
                }

                partitionedTable = partitionedTableHandle.getPartitionedTable();
            }
        }

        return partitionedTable;
    }

    public TableBackedPartitionedTableHandle getPartitionedTableHandle() {
        return partitionedTableHandle;
    }

    @Override
    public void applyTransform(final String columnName, final String update, final Class[] classesToImport,
            final Map<String, Object> params, boolean columnTypesPreserved) {
        ArgumentValidations.assertNull(partitionedTable, "partitionedTable must be null", getPlotInfo());
        Arrays.stream(classesToImport)
                .forEach(aClass -> ExecutionContext.getContext().getQueryLibrary().importClass(aClass));
        params.forEach(QueryScope::addParam);
        partitionedTableHandle.addColumn(columnName);

        chart().figure().registerTableFunction(partitionedTableHandle.getTable(), t -> {
            return t.update(update);
        });
    }
}
