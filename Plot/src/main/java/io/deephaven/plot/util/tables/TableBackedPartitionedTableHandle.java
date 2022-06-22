/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.util.tables;

import io.deephaven.plot.errors.PlotIllegalStateException;
import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;

import java.util.Collection;

/**
 * {@link PartitionedTableHandle} with an underlying table. The purpose of this class is to allow consolidation between
 * constructed PartitionedTables in FigureWidget.
 */
public class TableBackedPartitionedTableHandle extends PartitionedTableHandle {

    private transient Table table;
    private final TableDefinition tableDefinition;

    public TableBackedPartitionedTableHandle(
            final TableHandle tableHandle,
            final String[] keyColumns,
            final PlotInfo plotInfo) {
        this(tableHandle.getTable(), tableHandle.getColumns(), keyColumns, plotInfo);
    }

    public TableBackedPartitionedTableHandle(
            final Table table,
            final Collection<String> columns,
            final String[] keyColumns,
            final PlotInfo plotInfo) {
        super(columns, keyColumns, plotInfo);

        ArgumentValidations.assertNotNull(table, "table", plotInfo);
        this.table = table;
        this.tableDefinition = table.getDefinition();
    }

    public Table getTable() {
        if (table == null) {
            throw new PlotIllegalStateException("Null table", this);
        }

        return table;
    }

    public void setTable(final Table table) {
        this.table = table;
    }

    @Override
    public TableDefinition getTableDefinition() {
        return tableDefinition;
    }
}
