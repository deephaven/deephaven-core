package io.deephaven.engine.plot.util.tables;

import io.deephaven.engine.plot.errors.PlotIllegalStateException;
import io.deephaven.engine.plot.errors.PlotInfo;
import io.deephaven.engine.plot.util.ArgumentValidations;
import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.TableDefinition;

import java.util.Collection;

/**
 * {@link TableMapHandle} with an underlying table. The purpose of this class is to allow consolidation between
 * constructed TableMaps in FigureWidget.
 */
public class TableBackedTableMapHandle extends TableMapHandle {

    private transient Table table;
    private final TableDefinition tableDefinition;

    public TableBackedTableMapHandle(final TableHandle tableHandle, final String[] keyColumns,
            final PlotInfo plotInfo) {
        this(tableHandle.getTable(), tableHandle.getColumns(), keyColumns, plotInfo);
    }

    public TableBackedTableMapHandle(final Table table, final Collection<String> columns, final String[] keyColumns,
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
