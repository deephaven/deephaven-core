package io.deephaven.db.plot.util.tables;

import io.deephaven.db.plot.errors.PlotIllegalStateException;
import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;

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
