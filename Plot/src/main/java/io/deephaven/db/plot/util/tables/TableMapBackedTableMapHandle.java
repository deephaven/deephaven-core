package io.deephaven.db.plot.util.tables;

import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.TableMap;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * {@link TableMapHandle} with an underlying TableMap.
 */
public class TableMapBackedTableMapHandle extends TableMapHandle {

    private static final long serialVersionUID = 4820388203519593898L;

    private TableDefinition tableDefinition;
    private transient Table tableDefinitionTable;;
    private Set<String> viewColumns;

    public TableMapBackedTableMapHandle(final TableMap tableMap, final TableDefinition tableDefinition,
            final String[] keyColumns, final PlotInfo plotInfo, Collection<String> columns) {
        this(tableMap, tableDefinition, keyColumns, plotInfo, columns, null);
    }

    public TableMapBackedTableMapHandle(@NotNull final TableMap tableMap,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final String[] keyColumns,
            final PlotInfo plotInfo,
            @NotNull Collection<String> columns,
            @Nullable Collection<String> viewColumns) {
        super(columns, keyColumns, plotInfo);

        ArgumentValidations.assertNotNull(tableMap, "tableMap", plotInfo);
        this.tableDefinition = tableDefinition;
        tableDefinitionTable = TableTools.newTable(tableDefinition);
        this.viewColumns = viewColumns == null ? null : new HashSet<>(viewColumns);
    }

    @Override
    public Set<String> getFetchViewColumns() {
        return viewColumns != null ? viewColumns : getColumns();
    }

    @Override
    public TableDefinition getTableDefinition() {
        return tableDefinition;
    }

    @Override
    public void applyFunction(final Function<Table, Table> function) {
        tableDefinitionTable = function.apply(tableDefinitionTable);
        tableDefinition = tableDefinitionTable.getDefinition();
    }
}
