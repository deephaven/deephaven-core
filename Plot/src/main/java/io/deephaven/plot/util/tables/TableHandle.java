//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.util.tables;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.*;

/**
 * A handle describing a table as well as the columns of the table that are needed.
 */
public class TableHandle implements Serializable {
    private static final long serialVersionUID = 2390871455829249662L;

    /** The base table for this query. */
    private transient Table table;

    /** The set of columns that will be used from this table. */
    private final Set<String> columns;

    public TableHandle(@NotNull final Table table,
            @NotNull final String... columns) {
        this.table = table;
        this.columns =
                new TreeSet<>(Arrays.asList(Arrays.stream(columns).filter(Objects::nonNull).toArray(String[]::new)));
    }

    public void addColumn(final String column) {
        this.columns.add(column);
    }

    public Set<String> getColumns() {
        return columns;
    }

    public boolean hasColumns(String... cols) {
        return columns.containsAll(Arrays.asList(cols));
    }

    public Table getTable() {
        return table;
    }

    /**
     * Get the {@link TableDefinition} of the table that will be handed off to actual plotting methods. This method is
     * important because in some cases (ie when ACls are applied to source tables) computations must be deferred until
     * after ACL application so that they are applied correctly. In this case, the table produced by {@link #getTable()}
     * may be the raw source table, not the final table. This method is used to get the final result table definition no
     * matter what the preconditions are.
     *
     * @return The {@link TableDefinition} of the plotted table.
     */
    public TableDefinition getFinalTableDefinition() {
        return table.getDefinition();
    }

    public void setTable(Table table) {
        this.table = table;
    }
}
