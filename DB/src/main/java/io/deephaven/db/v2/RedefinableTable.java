/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.select.*;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

/**
 * An uncoalesced table that may be redefined without triggering a {@link #coalesce()}.
 */
public abstract class RedefinableTable extends UncoalescedTable {

    protected RedefinableTable(@NotNull final TableDefinition definition,
        @NotNull final String description) {
        super(definition, description);
    }

    @Override
    public Table view(SelectColumn... columns) {
        if (columns == null || columns.length == 0) {
            return this;
        }
        Set<ColumnDefinition> resultColumnsInternal = new HashSet<>();
        Map<String, ColumnDefinition> resultColumnsExternal = new LinkedHashMap<>();
        Map<String, ColumnDefinition> allColumns = new HashMap<>(definition.getColumnNameMap());
        Map<String, Set<String>> columnDependency = new HashMap<>();
        boolean simpleRetain = true;
        for (SelectColumn selectColumn : columns) {
            List<String> usedColumnNames = selectColumn.initDef(allColumns);
            columnDependency.put(selectColumn.getName(), new HashSet<>(usedColumnNames));
            resultColumnsInternal.addAll(usedColumnNames.stream()
                .filter(usedColumnName -> !resultColumnsExternal.containsKey(usedColumnName))
                .map(definition::getColumn).collect(Collectors.toList()));
            final ColumnDefinition columnDef;
            if (selectColumn.isRetain()) {
                columnDef = definition.getColumn(selectColumn.getName());
            } else {
                simpleRetain = false;
                // noinspection unchecked
                columnDef = ColumnDefinition.fromGenericType(selectColumn.getName(),
                    selectColumn.getReturnedType());
            }
            resultColumnsExternal.put(selectColumn.getName(), columnDef);
            allColumns.put(selectColumn.getName(), columnDef);
        }

        TableDefinition newDefExternal = new TableDefinition(resultColumnsExternal.values()
            .toArray(new ColumnDefinition[resultColumnsExternal.size()]));
        if (simpleRetain) {
            // NB: We use the *external* TableDefinition because it's ordered appropriately.
            return redefine(newDefExternal);
        }
        TableDefinition newDefInternal = new TableDefinition(
            resultColumnsInternal.toArray(new ColumnDefinition[resultColumnsInternal.size()]));
        return redefine(newDefExternal, newDefInternal, columns, columnDependency);
    }

    @Override
    public Table updateView(SelectColumn... columns) {
        if (columns == null || columns.length == 0) {
            return this;
        }
        LinkedHashMap<String, SelectColumn> viewColumns = new LinkedHashMap<>();
        for (ColumnDefinition cDef : definition.getColumns()) {
            viewColumns.put(cDef.getName(), new SourceColumn(cDef.getName()));
        }
        for (SelectColumn updateColumn : columns) {
            viewColumns.put(updateColumn.getName(), updateColumn);
        }
        return view(viewColumns.values().toArray(new SelectColumn[viewColumns.size()]));
    }

    @Override
    public Table dropColumns(final String... columnNames) {
        if (columnNames == null || columnNames.length == 0) {
            return this;
        }

        final Set<String> columnNamesToDrop = new HashSet<>(Arrays.asList(columnNames));
        final Set<String> existingColumns = new HashSet<>(definition.getColumnNames());
        if (!existingColumns.containsAll(columnNamesToDrop)) {
            columnNamesToDrop.removeAll(existingColumns);
            throw new RuntimeException("Unknown columns: " + columnNamesToDrop.toString()
                + ", available columns = " + getColumnSourceMap().keySet());
        }

        List<ColumnDefinition> resultColumns = new ArrayList<>();
        for (ColumnDefinition cDef : definition.getColumns()) {
            if (!columnNamesToDrop.contains(cDef.getName())) {
                resultColumns.add(cDef);
            }
        }
        return redefine(new TableDefinition(resultColumns));
    }

    @Override
    public Table renameColumns(MatchPair... pairs) {
        if (pairs == null || pairs.length == 0) {
            return this;
        }
        Map<String, Set<String>> columnDependency = new HashMap<>();
        Map<String, String> pairLookup = new HashMap<>();
        for (MatchPair pair : pairs) {
            if (pair.leftColumn == null || pair.leftColumn.equals("")) {
                throw new IllegalArgumentException(
                    "Bad left column in rename pair \"" + pair.toString() + "\"");
            }
            ColumnDefinition cDef = definition.getColumn(pair.rightColumn);
            if (cDef == null) {
                throw new IllegalArgumentException("Column \"" + pair.rightColumn + "\" not found");
            }
            pairLookup.put(pair.rightColumn, pair.leftColumn);
            columnDependency.put(pair.leftColumn,
                new HashSet<>(Collections.singletonList(pair.rightColumn)));
        }

        ColumnDefinition columnDefinitions[] = definition.getColumns();
        ColumnDefinition resultColumnsExternal[] = new ColumnDefinition[columnDefinitions.length];
        SelectColumn[] viewColumns = new SelectColumn[columnDefinitions.length];
        for (int ci = 0; ci < columnDefinitions.length; ++ci) {
            ColumnDefinition cDef = columnDefinitions[ci];
            String newName = pairLookup.get(cDef.getName());
            if (newName == null) {
                resultColumnsExternal[ci] = cDef;
                viewColumns[ci] = new SourceColumn(cDef.getName());
            } else {
                resultColumnsExternal[ci] = cDef.rename(newName);
                viewColumns[ci] = new SourceColumn(cDef.getName(), newName);
            }
        }
        return redefine(new TableDefinition(resultColumnsExternal), definition, viewColumns,
            columnDependency);
    }

    /**
     * Redefine this table with a subset of its current columns.
     * 
     * @param newDefinition A TableDefinition with a subset of this RedefinableTable's
     *        ColumnDefinitions.
     * @return
     */
    protected abstract Table redefine(TableDefinition newDefinition);

    /**
     * Redefine this table with a subset of its current columns, with a potentially-differing
     * definition to present to external interfaces and one or more select columns to apply.
     *
     * @param newDefinitionExternal A TableDefinition that represents the results of
     *        redefine(newDefinitionInternal).view(viewColumns).
     * @param newDefinitionInternal A TableDefinition with a subset of this RedefinableTable's
     *        ColumnDefinitions.
     * @param viewColumns A set of SelectColumns to apply in order to transform a table with
     *        newDefinitionInternal to a table with newDefinitionExternal.
     * @param columnDependency
     * @return
     */
    protected abstract Table redefine(TableDefinition newDefinitionExternal,
        TableDefinition newDefinitionInternal, SelectColumn[] viewColumns,
        Map<String, Set<String>> columnDependency);
}
