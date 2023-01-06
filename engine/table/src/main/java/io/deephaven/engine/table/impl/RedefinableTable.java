/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.api.Selectable;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.select.*;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An uncoalesced table that may be redefined without triggering a {@link #coalesce()}.
 */
public abstract class RedefinableTable<IMPL_TYPE extends RedefinableTable<IMPL_TYPE>>
        extends UncoalescedTable<IMPL_TYPE> {

    protected RedefinableTable(@NotNull final TableDefinition definition, @NotNull final String description) {
        super(definition, description);
    }

    @Override
    public Table view(Collection<? extends Selectable> selectables) {
        return viewInternal(selectables, false);
    }

    @Override
    public Table updateView(Collection<? extends Selectable> selectables) {
        return viewInternal(selectables, true);
    }

    private Table viewInternal(Collection<? extends Selectable> selectables, boolean isUpdate) {
        if (selectables == null || selectables.isEmpty()) {
            return this;
        }

        final SelectColumn[] columns = Stream.concat(
                isUpdate ? definition.getColumnStream().map(cd -> new SourceColumn(cd.getName())) : Stream.empty(),
                selectables.stream().map(SelectColumn::of))
                .toArray(SelectColumn[]::new);

        final Set<ColumnDefinition<?>> resultColumnsInternal = new HashSet<>();
        final Map<String, ColumnDefinition<?>> resultColumnsExternal = new LinkedHashMap<>();
        final Map<String, ColumnDefinition<?>> allColumns = new HashMap<>(definition.getColumnNameMap());
        final Map<String, Set<String>> columnDependency = new HashMap<>();
        boolean simpleRetain = true;
        for (final SelectColumn selectColumn : columns) {
            List<String> usedColumnNames = selectColumn.initDef(allColumns);
            usedColumnNames.addAll(selectColumn.getColumnArrays());
            columnDependency.put(selectColumn.getName(), new HashSet<>(usedColumnNames));
            resultColumnsInternal.addAll(usedColumnNames.stream()
                    .filter(usedColumnName -> !resultColumnsExternal.containsKey(usedColumnName))
                    .map(definition::getColumn).collect(Collectors.toList()));
            final ColumnDefinition<?> columnDef;
            if (selectColumn.isRetain()) {
                columnDef = definition.getColumn(selectColumn.getName());
            } else {
                simpleRetain = false;
                columnDef = ColumnDefinition.fromGenericType(selectColumn.getName(), selectColumn.getReturnedType());
            }
            resultColumnsExternal.put(selectColumn.getName(), columnDef);
            allColumns.put(selectColumn.getName(), columnDef);
        }

        TableDefinition newDefExternal = TableDefinition.of(
                resultColumnsExternal.values().toArray(ColumnDefinition.ZERO_LENGTH_COLUMN_DEFINITION_ARRAY));
        if (simpleRetain) {
            // NB: We use the *external* TableDefinition because it's ordered appropriately.
            return redefine(newDefExternal);
        }
        TableDefinition newDefInternal =
                TableDefinition.of(
                        resultColumnsInternal.toArray(ColumnDefinition.ZERO_LENGTH_COLUMN_DEFINITION_ARRAY));

        return redefine(newDefExternal, newDefInternal, columns, columnDependency);
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
            throw new RuntimeException("Unknown columns: " + columnNamesToDrop.toString() + ", available columns = "
                    + getColumnSourceMap().keySet());
        }

        List<ColumnDefinition<?>> resultColumns = new ArrayList<>();
        for (ColumnDefinition<?> cDef : definition.getColumns()) {
            if (!columnNamesToDrop.contains(cDef.getName())) {
                resultColumns.add(cDef);
            }
        }
        return redefine(TableDefinition.of(resultColumns));
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
                throw new IllegalArgumentException("Bad left column in rename pair \"" + pair.toString() + "\"");
            }
            ColumnDefinition<?> cDef = definition.getColumn(pair.rightColumn);
            if (cDef == null) {
                throw new IllegalArgumentException("Column \"" + pair.rightColumn + "\" not found");
            }
            pairLookup.put(pair.rightColumn, pair.leftColumn);
            columnDependency.put(pair.leftColumn, new HashSet<>(Collections.singletonList(pair.rightColumn)));
        }

        ColumnDefinition<?>[] columnDefinitions = definition.getColumnsArray();
        ColumnDefinition<?>[] resultColumnsExternal = new ColumnDefinition[columnDefinitions.length];
        SelectColumn[] viewColumns = new SelectColumn[columnDefinitions.length];
        for (int ci = 0; ci < columnDefinitions.length; ++ci) {
            ColumnDefinition<?> cDef = columnDefinitions[ci];
            String newName = pairLookup.get(cDef.getName());
            if (newName == null) {
                resultColumnsExternal[ci] = cDef;
                viewColumns[ci] = new SourceColumn(cDef.getName());
            } else {
                resultColumnsExternal[ci] = cDef.withName(newName);
                viewColumns[ci] = new SourceColumn(cDef.getName(), newName);
            }
        }
        return redefine(TableDefinition.of(resultColumnsExternal), definition, viewColumns, columnDependency);
    }

    /**
     * Redefine this table with a subset of its current columns.
     *
     * @param newDefinition A TableDefinition with a subset of this RedefinableTable's ColumnDefinitions.
     * @return
     */
    protected abstract Table redefine(TableDefinition newDefinition);

    /**
     * Redefine this table with a subset of its current columns, with a potentially-differing definition to present to
     * external interfaces and one or more select columns to apply.
     *
     * @param newDefinitionExternal A TableDefinition that represents the results of
     *        redefine(newDefinitionInternal).view(viewColumns).
     * @param newDefinitionInternal A TableDefinition with a subset of this RedefinableTable's ColumnDefinitions.
     * @param viewColumns A set of SelectColumns to apply in order to transform a table with newDefinitionInternal to a
     *        table with newDefinitionExternal.
     * @param columnDependency
     * @return
     */
    protected abstract Table redefine(TableDefinition newDefinitionExternal, TableDefinition newDefinitionInternal,
            SelectColumn[] viewColumns, Map<String, Set<String>> columnDependency);
}
