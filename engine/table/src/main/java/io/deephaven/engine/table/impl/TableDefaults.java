//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.*;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.Pair;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.snapshot.SnapshotWhenOptions;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Flag;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import io.deephaven.engine.table.impl.select.WouldMatchPairFactory;
import io.deephaven.api.util.ConcurrentMethod;
import io.deephaven.engine.util.ColumnFormatting;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.annotations.FinalDefault;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Sub-interface to capture default methods rom {@link Table}.
 */
public interface TableDefaults extends Table, TableOperationsDefaults<Table, Table> {

    Table[] ZERO_LENGTH_TABLE_ARRAY = new Table[0];

    @Override
    default Table coalesce() {
        if (isRefreshing()) {
            LivenessScopeStack.peek().manage(this);
        }
        return this;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Metadata
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table meta() {
        return TableTools.metaTable(getDefinition());
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default int numColumns() {
        return getDefinition().numColumns();
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default boolean hasColumns(final String... columnNames) {
        if (columnNames == null) {
            throw new IllegalArgumentException("columnNames cannot be null!");
        }
        return hasColumns(Arrays.asList(columnNames));
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default boolean hasColumns(Collection<String> columnNames) {
        if (columnNames == null) {
            throw new IllegalArgumentException("columnNames cannot be null!");
        }
        return getDefinition().getColumnNameSet().containsAll(columnNames);
    }

    @Override
    default long sizeForInstrumentation() {
        return size();
    }

    @Override
    @FinalDefault
    default boolean isEmpty() {
        return size() == 0;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ColumnSources for fetching data by row key
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @FinalDefault
    default <T> ColumnSource<T> getColumnSource(String sourceName, Class<? extends T> clazz) {
        @SuppressWarnings("rawtypes")
        ColumnSource rawColumnSource = getColumnSource(sourceName);
        // noinspection unchecked
        return rawColumnSource.cast(clazz, sourceName);
    }

    @Override
    @FinalDefault
    default <T> ColumnSource<T> getColumnSource(String sourceName, Class<? extends T> clazz,
            @Nullable Class<?> componentType) {
        @SuppressWarnings("rawtypes")
        ColumnSource rawColumnSource = getColumnSource(sourceName);
        // noinspection unchecked
        return rawColumnSource.cast(clazz, componentType, sourceName);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Filter Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table wouldMatch(String... expressions) {
        return wouldMatch(WouldMatchPairFactory.getExpressions(expressions));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Column Selection Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table selectDistinct() {
        return selectDistinct(getDefinition().getColumnNamesArray());
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table dropColumnFormats() {
        String[] columnAry = getDefinition().getColumnStream()
                .map(ColumnDefinition::getName)
                .filter(ColumnFormatting::isFormattingColumn)
                .toArray(String[]::new);
        if (columnAry.length == 0) {
            if (isRefreshing()) {
                LivenessScopeStack.peek().manage(this);
            }
            return this;
        }
        return dropColumns(columnAry);
    }

    @Override
    @FinalDefault
    default Table renameColumns(String... pairs) {
        return renameColumns(Pair.from(pairs));
    }

    @Override
    @FinalDefault
    default Table renameAllColumns(UnaryOperator<String> renameFunction) {
        return renameColumns(getDefinition()
                .getColumnStream()
                .map(ColumnDefinition::getName)
                .map(n -> Pair.of(ColumnName.of(n), ColumnName.of(renameFunction.apply(n))))
                .collect(Collectors.toList()));
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table formatRowWhere(String condition, String formula) {
        return formatColumnWhere(ColumnFormatting.Constants.ROW_FORMAT_WILDCARD, condition, formula);
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table formatColumnWhere(String columnName, String condition, String formula) {
        return formatColumns(
                columnName + " = (" + condition + ") ? io.deephaven.engine.util.ColorUtil.toLong(" + formula
                        + ") : io.deephaven.engine.util.ColorUtil.toLong(NO_FORMATTING)");
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table formatColumns(String... columnFormats) {
        final List<SelectColumn> selectColumns = Arrays.asList(SelectColumnFactory.getFormatExpressions(columnFormats));

        final Set<String> existingColumns = getDefinition().getColumnNames()
                .stream()
                .filter(column -> !ColumnFormatting.isFormattingColumn(column))
                .collect(Collectors.toSet());

        final String[] unknownColumns = selectColumns.stream()
                .map(sc -> ColumnFormatting.getFormatBaseColumn(sc.getName()))
                .filter(column -> (column != null
                        && !column.equals(ColumnFormatting.Constants.ROW_FORMAT_WILDCARD)
                        && !existingColumns.contains(column)))
                .toArray(String[]::new);

        if (unknownColumns.length > 0) {
            throw new RuntimeException(
                    "Unknown columns: " + Arrays.toString(unknownColumns) + ", available columns = " + existingColumns);
        }

        return updateView(selectColumns);
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table moveColumnsUp(String... columnsToMove) {
        return moveColumns(0, columnsToMove);
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table moveColumnsDown(String... columnsToMove) {
        return moveColumns(numColumns() - columnsToMove.length, columnsToMove);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Join Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @FinalDefault
    default Table join(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return join(
                rightTable,
                columnsToMatch,
                columnsToAdd,
                CrossJoinHelper.DEFAULT_NUM_RIGHT_BITS_TO_RESERVE);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Aggregation Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @FinalDefault
    default Table headBy(long nRows, Collection<String> groupByColumnNames) {
        return headBy(nRows, groupByColumnNames.toArray(String[]::new));
    }

    @Override
    @FinalDefault
    default Table tailBy(long nRows, Collection<String> groupByColumnNames) {
        return tailBy(nRows, groupByColumnNames.toArray(String[]::new));
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table applyToAllBy(String formulaColumn, String columnParamName,
            Collection<? extends ColumnName> groupByColumns) {
        return aggAllBy(AggSpec.formula(formulaColumn, columnParamName), groupByColumns.toArray(ColumnName[]::new));
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table applyToAllBy(String formulaColumn, Collection<? extends ColumnName> groupByColumns) {
        return applyToAllBy(formulaColumn, "each", groupByColumns);
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table applyToAllBy(String formulaColumn, String... groupByColumns) {
        return applyToAllBy(formulaColumn, ColumnName.from(groupByColumns));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // PartitionBy Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    @FinalDefault
    default PartitionedTable partitionBy(String... keyColumnNames) {
        return partitionBy(false, keyColumnNames);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Hierarchical table operations (rollup and tree).
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    @FinalDefault
    default RollupTable rollup(Collection<? extends Aggregation> aggregations) {
        return rollup(aggregations, Collections.emptyList());
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default RollupTable rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents) {
        return rollup(aggregations, includeConstituents, Collections.emptyList());
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default RollupTable rollup(Collection<? extends Aggregation> aggregations, String... groupByColumns) {
        return rollup(aggregations, ColumnName.from(groupByColumns));
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default RollupTable rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents,
            String... groupByColumns) {
        return rollup(aggregations, includeConstituents, ColumnName.from(groupByColumns));
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default RollupTable rollup(Collection<? extends Aggregation> aggregations,
            Collection<? extends ColumnName> groupByColumns) {
        return rollup(aggregations, false, groupByColumns);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Snapshot Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @FinalDefault
    default Table snapshotWhen(Table trigger, Flag... features) {
        return snapshotWhen(trigger, SnapshotWhenOptions.of(features));
    }

    @Override
    @FinalDefault
    default Table snapshotWhen(Table trigger, Collection<Flag> features, String... stampColumns) {
        return snapshotWhen(trigger, SnapshotWhenOptions.of(features, stampColumns));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Resource Management
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @FinalDefault
    default void close() {
        releaseCachedResources();
    }

    @Override
    default void releaseCachedResources() {}

    // -----------------------------------------------------------------------------------------------------------------
    // Methods for dynamic tables
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @FinalDefault
    default void addUpdateListener(ShiftObliviousListener listener) {
        addUpdateListener(listener, false);
    }
}
