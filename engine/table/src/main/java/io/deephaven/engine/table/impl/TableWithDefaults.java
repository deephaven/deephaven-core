/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.api.*;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.Pair;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.lang.QueryLanguageParser;
import io.deephaven.engine.table.iterators.*;
import io.deephaven.engine.table.impl.select.AjMatchPairFactory;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.table.impl.select.WouldMatchPairFactory;
import io.deephaven.engine.util.TableTools;
import io.deephaven.api.util.ConcurrentMethod;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.util.ColumnFormattingValues;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Sub-interface to capture default methods rom {@link Table}.
 */
public interface TableWithDefaults extends Table, TableOperationsWithDefaults<Table, Table> {

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
    default Table getMeta() {
        List<String> columnNames = new ArrayList<>();
        List<String> columnDataTypes = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();
        List<Boolean> columnPartitioning = new ArrayList<>();
        List<Boolean> columnGrouping = new ArrayList<>();
        for (ColumnDefinition<?> cDef : getDefinition().getColumns()) {
            columnNames.add(cDef.getName());
            columnDataTypes.add(cDef.getDataType().getName());
            columnTypes.add(cDef.getColumnType().name());
            columnPartitioning.add(cDef.isPartitioning());
            columnGrouping.add(cDef.isGrouping());

        }
        final String[] resultColumnNames = {"Name", "DataType", "ColumnType", "IsPartitioning", "IsGrouping"};
        final Object[] resultValues = {
                columnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                columnDataTypes.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                columnTypes.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                columnPartitioning.toArray(new Boolean[0]),
                columnGrouping.toArray(new Boolean[0]),
        };

        return new InMemoryTable(resultColumnNames, resultValues);
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default int numColumns() {
        return getDefinition().numColumns();
    }

    @Override
    @ConcurrentMethod
    default boolean hasColumns(final String... columnNames) {
        if (columnNames == null) {
            throw new IllegalArgumentException("columnNames cannot be null!");
        }
        return hasColumns(Arrays.asList(columnNames));
    }

    @Override
    @ConcurrentMethod
    default boolean hasColumns(Collection<String> columnNames) {
        if (columnNames == null) {
            throw new IllegalArgumentException("columnNames cannot be null!");
        }
        return getDefinition().getColumnNameMap().keySet().containsAll(columnNames);
    }

    @Override
    default long sizeForInstrumentation() {
        return size();
    }

    @Override
    default boolean isEmpty() {
        return size() == 0;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Attributes
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default Map<String, Object> getAttributes() {
        return getAttributes(Collections.emptySet());
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ColumnSources for fetching data by row key
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default <T> ColumnSource<T> getColumnSource(String sourceName, Class<? extends T> clazz) {
        @SuppressWarnings("rawtypes")
        ColumnSource rawColumnSource = getColumnSource(sourceName);
        // noinspection unchecked
        return rawColumnSource.cast(clazz);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // DataColumns for fetching data by row position; generally much less efficient than ColumnSource
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default DataColumn[] getColumns() {
        return getDefinition().getColumnStream().map(c -> getColumn(c.getName())).toArray(DataColumn[]::new);
    }

    @Override
    default DataColumn getColumn(final int columnIndex) {
        return getColumn(this.getDefinition().getColumns().get(columnIndex).getName());
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Column Iterators
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default <TYPE> Iterator<TYPE> columnIterator(@NotNull final String columnName) {
        return ColumnIterator.make(getColumnSource(columnName), getRowSet());
    }

    @Override
    default CharacterColumnIterator characterColumnIterator(@NotNull final String columnName) {
        return new CharacterColumnIterator(this, columnName);
    }

    @Override
    default ByteColumnIterator byteColumnIterator(@NotNull final String columnName) {
        return new ByteColumnIterator(this, columnName);
    }

    @Override
    default ShortColumnIterator shortColumnIterator(@NotNull final String columnName) {
        return new ShortColumnIterator(this, columnName);
    }

    @Override
    default IntegerColumnIterator integerColumnIterator(@NotNull final String columnName) {
        return new IntegerColumnIterator(this, columnName);
    }

    @Override
    default LongColumnIterator longColumnIterator(@NotNull final String columnName) {
        return new LongColumnIterator(this, columnName);
    }

    @Override
    default FloatColumnIterator floatColumnIterator(@NotNull final String columnName) {
        return new FloatColumnIterator(this, columnName);
    }

    @Override
    default DoubleColumnIterator doubleColumnIterator(@NotNull final String columnName) {
        return new DoubleColumnIterator(this, columnName);
    }

    @Override
    default <DATA_TYPE> ObjectColumnIterator<DATA_TYPE> objectColumnIterator(@NotNull final String columnName) {
        return new ObjectColumnIterator<>(this, columnName);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Filter Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default Table where(Filter... filters) {
        return where(List.of(filters));
    }

    @Override
    @ConcurrentMethod
    default Table wouldMatch(String... expressions) {
        return wouldMatch(WouldMatchPairFactory.getExpressions(expressions));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Column Selection Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default Table select(Selectable... columns) {
        return select(List.of(columns));
    }

    @Override
    default Table select() {
        return select(getDefinition().getColumnNamesArray());
    }

    @Override
    @ConcurrentMethod
    default Table selectDistinct() {
        return selectDistinct(getDefinition().getColumnNamesArray());
    }

    @Override
    default Table update(Selectable... newColumns) {
        return update(List.of(newColumns));
    }

    @Override
    default Table lazyUpdate(Selectable... newColumns) {
        return lazyUpdate(List.of(newColumns));
    }

    @Override
    default Table view(Selectable... columns) {
        return view(List.of(columns));
    }

    @Override
    default Table updateView(Selectable... newColumns) {
        return updateView(List.of(newColumns));
    }

    @Override
    @ConcurrentMethod
    default Table dropColumns(Collection<String> columnNames) {
        return dropColumns(columnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default Table dropColumnFormats() {
        String[] columnAry = getDefinition().getColumnStream()
                .map(ColumnDefinition::getName)
                .filter(ColumnFormattingValues::isFormattingColumn)
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
    default Table renameColumns(Collection<String> columns) {
        return renameColumns(MatchPairFactory.getExpressions(columns));
    }

    @Override
    default Table renameColumns(String... columns) {
        return renameColumns(MatchPairFactory.getExpressions(columns));
    }

    @Override
    default Table renameAllColumns(UnaryOperator<String> renameFunction) {
        return renameColumns(getDefinition().getColumnStream().map(ColumnDefinition::getName)
                .map(n -> new MatchPair(renameFunction.apply(n), n)).toArray(MatchPair[]::new));
    }

    @Override
    @ConcurrentMethod
    default Table formatRowWhere(String condition, String formula) {
        return formatColumnWhere(ColumnFormattingValues.ROW_FORMAT_NAME, condition, formula);
    }

    @Override
    @ConcurrentMethod
    default Table formatColumnWhere(String columnName, String condition, String formula) {
        return formatColumns(
                columnName + " = (" + condition + ") ? io.deephaven.engine.util.ColorUtil.toLong(" + formula
                        + ") : io.deephaven.engine.util.ColorUtil.toLong(NO_FORMATTING)");
    }

    @Override
    @ConcurrentMethod
    default Table moveColumnsUp(String... columnsToMove) {
        return moveColumns(0, columnsToMove);
    }

    @Override
    @ConcurrentMethod
    default Table moveColumnsDown(String... columnsToMove) {
        return moveColumns(numColumns() - columnsToMove.length, true, columnsToMove);
    }

    @Override
    @ConcurrentMethod
    default Table moveColumns(int index, String... columnsToMove) {
        return moveColumns(index, false, columnsToMove);
    }

    @Override
    @ConcurrentMethod
    default Table dateTimeColumnAsNanos(String columnName) {
        return dateTimeColumnAsNanos(columnName, columnName);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Join Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default Table exactJoin(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return exactJoin(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd));
    }

    @Override
    default Table aj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return aj(rightTable, columnsToMatch, columnsToAdd, AsOfMatchRule.LESS_THAN_EQUAL);
    }

    @Override
    default Table aj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, AsOfJoinRule asOfJoinRule) {
        return aj(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd),
                AsOfMatchRule.of(asOfJoinRule));
    }

    @Override
    default Table aj(Table rightTable, Collection<String> columnsToMatch) {
        Pair<MatchPair[], AsOfMatchRule> expressions = AjMatchPairFactory.getExpressions(false, columnsToMatch);
        return aj(
                rightTable,
                expressions.getFirst(),
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                expressions.getSecond());
    }

    @Override
    default Table raj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return raj(rightTable, columnsToMatch, columnsToAdd, AsOfMatchRule.GREATER_THAN_EQUAL);
    }

    @Override
    default Table raj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, ReverseAsOfJoinRule reverseAsOfJoinRule) {
        return raj(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd),
                AsOfMatchRule.of(reverseAsOfJoinRule));
    }

    @Override
    default Table raj(Table rightTable, Collection<String> columnsToMatch) {
        Pair<MatchPair[], AsOfMatchRule> expressions = AjMatchPairFactory.getExpressions(true, columnsToMatch);
        return raj(
                rightTable,
                expressions.getFirst(),
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                expressions.getSecond());
    }

    @Override
    default Table naturalJoin(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return naturalJoin(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd));
    }

    @Override
    default Table join(Table rightTable) {
        return join(
                rightTable,
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY);
    }

    @Override
    default Table join(Table rightTable, int numRightBitsToReserve) {
        return join(rightTable, Collections.emptyList(), Collections.emptyList(), numRightBitsToReserve);
    }

    @Override
    default Table join(Table rightTable, String columnsToMatch, int numRightBitsToReserve) {
        return join(
                rightTable,
                MatchPairFactory.getExpressions(TableOperationsWithDefaults.splitToCollection(columnsToMatch)),
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                numRightBitsToReserve);
    }

    @Override
    default Table join(Table rightTable, String columnsToMatch, String columnsToAdd, int numRightBitsToReserve) {
        return join(
                rightTable,
                MatchPairFactory.getExpressions(TableOperationsWithDefaults.splitToCollection(columnsToMatch)),
                MatchPairFactory.getExpressions(TableOperationsWithDefaults.splitToCollection(columnsToAdd)),
                numRightBitsToReserve);
    }

    @Override
    default Table join(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return join(rightTable, columnsToMatch, columnsToAdd, CrossJoinHelper.DEFAULT_NUM_RIGHT_BITS_TO_RESERVE);
    }

    @Override
    default Table join(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return join(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd));
    }

    @Override
    default Table join(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, int numRightBitsToReserve) {
        return join(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd),
                numRightBitsToReserve);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Aggregation Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default Table headBy(long nRows, Collection<String> groupByColumnNames) {
        return headBy(nRows, groupByColumnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }

    @Override
    default Table tailBy(long nRows, Collection<String> groupByColumnNames) {
        return tailBy(nRows, groupByColumnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default Table applyToAllBy(String formulaColumn, String columnParamName,
            Collection<? extends ColumnName> groupByColumns) {
        return aggAllBy(AggSpec.formula(formulaColumn, columnParamName), groupByColumns.toArray(ColumnName[]::new));
    }

    @Override
    @ConcurrentMethod
    default Table applyToAllBy(String formulaColumn, Collection<? extends ColumnName> groupByColumns) {
        return applyToAllBy(formulaColumn, "each", groupByColumns);
    }

    @Override
    @ConcurrentMethod
    default Table applyToAllBy(String formulaColumn, String... groupByColumns) {
        return applyToAllBy(formulaColumn, ColumnName.from(groupByColumns));
    }

   // -----------------------------------------------------------------------------------------------------------------
    // Disaggregation Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default Table ungroup(String... columnsToUngroup) {
        return ungroup(false, columnsToUngroup);
    }

    @Override
    default Table ungroupAllBut(String... columnsNotToUngroup) {
        final Set<String> columnsNotToUnwrapSet = Arrays.stream(columnsNotToUngroup).collect(Collectors.toSet());
        return ungroup(getDefinition().getColumnStream()
                .filter(c -> !columnsNotToUnwrapSet.contains(c.getName())
                        && (c.getDataType().isArray() || QueryLanguageParser.isTypedVector(c.getDataType())))
                .map(ColumnDefinition::getName).toArray(String[]::new));
    }

    @Override
    default Table ungroup() {
        return ungroup(getDefinition().getColumnStream()
                .filter(c -> c.getDataType().isArray() || QueryLanguageParser.isTypedVector(c.getDataType()))
                .map(ColumnDefinition::getName).toArray(String[]::new));
    }

    @Override
    default Table ungroup(boolean nullFill) {
        return ungroup(nullFill,
                getDefinition().getColumnStream()
                        .filter(c -> c.getDataType().isArray() || QueryLanguageParser.isTypedVector(c.getDataType()))
                        .map(ColumnDefinition::getName).toArray(String[]::new));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // PartitionBy Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default PartitionedTable partitionBy(String... keyColumnNames) {
        return partitionBy(false, keyColumnNames);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Hierarchical table operations (rollup and treeTable).
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    default Table rollup(Collection<? extends Aggregation> aggregations, Collection<String> groupByColumns) {
        return rollup(aggregations, ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default Table rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents,
            Collection<String> groupByColumns) {
        return rollup(aggregations, includeConstituents,
                ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default Table rollup(Collection<? extends Aggregation> aggregations, String... groupByColumns) {
        return rollup(aggregations, ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default Table rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents,
            String... groupByColumns) {
        return rollup(aggregations, includeConstituents,
                ColumnName.from(groupByColumns).toArray(ZERO_LENGTH_COLUMNNAME_ARRAY));
    }

    @Override
    @ConcurrentMethod
    default Table rollup(Collection<? extends Aggregation> aggregations, ColumnName... groupByColumns) {
        return rollup(aggregations, false, groupByColumns);
    }

    @Override
    @ConcurrentMethod
    default Table rollup(Collection<? extends Aggregation> aggregations) {
        return rollup(aggregations, false, ZERO_LENGTH_COLUMNNAME_ARRAY);
    }

    @Override
    @ConcurrentMethod
    default Table rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents) {
        return rollup(aggregations, includeConstituents, ZERO_LENGTH_COLUMNNAME_ARRAY);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Snapshot Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default Table snapshotIncremental(Table rightTable, String... stampColumns) {
        return snapshotIncremental(rightTable, false, stampColumns);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Merge Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default Table mergeBefore(final Table... others) {
        final List<Table> tables = new ArrayList<>(others.length + 1);
        tables.add(this);
        tables.addAll(List.of(others));
        return TableTools.merge(tables);
    }

    @Override
    default Table mergeAfter(final Table... others) {
        final List<Table> tables = new ArrayList<>(others.length + 1);
        tables.addAll(List.of(others));
        tables.add(this);
        return TableTools.merge(tables);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Miscellaneous Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default <R> R apply(Function<Table, R> function) {
        final QueryPerformanceNugget nugget =
                QueryPerformanceRecorder.getInstance().getNugget("apply(" + function + ")");
        try {
            return function.apply(this);
        } finally {
            nugget.done();
        }
    }

    @Override
    @ConcurrentMethod
    default Table withColumnDescription(String column, String description) {
        return withColumnDescription(Collections.singletonMap(column, description));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Resource Management
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default void close() {
        releaseCachedResources();
    }

    @Override
    default void releaseCachedResources() {}

    // -----------------------------------------------------------------------------------------------------------------
    // Methods for dynamic tables
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default void listenForUpdates(ShiftObliviousListener listener) {
        listenForUpdates(listener, false);
    }

    @Override
    default boolean isFailed() {
        return false;
    }
}
