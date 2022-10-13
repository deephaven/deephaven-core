/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.api.*;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.filter.Filter;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.lang.QueryLanguageParser;
import io.deephaven.engine.table.iterators.*;
import io.deephaven.api.expression.AsOfJoinMatchFactory;
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
    default Table getMeta() {
        List<String> columnNames = new ArrayList<>();
        List<String> columnDataTypes = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();
        List<Boolean> columnPartitioning = new ArrayList<>();
        List<Boolean> columnGrouping = new ArrayList<>();
        for (ColumnDefinition<?> cDef : getDefinition().getColumns()) {
            columnNames.add(cDef.getName());
            final Class<?> dataType = cDef.getDataType();
            final String dataTypeName = dataType.getCanonicalName();
            columnDataTypes.add(dataTypeName == null ? dataType.getName() : dataTypeName);
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
        return getDefinition().getColumnNameMap().keySet().containsAll(columnNames);
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
    // Attributes
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Map<String, Object> getAttributes() {
        return getAttributes(Collections.emptySet());
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
    @FinalDefault
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
    @FinalDefault
    default Table where(Filter... filters) {
        return where(List.of(filters));
    }

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
    @FinalDefault
    default Table select(Selectable... columns) {
        return select(List.of(columns));
    }

    @Override
    @FinalDefault
    default Table select() {
        return select(getDefinition().getColumnNamesArray());
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table selectDistinct() {
        return selectDistinct(getDefinition().getColumnNamesArray());
    }

    @Override
    @FinalDefault
    default Table update(Selectable... newColumns) {
        return update(List.of(newColumns));
    }

    @Override
    @FinalDefault
    default Table lazyUpdate(Selectable... newColumns) {
        return lazyUpdate(List.of(newColumns));
    }

    @Override
    @FinalDefault
    default Table view(Selectable... columns) {
        return view(List.of(columns));
    }

    @Override
    @FinalDefault
    default Table updateView(Selectable... newColumns) {
        return updateView(List.of(newColumns));
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table dropColumns(Collection<String> columnNames) {
        return dropColumns(columnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
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
    @FinalDefault
    default Table renameColumns(Collection<String> columns) {
        return renameColumns(MatchPairFactory.getExpressions(columns));
    }

    @Override
    @FinalDefault
    default Table renameColumns(String... columns) {
        return renameColumns(MatchPairFactory.getExpressions(columns));
    }

    @Override
    @FinalDefault
    default Table renameAllColumns(UnaryOperator<String> renameFunction) {
        return renameColumns(getDefinition().getColumnStream().map(ColumnDefinition::getName)
                .map(n -> new MatchPair(renameFunction.apply(n), n)).toArray(MatchPair[]::new));
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table formatRowWhere(String condition, String formula) {
        return formatColumnWhere(ColumnFormattingValues.ROW_FORMAT_NAME, condition, formula);
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
    default Table moveColumnsUp(String... columnsToMove) {
        return moveColumns(0, columnsToMove);
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table moveColumnsDown(String... columnsToMove) {
        return moveColumns(numColumns() - columnsToMove.length, true, columnsToMove);
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table moveColumns(int index, String... columnsToMove) {
        return moveColumns(index, false, columnsToMove);
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table dateTimeColumnAsNanos(String columnName) {
        return dateTimeColumnAsNanos(columnName, columnName);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Join Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @FinalDefault
    default Table exactJoin(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return exactJoin(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd));
    }

    @Override
    @FinalDefault
    default Table aj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return aj(rightTable, columnsToMatch, columnsToAdd, AsOfMatchRule.LESS_THAN_EQUAL);
    }

    @Override
    @FinalDefault
    default Table aj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, AsOfJoinRule asOfJoinRule) {
        return aj(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd),
                AsOfMatchRule.of(asOfJoinRule));
    }

    @Override
    @FinalDefault
    default Table aj(Table rightTable, Collection<String> columnsToMatch) {
        AsOfJoinMatchFactory.AsOfJoinResult result = AsOfJoinMatchFactory.getAjExpressions(columnsToMatch);
        return aj(
                rightTable,
                MatchPair.fromMatches(List.of(result.matches)),
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                AsOfMatchRule.of(result.rule));
    }

    @Override
    @FinalDefault
    default Table raj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return raj(rightTable, columnsToMatch, columnsToAdd, AsOfMatchRule.GREATER_THAN_EQUAL);
    }

    @Override
    @FinalDefault
    default Table raj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, ReverseAsOfJoinRule reverseAsOfJoinRule) {
        return raj(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd),
                AsOfMatchRule.of(reverseAsOfJoinRule));
    }

    @Override
    @FinalDefault
    default Table raj(Table rightTable, Collection<String> columnsToMatch) {
        AsOfJoinMatchFactory.ReverseAsOfJoinResult result = AsOfJoinMatchFactory.getRajExpressions(columnsToMatch);
        return raj(
                rightTable,
                MatchPair.fromMatches(List.of(result.matches)),
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                AsOfMatchRule.of(result.rule));
    }

    @Override
    @FinalDefault
    default Table naturalJoin(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return naturalJoin(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd));
    }

    @Override
    @FinalDefault
    default Table join(Table rightTable) {
        return join(
                rightTable,
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY);
    }

    @Override
    @FinalDefault
    default Table join(Table rightTable, int numRightBitsToReserve) {
        return join(rightTable, Collections.emptyList(), Collections.emptyList(), numRightBitsToReserve);
    }

    @Override
    @FinalDefault
    default Table join(Table rightTable, String columnsToMatch, int numRightBitsToReserve) {
        return join(
                rightTable,
                MatchPairFactory.getExpressions(TableOperationsDefaults.splitToCollection(columnsToMatch)),
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                numRightBitsToReserve);
    }

    @Override
    @FinalDefault
    default Table join(Table rightTable, String columnsToMatch, String columnsToAdd, int numRightBitsToReserve) {
        return join(
                rightTable,
                MatchPairFactory.getExpressions(TableOperationsDefaults.splitToCollection(columnsToMatch)),
                MatchPairFactory.getExpressions(TableOperationsDefaults.splitToCollection(columnsToAdd)),
                numRightBitsToReserve);
    }

    @Override
    @FinalDefault
    default Table join(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return join(rightTable, columnsToMatch, columnsToAdd, CrossJoinHelper.DEFAULT_NUM_RIGHT_BITS_TO_RESERVE);
    }

    @Override
    @FinalDefault
    default Table join(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return join(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd));
    }

    @Override
    @FinalDefault
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
    @FinalDefault
    default Table headBy(long nRows, Collection<String> groupByColumnNames) {
        return headBy(nRows, groupByColumnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }

    @Override
    @FinalDefault
    default Table tailBy(long nRows, Collection<String> groupByColumnNames) {
        return tailBy(nRows, groupByColumnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
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
    // Disaggregation Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @FinalDefault
    default Table ungroupAllBut(String... columnsNotToUngroup) {
        final Set<String> columnsNotToUnwrapSet = Arrays.stream(columnsNotToUngroup).collect(Collectors.toSet());
        return ungroup(getDefinition().getColumnStream()
                .filter(c -> !columnsNotToUnwrapSet.contains(c.getName())
                        && (c.getDataType().isArray() || QueryLanguageParser.isTypedVector(c.getDataType())))
                .map(ColumnDefinition::getName).toArray(String[]::new));
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
    default Table rollup(Collection<? extends Aggregation> aggregations) {
        return rollup(aggregations, Collections.emptyList());
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents) {
        return rollup(aggregations, includeConstituents, Collections.emptyList());
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table rollup(Collection<? extends Aggregation> aggregations, String... groupByColumns) {
        return rollup(aggregations, ColumnName.from(groupByColumns));
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents,
            String... groupByColumns) {
        return rollup(aggregations, includeConstituents, ColumnName.from(groupByColumns));
    }

    @Override
    @ConcurrentMethod
    @FinalDefault
    default Table rollup(Collection<? extends Aggregation> aggregations,
            Collection<? extends ColumnName> groupByColumns) {
        return rollup(aggregations, false, groupByColumns);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Snapshot Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @FinalDefault
    default Table snapshotIncremental(Table rightTable, String... stampColumns) {
        return snapshotIncremental(rightTable, false, stampColumns);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Merge Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @FinalDefault
    default Table mergeBefore(final Table... others) {
        final List<Table> tables = new ArrayList<>(others.length + 1);
        tables.add(this);
        tables.addAll(List.of(others));
        return TableTools.merge(tables);
    }

    @Override
    @FinalDefault
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
    @FinalDefault
    default Table withColumnDescription(String column, String description) {
        return withColumnDescription(Collections.singletonMap(column, description));
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

    @Override
    default boolean isFailed() {
        return false;
    }
}
