/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2;

import io.deephaven.api.*;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.base.Pair;
import io.deephaven.base.StringUtils;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.LayoutHintBuilder;
import io.deephaven.engine.table.iterators.*;
import io.deephaven.engine.tables.ColumnDefinition;
import io.deephaven.engine.tables.DataColumn;
import io.deephaven.engine.tables.SortPair;
import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.remote.AsyncMethod;
import io.deephaven.engine.tables.select.*;
import io.deephaven.engine.tables.utils.QueryPerformanceNugget;
import io.deephaven.engine.tables.utils.QueryPerformanceRecorder;
import io.deephaven.engine.time.DateTime;
import io.deephaven.engine.util.ColumnFormattingValues;
import io.deephaven.engine.util.liveness.LivenessScopeStack;
import io.deephaven.engine.v2.select.ReinterpretedColumn;
import io.deephaven.engine.v2.select.SelectColumn;
import io.deephaven.engine.vector.Vector;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Sub-interface to capture default methods rom {@link Table}.
 */
public interface TableWithDefaults extends Table {

    Table[] ZERO_LENGTH_TABLE_ARRAY = new Table[0];
    Filter[] ZERO_LENGTH_FILTER_ARRAY = new Filter[0];
    Selectable[] ZERO_LENGTH_SELECTABLE_ARRAY = new Selectable[0];

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
    @AsyncMethod
    default Table getMeta() {
        List<String> columnNames = new ArrayList<>();
        List<String> columnDataTypes = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();
        List<Boolean> columnPartitioning = new ArrayList<>();
        List<Boolean> columnGrouping = new ArrayList<>();
        for (ColumnDefinition<?> cDef : getDefinition().getColumns()) {
            columnNames.add(cDef.getName());
            columnDataTypes.add(cDef.getDataType().getName());
            columnTypes.add(ColumnDefinition.COLUMN_TYPE_FORMATTER.format(cDef.getColumnType()));
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
    @AsyncMethod
    default boolean hasColumns(final String... columnNames) {
        if (columnNames == null) {
            throw new IllegalArgumentException("columnNames cannot be null!");
        }
        return hasColumns(Arrays.asList(columnNames));
    }

    @Override
    @AsyncMethod
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
    @AsyncMethod
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
    default DataColumn getColumn(int columnIndex) {
        return getColumn(this.getDefinition().getColumns()[columnIndex].getName());
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Column Iterators
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default <TYPE> Iterator<TYPE> columnIterator(@NotNull final String columnName) {
        // noinspection rawtypes
        Iterator result;
        final Class<TYPE> type = getDefinition().<TYPE>getColumn(columnName).getDataType();
        if (type == byte.class || type == Byte.class) {
            result = byteColumnIterator(columnName);
        } else if (type == char.class || type == Character.class) {
            result = characterColumnIterator(columnName);
        } else if (type == double.class || type == Double.class) {
            result = doubleColumnIterator(columnName);
        } else if (type == float.class || type == Float.class) {
            result = floatColumnIterator(columnName);
        } else if (type == int.class || type == Integer.class) {
            result = integerColumnIterator(columnName);
        } else if (type == long.class || type == Long.class) {
            result = longColumnIterator(columnName);
        } else if (type == short.class || type == Short.class) {
            result = shortColumnIterator(columnName);
        } else {
            result = new ColumnIterator<>(this, columnName);
        }
        // noinspection unchecked
        return result;
    }

    @Override
    default ByteColumnIterator byteColumnIterator(@NotNull final String columnName) {
        return new ByteColumnIterator(this, columnName);
    }

    @Override
    default CharacterColumnIterator characterColumnIterator(@NotNull final String columnName) {
        return new CharacterColumnIterator(this, columnName);
    }

    @Override
    default DoubleColumnIterator doubleColumnIterator(@NotNull final String columnName) {
        return new DoubleColumnIterator(this, columnName);
    }

    @Override
    default FloatColumnIterator floatColumnIterator(@NotNull final String columnName) {
        return new FloatColumnIterator(this, columnName);
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
    default ShortColumnIterator shortColumnIterator(@NotNull final String columnName) {
        return new ShortColumnIterator(this, columnName);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Filter Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @AsyncMethod
    default Table where(String... filters) {
        return where(Filter.from(filters));
    }

    @Override
    @AsyncMethod
    default Table wouldMatch(String... expressions) {
        return wouldMatch(WouldMatchPairFactory.getExpressions(expressions));
    }

    @Override
    default Table whereIn(Table rightTable, String... columnsToMatch) {
        return whereIn(rightTable, JoinMatch.from(columnsToMatch));
    }

    @Override
    default Table whereNotIn(Table rightTable, String... columnsToMatch) {
        return whereNotIn(rightTable, JoinMatch.from(columnsToMatch));
    }

    @Override
    @AsyncMethod
    default Table whereOneOf(String... filtersToApply) {
        final FilterOr.Builder orBuilder = FilterOr.builder();
        for (final String filterToApply : filtersToApply) {
            orBuilder.addFilters(RawString.of(filterToApply));
        }
        return where(Collections.singleton(orBuilder.build()));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Column Selection Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default Table select(String... columns) {
        return select(Selectable.from(columns));
    }

    @Override
    default Table select() {
        return select(getDefinition().getColumnNamesArray());
    }

    @Override
    @AsyncMethod
    default Table selectDistinct(String... columns) {
        return selectDistinct(Selectable.from(columns));
    }

    @Override
    @AsyncMethod
    default Table selectDistinct() {
        return selectDistinct(getDefinition().getColumnNamesArray());
    }

    @Override
    default Table update(String... newColumns) {
        return update(Selectable.from((newColumns)));
    }

    @Override
    default Table lazyUpdate(String... newColumns) {
        return lazyUpdate(Selectable.from((newColumns)));
    }

    @Override
    @AsyncMethod
    default Table view(String... columns) {
        return view(Selectable.from(columns));
    }

    @Override
    @AsyncMethod
    default Table updateView(String... newColumns) {
        return updateView(Selectable.from((newColumns)));
    }

    @Override
    @AsyncMethod
    default Table dropColumns(Collection<String> columnNames) {
        return dropColumns(columnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }

    @Override
    @AsyncMethod
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
    @AsyncMethod
    default Table formatColumns(String... columnFormats) {
        final SelectColumn[] selectColumns = SelectColumnFactory.getFormatExpressions(columnFormats);

        final Set<String> existingColumns = getDefinition().getColumnNames()
                .stream()
                .filter(column -> !ColumnFormattingValues.isFormattingColumn(column))
                .collect(Collectors.toSet());

        final String[] unknownColumns = Arrays.stream(selectColumns)
                .map(SelectColumnFactory::getFormatBaseColumn)
                .filter(column -> (column != null && !column.equals("*") && !existingColumns.contains(column)))
                .toArray(String[]::new);

        if (unknownColumns.length > 0) {
            throw new RuntimeException(
                    "Unknown columns: " + Arrays.toString(unknownColumns) + ", available columns = " + existingColumns);
        }

        return updateView(selectColumns);
    }

    @Override
    @AsyncMethod
    default Table formatRowWhere(String condition, String formula) {
        return formatColumnWhere(ColumnFormattingValues.ROW_FORMAT_NAME, condition, formula);
    }

    @Override
    @AsyncMethod
    default Table formatColumnWhere(String columnName, String condition, String formula) {
        return formatColumns(
                columnName + " = (" + condition + ") ? io.deephaven.engine.util.ColorUtil.toLong(" + formula
                        + ") : io.deephaven.engine.util.ColorUtil.toLong(NO_FORMATTING)");
    }

    @Override
    @AsyncMethod
    default Table moveColumnsUp(String... columnsToMove) {
        return moveColumns(0, columnsToMove);
    }

    @Override
    @AsyncMethod
    default Table moveColumnsDown(String... columnsToMove) {
        return moveColumns(getDefinition().getColumns().length - columnsToMove.length, true, columnsToMove);
    }

    @Override
    @AsyncMethod
    default Table moveColumns(int index, String... columnsToMove) {
        return moveColumns(index, false, columnsToMove);
    }

    @Override
    @AsyncMethod
    default Table moveColumns(int index, boolean moveToEnd, String... columnsToMove) {
        // Get the current columns
        final List<String> currentColumns = getDefinition().getColumnNames();

        // Create a Set from columnsToMove. This way, we can rename and rearrange columns at once.
        final Set<String> leftColsToMove = new HashSet<>();
        final Set<String> rightColsToMove = new HashSet<>();
        int extraCols = 0;

        for (final String columnToMove : columnsToMove) {
            final String left = MatchPairFactory.getExpression(columnToMove).leftColumn;
            final String right = MatchPairFactory.getExpression(columnToMove).rightColumn;

            if (!leftColsToMove.add(left) || !currentColumns.contains(left) || (rightColsToMove.contains(left)
                    && !left.equals(right) && leftColsToMove.stream().anyMatch(col -> col.equals(right)))) {
                extraCols++;
            }
            if (currentColumns.stream().anyMatch(currentColumn -> currentColumn.equals(right)) && !left.equals(right)
                    && rightColsToMove.add(right) && !rightColsToMove.contains(left)) {
                extraCols--;
            }
        }
        index += moveToEnd ? extraCols : 0;

        // vci for write, cci for currentColumns, ctmi for columnsToMove
        final SelectColumn[] viewColumns = new SelectColumn[currentColumns.size() + extraCols];
        for (int vci = 0, cci = 0, ctmi = 0; vci < viewColumns.length;) {
            if (vci >= index && ctmi < columnsToMove.length) {
                viewColumns[vci++] = SelectColumnFactory.getExpression(columnsToMove[ctmi++]);
            } else {
                // Don't add the column if it's one of the columns we're moving or if it has been renamed.
                final String currentColumn = currentColumns.get(cci++);
                if (!leftColsToMove.contains(currentColumn)
                        && Arrays.stream(viewColumns).noneMatch(
                                viewCol -> viewCol != null && viewCol.getMatchPair().leftColumn.equals(currentColumn))
                        && Arrays.stream(columnsToMove)
                                .noneMatch(colToMove -> MatchPairFactory.getExpression(colToMove).rightColumn
                                        .equals(currentColumn))) {

                    viewColumns[vci++] = SelectColumnFactory.getExpression(currentColumn);
                }
            }
        }
        return view(viewColumns);
    }

    @Override
    @AsyncMethod
    default Table dateTimeColumnAsNanos(String dateTimeColumnName, String nanosColumnName) {
        return updateView(new ReinterpretedColumn<>(dateTimeColumnName, DateTime.class, nanosColumnName, long.class));
    }

    @Override
    @AsyncMethod
    default Table dateTimeColumnAsNanos(String columnName) {
        return dateTimeColumnAsNanos(columnName, columnName);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Join Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default Table leftJoin(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return leftJoin(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd));
    }

    @Override
    default Table leftJoin(Table rightTable, Collection<String> columnsToMatch) {
        return leftJoin(
                rightTable,
                MatchPairFactory.getExpressions(columnsToMatch),
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY);
    }

    @Override
    default Table leftJoin(Table rightTable, String columnsToMatch, String columnsToAdd) {
        return leftJoin(
                rightTable,
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToAdd)));
    }

    @Override
    default Table leftJoin(Table rightTable, String columnsToMatch) {
        return leftJoin(rightTable, StringUtils.splitToCollection(columnsToMatch));
    }

    @Override
    default Table leftJoin(Table rightTable) {
        return leftJoin(rightTable, Collections.emptyList());
    }

    @Override
    default Table exactJoin(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return exactJoin(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd));
    }

    @Override
    default Table exactJoin(Table rightTable, String columnsToMatch, String columnsToAdd) {
        return exactJoin(
                rightTable,
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToAdd)));
    }

    @Override
    default Table exactJoin(Table rightTable, String columnsToMatch) {
        return exactJoin(
                rightTable,
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY);
    }


    @Override
    default Table aj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return aj(rightTable, columnsToMatch, columnsToAdd, AsOfMatchRule.LESS_THAN_EQUAL);
    }

    @Override
    default Table aj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return aj(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd));
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
    default Table aj(Table rightTable, String columnsToMatch, String columnsToAdd) {
        Pair<MatchPair[], AsOfMatchRule> expressions =
                AjMatchPairFactory.getExpressions(false, StringUtils.splitToCollection(columnsToMatch));
        return aj(
                rightTable,
                expressions.getFirst(),
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToAdd)),
                expressions.getSecond());
    }

    @Override
    default Table aj(Table rightTable, String columnsToMatch) {
        return aj(rightTable, StringUtils.splitToCollection(columnsToMatch));
    }


    @Override
    default Table raj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return raj(rightTable, columnsToMatch, columnsToAdd, AsOfMatchRule.GREATER_THAN_EQUAL);
    }

    @Override
    default Table raj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return raj(
                rightTable,
                MatchPair.fromMatches(columnsToMatch),
                MatchPair.fromAddition(columnsToAdd));
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
    default Table raj(Table rightTable, String columnsToMatch, String columnsToAdd) {
        Pair<MatchPair[], AsOfMatchRule> expressions =
                AjMatchPairFactory.getExpressions(true, StringUtils.splitToCollection(columnsToMatch));
        return raj(
                rightTable,
                expressions.getFirst(),
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToAdd)),
                expressions.getSecond());
    }

    @Override
    default Table raj(Table rightTable, String columnsToMatch) {
        return raj(rightTable, StringUtils.splitToCollection(columnsToMatch));
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
    default Table naturalJoin(Table rightTable, String columnsToMatch, String columnsToAdd) {
        return naturalJoin(
                rightTable,
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToAdd)));
    }

    @Override
    default Table naturalJoin(Table rightTable, String columnsToMatch) {
        return naturalJoin(
                rightTable,
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY);
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
    default Table join(Table rightTable, String columnsToMatch) {
        return join(
                rightTable,
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY);
    }

    @Override
    default Table join(Table rightTable, String columnsToMatch, int numRightBitsToReserve) {
        return join(
                rightTable,
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
                MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
                numRightBitsToReserve);
    }

    @Override
    default Table join(Table rightTable, String columnsToMatch, String columnsToAdd) {
        return join(
                rightTable,
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToAdd)));
    }

    @Override
    default Table join(Table rightTable, String columnsToMatch, String columnsToAdd, int numRightBitsToReserve) {
        return join(
                rightTable,
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToMatch)),
                MatchPairFactory.getExpressions(StringUtils.splitToCollection(columnsToAdd)),
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
    @AsyncMethod
    default Table groupBy(Collection<? extends Selectable> groupByColumns) {
        return groupBy(SelectColumn.from(groupByColumns));
    }

    @Override
    @AsyncMethod
    default Table groupBy(String... groupByColumns) {
        return groupBy(Selectable.from(groupByColumns));
    }

    @Override
    @AsyncMethod
    default Table groupBy() {
        return groupBy(Collections.emptyList());
    }


    @Override
    @AsyncMethod
    default Table aggBy(Collection<? extends Aggregation> aggregations,
            Collection<? extends Selectable> groupByColumns) {
        return aggBy(aggregations, SelectColumn.from(groupByColumns));
    }

    @Override
    @AsyncMethod
    default Table aggBy(Collection<? extends Aggregation> aggregations, String... groupByColumns) {
        return aggBy(aggregations, Stream.of(groupByColumns).map(Selectable::parse).collect(Collectors.toList()));
    }

    @Override
    @AsyncMethod
    default Table aggBy(Collection<? extends Aggregation> aggregations) {
        return aggBy(aggregations, Collections.emptyList());
    }

    @Override
    default Table headBy(long nRows, Collection<String> groupByColumnNames) {
        return headBy(nRows, groupByColumnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }

    @Override
    default Table tailBy(long nRows, Collection<String> groupByColumnNames) {
        return tailBy(nRows, groupByColumnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table applyToAllBy(String formulaColumn, Selectable... groupByColumns) {
        return applyToAllBy(formulaColumn, "each", groupByColumns);
    }

    @Override
    @AsyncMethod
    default Table applyToAllBy(String formulaColumn, String... groupByColumns) {
        return applyToAllBy(formulaColumn, Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table applyToAllBy(String formulaColumn, String groupByColumn) {
        return applyToAllBy(formulaColumn, Selectable.from(groupByColumn).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table sumBy(String... groupByColumns) {
        return sumBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table sumBy(Collection<String> groupByColumns) {
        return sumBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table sumBy() {
        return sumBy(ZERO_LENGTH_SELECTABLE_ARRAY);
    }

    @Override
    @AsyncMethod
    default Table absSumBy(String... groupByColumns) {
        return absSumBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table absSumBy(Collection<String> groupByColumns) {
        return absSumBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table absSumBy() {
        return absSumBy(ZERO_LENGTH_SELECTABLE_ARRAY);
    }

    @Override
    @AsyncMethod
    default Table avgBy(String... groupByColumns) {
        return avgBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table avgBy(Collection<String> groupByColumns) {
        return avgBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table avgBy() {
        return avgBy(ZERO_LENGTH_SELECTABLE_ARRAY);
    }

    @Override
    @AsyncMethod
    default Table wavgBy(String weightColumn, String... groupByColumns) {
        return wavgBy(weightColumn, Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table wavgBy(String weightColumn, Collection<String> groupByColumns) {
        return wavgBy(weightColumn, Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table wavgBy(String weightColumn) {
        return wavgBy(weightColumn, ZERO_LENGTH_SELECTABLE_ARRAY);
    }

    @Override
    @AsyncMethod
    default Table wsumBy(String weightColumn) {
        return wsumBy(weightColumn, ZERO_LENGTH_SELECTABLE_ARRAY);
    }

    @Override
    @AsyncMethod
    default Table wsumBy(String weightColumn, String... groupByColumns) {
        return wsumBy(weightColumn, Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table wsumBy(String weightColumn, Collection<String> groupByColumns) {
        return wsumBy(weightColumn, Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table stdBy(String... groupByColumns) {
        return stdBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table stdBy(Collection<String> groupByColumns) {
        return stdBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    default Table stdBy() {
        return stdBy(ZERO_LENGTH_SELECTABLE_ARRAY);
    }

    @Override
    @AsyncMethod
    default Table varBy(String... groupByColumns) {
        return varBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table varBy(Collection<String> groupByColumns) {
        return varBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    default Table varBy() {
        return varBy(ZERO_LENGTH_SELECTABLE_ARRAY);
    }

    @Override
    @AsyncMethod
    default Table lastBy(String... groupByColumns) {
        return lastBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table lastBy(Collection<String> groupByColumns) {
        return lastBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table lastBy() {
        return lastBy(ZERO_LENGTH_SELECTABLE_ARRAY);
    }

    @Override
    @AsyncMethod
    default Table firstBy(String... groupByColumns) {
        return firstBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table firstBy(Collection<String> groupByColumns) {
        return firstBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table firstBy() {
        return firstBy(ZERO_LENGTH_SELECTABLE_ARRAY);
    }

    @Override
    @AsyncMethod
    default Table minBy(String... groupByColumns) {
        return minBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table minBy(Collection<String> groupByColumns) {
        return minBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table minBy() {
        return minBy(ZERO_LENGTH_SELECTABLE_ARRAY);
    }

    @Override
    @AsyncMethod
    default Table maxBy(String... groupByColumns) {
        return maxBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table maxBy(Collection<String> groupByColumns) {
        return maxBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table maxBy() {
        return maxBy(ZERO_LENGTH_SELECTABLE_ARRAY);
    }

    @Override
    @AsyncMethod
    default Table medianBy(String... groupByColumns) {
        return medianBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table medianBy(Collection<String> groupByColumns) {
        return medianBy(Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table medianBy() {
        return medianBy(ZERO_LENGTH_SELECTABLE_ARRAY);
    }

    @Override
    @AsyncMethod
    default Table countBy(String countColumnName, String... groupByColumns) {
        return countBy(countColumnName, Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table countBy(String countColumnName, Collection<String> groupByColumns) {
        return countBy(countColumnName, Selectable.from(groupByColumns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table countBy(String countColumnName) {
        return countBy(countColumnName, ZERO_LENGTH_SELECTABLE_ARRAY);
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
                        && (c.getDataType().isArray() || Vector.class.isAssignableFrom(c.getDataType())))
                .map(ColumnDefinition::getName).toArray(String[]::new));
    }

    @Override
    default Table ungroup() {
        return ungroup(getDefinition().getColumnStream()
                .filter(c -> c.getDataType().isArray() || Vector.class.isAssignableFrom(c.getDataType()))
                .map(ColumnDefinition::getName).toArray(String[]::new));
    }

    @Override
    default Table ungroup(boolean nullFill) {
        return ungroup(nullFill,
                getDefinition().getColumnStream()
                        .filter(c -> c.getDataType().isArray() || Vector.class.isAssignableFrom(c.getDataType()))
                        .map(ColumnDefinition::getName).toArray(String[]::new));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // PartitionBy Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @AsyncMethod
    default TableMap partitionBy(String... keyColumnNames) {
        return partitionBy(false, keyColumnNames);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Hierarchical table operations (rollup and treeTable).
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @AsyncMethod
    default Table rollup(Collection<? extends Aggregation> aggregations, Collection<String> columns) {
        return rollup(aggregations, Selectable.from(columns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents,
            Collection<String> columns) {
        return rollup(aggregations, includeConstituents,
                Selectable.from(columns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table rollup(Collection<? extends Aggregation> aggregations, String... columns) {
        return rollup(aggregations, Selectable.from(columns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents, String... columns) {
        return rollup(aggregations, includeConstituents,
                Selectable.from(columns).toArray(ZERO_LENGTH_SELECTABLE_ARRAY));
    }

    @Override
    @AsyncMethod
    default Table rollup(Collection<? extends Aggregation> aggregations, Selectable... columns) {
        return rollup(aggregations, false, columns);
    }

    @Override
    @AsyncMethod
    default Table rollup(Collection<? extends Aggregation> aggregations) {
        return rollup(aggregations, false, ZERO_LENGTH_SELECTABLE_ARRAY);
    }

    @Override
    @AsyncMethod
    default Table rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents) {
        return rollup(aggregations, includeConstituents, ZERO_LENGTH_SELECTABLE_ARRAY);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Sort Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @AsyncMethod
    default Table sort(String... columnsToSortBy) {
        return sort(SortPair.ascendingPairs(columnsToSortBy));
    }

    @Override
    @AsyncMethod
    default Table sortDescending(String... columnsToSortBy) {
        return sort(SortPair.descendingPairs(columnsToSortBy));
    }

    @Override
    @AsyncMethod
    default Table sort(Collection<SortColumn> columnsToSortBy) {
        return sort(SortPair.from(columnsToSortBy));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Snapshot Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    default Table snapshot(Table baseTable, String... stampColumns) {
        return snapshot(baseTable, true, stampColumns);
    }

    @Override
    default Table snapshotIncremental(Table rightTable, String... stampColumns) {
        return snapshotIncremental(rightTable, false, stampColumns);
    }

    @Override
    default Table snapshot(Table baseTable, boolean doInitialSnapshot, Collection<ColumnName> stampColumns) {
        return snapshot(baseTable, doInitialSnapshot,
                stampColumns.stream().map(ColumnName::name).toArray(String[]::new));
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
    @AsyncMethod
    default Table layoutHints(LayoutHintBuilder builder) {
        return layoutHints(builder.build());
    }

    @Override
    @AsyncMethod
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
