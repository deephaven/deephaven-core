/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table;

import io.deephaven.api.*;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.primitive.iterator.*;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.api.util.ConcurrentMethod;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.util.systemicmarking.SystemicObject;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * A Deephaven table.
 */
public interface Table extends
        LongSizedDataStructure,
        LivenessNode,
        NotificationQueue.Dependency,
        DynamicNode,
        SystemicObject<Table>,
        TableOperations<Table, Table>,
        AttributeMap<Table>,
        GridAttributes<Table> {

    // -----------------------------------------------------------------------------------------------------------------
    // Metadata
    // -----------------------------------------------------------------------------------------------------------------

    @ConcurrentMethod
    TableDefinition getDefinition();

    /**
     * Provides column metadata in Table form. Convenience method, behaves exactly the same as
     * getDefinition().getColumnDefinitionsTable().
     *
     * @return A Table of metadata about this Table's columns.
     */
    @ConcurrentMethod
    Table getMeta();

    @ConcurrentMethod
    String getDescription();

    /**
     * Get the number of columns defined for this table. Equivalent to {@code getDefinition().getColumns().length}.
     *
     * @return The number of columns defined for this table
     */
    @ConcurrentMethod
    int numColumns();

    /**
     * Determines whether this Table contains a column for each string in the specified array of {@code columnNames}.
     *
     * @param columnNames The array of column names to be checked for inclusion in this table. Must not be {@code null}.
     * @return {@code true} if this Table contains a column for each and every string in the {@code columnNames} array;
     *         {@code false} if any element of {@code columnNames} is <b>not</b> the name of a column in this table
     */
    @ConcurrentMethod
    boolean hasColumns(String... columnNames);

    /**
     * Determines whether this Table contains a column for each string in the specified collection of
     * {@code columnNames}.
     *
     * @param columnNames The collection of column names to be checked for inclusion in this table. Must not be
     *        {@code null}.
     * @return {@code true} if this Table contains a column for each and every string in the {@code columnNames}
     *         collection; {@code false} if any element of {@code columnNames} is <b>not</b> the name of a column in
     *         this table
     */
    @ConcurrentMethod
    boolean hasColumns(Collection<String> columnNames);

    @Override
    @ConcurrentMethod
    boolean isRefreshing();

    /**
     * @return The {@link TrackingRowSet} that exposes the row keys present in this Table
     */
    TrackingRowSet getRowSet();

    /**
     * @return {@link #size() Size} if it is currently known without subsequent steps to coalesce the Table, else
     *         {@link io.deephaven.util.QueryConstants#NULL_LONG null}
     */
    long sizeForInstrumentation();

    /**
     * Returns {@code true} if this table has no rows (i.e. {@code size() == 0}).
     *
     * @return {@code true} if this table has no rows
     */
    boolean isEmpty();

    /**
     * Return true if this table is guaranteed to be flat. The RowSet of a flat table will be from 0...numRows-1.
     */
    @ConcurrentMethod
    boolean isFlat();

    // -----------------------------------------------------------------------------------------------------------------
    // Attributes
    // -----------------------------------------------------------------------------------------------------------------

    String INPUT_TABLE_ATTRIBUTE = "InputTable";
    String KEY_COLUMNS_ATTRIBUTE = "keyColumns";
    String UNIQUE_KEYS_ATTRIBUTE = "uniqueKeys";
    String FILTERABLE_COLUMNS_ATTRIBUTE = "FilterableColumns";
    String TOTALS_TABLE_ATTRIBUTE = "TotalsTable";
    String ADD_ONLY_TABLE_ATTRIBUTE = "AddOnly";
    String APPEND_ONLY_TABLE_ATTRIBUTE = "AppendOnly";
    String TEST_SOURCE_TABLE_ATTRIBUTE = "TestSource";
    /**
     * <p>
     * If this attribute is present with value {@code true}, this Table is a "stream table".
     * <p>
     * A stream table is a sequence of additions that represent rows newly received from a stream; on the cycle after
     * the stream table is refreshed the rows are removed. Note that this means any particular row of data (not to be
     * confused with a row key) never exists for more than one cycle.
     * <p>
     * Most operations are supported as normal on stream tables, but aggregation operations are treated specially,
     * producing aggregate results that are valid over the entire observed stream from the time the operation is
     * initiated. These semantics necessitate a few exclusions, i.e. unsupported operations that need to keep track of
     * all rows:
     * <ol>
     * <li>{@link #groupBy} is unsupported
     * <li>{@link #partitionBy} is unsupported</li>
     * <li>{@link #partitionedAggBy(Collection, boolean, Table, String...) partitionedAggBy} is unsupported</li>
     * <li>{@link #aggBy} is unsupported if either of {@link io.deephaven.api.agg.spec.AggSpecGroup group} or
     * {@link io.deephaven.api.agg.Partition partition} are used</li>
     * <li>{@link #rollup(Collection, boolean, Collection) rollup()} is unsupported if
     * {@code includeConstituents == true}</li>
     * <li>{@link #tree(String, String) tree()} is unsupported</li>
     * </ol>
     * <p>
     * To disable these semantics, a {@link #dropStream() dropStream} method is offered.
     */
    String STREAM_TABLE_ATTRIBUTE = "StreamTable";
    /**
     * The query engine may set or read this attribute to determine if a table is sorted by a particular column.
     */
    String SORTED_COLUMNS_ATTRIBUTE = "SortedColumns";
    String SYSTEMIC_TABLE_ATTRIBUTE = "SystemicTable";
    /**
     * Attribute on aggregation results used for hierarchical table construction. Specification is left to the
     * implementation.
     */
    String AGGREGATION_ROW_LOOKUP_ATTRIBUTE = "AggregationRowLookup";
    /**
     * Attribute on sort results used for hierarchical table construction. Specification is left to the implementation.
     */
    String SORT_REVERSE_LOOKUP_ATTRIBUTE = "SortReverseLookup";
    String SNAPSHOT_VIEWPORT_TYPE = "Snapshot";
    /**
     * This attribute is used internally by TableTools.merge to detect successive merges. Its presence indicates that it
     * is safe to decompose the table into its multiple constituent parts.
     */
    String MERGED_TABLE_ATTRIBUTE = "MergedTable";
    /**
     * <p>
     * This attribute is applied to the descendants of source tables, and takes on Boolean values.
     * <ul>
     * <li>True for the result of {@link #coalesce()} on source tables and their children if the source table was
     * initially empty on coalesce.</li>
     * <li>Missing for all other tables.</li>
     * </ul>
     * This effectively serves as a hint that filters may have been poorly selected, resulting in an empty result. If
     * {@code size() > 0}, this hint should be disregarded.
     */
    String INITIALLY_EMPTY_COALESCED_SOURCE_TABLE_ATTRIBUTE = "EmptySourceTable";
    /**
     * This attribute stores a reference to a table that is the parent table for a Preview Table.
     */
    String PREVIEW_PARENT_TABLE = "PreviewParentTable";
    /**
     * Set this attribute for tables that should not be displayed in the UI.
     */
    String NON_DISPLAY_TABLE = "NonDisplayTable";
    /**
     * Set this attribute to load a plugin for this table in the Web Client
     */
    String PLUGIN_NAME = "PluginName";
    /**
     * Set this attribute to enable collection of barrage performance stats.
     */
    String BARRAGE_PERFORMANCE_KEY_ATTRIBUTE = "BarragePerformanceTableKey";

    // -----------------------------------------------------------------------------------------------------------------
    // ColumnSources for fetching data by row key
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Retrieves a {@code ColumnSource}. It is conveniently cast to @{code ColumnSource<T>} using the type that caller
     * expects. This differs from {@link #getColumnSource(String, Class)} which uses the provided {@link Class} object
     * to verify that the data type is a subclass of the expected class.
     *
     * @param sourceName The name of the column
     * @param <T> The target type, as a type parameter. Inferred from context.
     * @return The column source for {@code sourceName}, parameterized by {@code T}
     */
    <T> ColumnSource<T> getColumnSource(String sourceName);

    /**
     * Retrieves a {@code ColumnSource} and {@link ColumnSource#cast casts} it to the target class {@code clazz}.
     *
     * @param sourceName The name of the column
     * @param clazz The target type
     * @param <T> The target type, as a type parameter. Intended to be inferred from {@code clazz}.
     * @return The column source for {@code sourceName}, parameterized by {@code T}
     */
    <T> ColumnSource<T> getColumnSource(String sourceName, Class<? extends T> clazz);

    Map<String, ? extends ColumnSource<?>> getColumnSourceMap();

    Collection<? extends ColumnSource<?>> getColumnSources();

    // -----------------------------------------------------------------------------------------------------------------
    // DataColumns for fetching data by row position; generally much less efficient than ColumnSource
    // -----------------------------------------------------------------------------------------------------------------

    DataColumn[] getColumns();

    DataColumn getColumn(int columnIndex);

    DataColumn getColumn(String columnName);

    // -----------------------------------------------------------------------------------------------------------------
    // Column Iterators
    // -----------------------------------------------------------------------------------------------------------------

    <DATA_TYPE> CloseableIterator<DATA_TYPE> columnIterator(@NotNull String columnName);

    CloseablePrimitiveIteratorOfChar characterColumnIterator(@NotNull String columnName);

    CloseablePrimitiveIteratorOfByte byteColumnIterator(@NotNull String columnName);

    CloseablePrimitiveIteratorOfShort shortColumnIterator(@NotNull String columnName);

    CloseablePrimitiveIteratorOfInt integerColumnIterator(@NotNull String columnName);

    CloseablePrimitiveIteratorOfLong longColumnIterator(@NotNull String columnName);

    CloseablePrimitiveIteratorOfFloat floatColumnIterator(@NotNull String columnName);

    CloseablePrimitiveIteratorOfDouble doubleColumnIterator(@NotNull String columnName);

    <DATA_TYPE> CloseableIterator<DATA_TYPE> objectColumnIterator(@NotNull String columnName);

    // -----------------------------------------------------------------------------------------------------------------
    // Convenience data fetching; highly inefficient
    // -----------------------------------------------------------------------------------------------------------------

    Object[] getRecord(long rowNo, String... columnNames);

    // -----------------------------------------------------------------------------------------------------------------
    // Filter Operations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Filters rows of data from the source table.
     *
     * @param filters formula(e) for filtering. Filters can be a match filter or a conditional filter.
     * @return a new table with only the rows from the source table that meet the filter criteria.
     */
    @ConcurrentMethod
    Table where(Filter... filters);

    /**
     * A table operation that applies the supplied predicate to each row in the table and produces columns containing
     * the pass/fail result of the predicate application. This is similar to {@link #where(String...)} except that
     * instead of selecting only rows that meet the criteria, new columns are added with the result of the comparison.
     *
     * @param matchers a pair of either a column name and a filter, or a column name and a String expression
     * @return a table with new columns containing the filter result for each row.
     */
    @ConcurrentMethod
    Table wouldMatch(WouldMatchPair... matchers);

    /**
     * A table operation that applies the supplied predicate to each row in the table and produces columns containing
     * the pass/fail result of the predicate application. This is similar to {@link #where(String...)} except that
     * instead of selecting only rows that meet the criteria, new columns are added with the result of the comparison.
     *
     * @param expressions one or more strings, each of which performs an assignment and a truth check,
     *                    e.g., "NewColumnName = ExistingColumnName == 3" or "XMoreThan5 = X > 5"
     * @return a new table with boolean column that contains values indicating whether each row would match the filter's criteria.
     */
    @ConcurrentMethod
    Table wouldMatch(String... expressions);

    // -----------------------------------------------------------------------------------------------------------------
    // Column Selection Operations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Creates a new in-memory table that includes one column for each argument.
     *
     * @param columns the formulas to compute columns in the new table. Column from table: "A" (equivalent to "A = A").
     *                Renamed column from table: "X = A". Calculated column: "X = A * sqrt(B)"
     * @return a new in-memory table that includes on column for each argument.
     */
    Table select(Selectable... columns);

    /**
     * Creates a new in-memory table that includes one column for each column in the source table.
     *
     * @return a new in-memory table that includes one column for each column in the source table.
     */
    Table select();

    /**
     * Creates a new table containing a new, in-memory column for each argument.
     *
     * @param newColumns formulas to compute columns in the new table, e.g., "X = A * sqrt(B)"
     * @return a new table that includes all the original columns from the source table and all the newly defined columns.
     */
    Table update(Selectable... newColumns);

    /**
     * Creates a new, cached formula column for each argument.
     *
     * @param newColumns formulas to compute columns in the new table, e.g., "X = A * sqrt(B)"
     * @return a new table that includes all the original columns from the source table and all the newly defined columns.
     */
    Table lazyUpdate(Selectable... newColumns);

    /**
     * Creates a new formula table that includes one column for each argument.
     *
     * @param columns formulas to compute columns in the new table. Column from source: "X" (equivalent to "X = X").
     *                Renamed column from source: "X = Y". Calculated column: "X = A * sqrt(B)"
     * @return a new formula table that includes one column for each argument
     */
    @ConcurrentMethod
    Table view(Selectable... columns);

    /**
     * Creates a new table containing a new formula column for each argument. When using updateView, the new columns
     * are not stored in memory. Rather, a formula is stored that is used to recalculate each cell every time it
     * is accessed.
     *
     * @param newColumns formulas to compute columns in the new table, e.g., "X = A * sqrt(B)".
     * @return a new table that includes all columns from the source table and the newly defined formula columns
     */
    @ConcurrentMethod
    Table updateView(Selectable... newColumns);

    /**
     * Removes formatting from all the columns in a table.
     *
     * @return a new table with all the rows and columns from the source table, but without the original formatting.
     */
    @ConcurrentMethod
    Table dropColumnFormats();

    /**
     * Creates a new table with the specified columns renamed.
     *
     * @param pairs the column-rename expressions, e.g., "X = Y"
     * @return a new table with the specified columns renamed.
     */
    Table renameColumns(MatchPair... pairs);

    /**
     * Creates a new table with the specified columns renamed.
     *
     * @param columns the column-rename expressions, e.g., "X = Y"
     * @return a new table with the specified columns renamed
     */
    Table renameColumns(Collection<String> columns);

    /**
     * Creates a new table with the specified columns renamed.
     *
     * @param columns the column-rename expressions, e.g., "X = Y"
     * @return a new table with specified columns renamed
     */
    Table renameColumns(String... columns);

    Table renameAllColumns(UnaryOperator<String> renameFunction);

    /**
     * Creates a table containing a new formula column, which defines a column format for each argument.
     *
     * @param columnFormats Formulas to compute formats for columns or rows in the table; e.g., "X = Y > 5 ? RED : NO_FORMATTING".
     *                      For color formats, the result of each formula must be either a color string
     *                      (such as a hexadecimal RGB color, e.g., "#040427"), a Color, or a packed long representation
     *                      of the background and foreground color as returned by "bgfg()" or "bgfga()").
     *                      For decimal formats, the result must be a string, and the formula must be wrapped in the
     *                      special internal function "Decimal()", e.g., "X = Decimal(`$#,##0.00`)".
     * @return a new table containing all the original columns from the source table and the newly defined format columns.
     */
    @ConcurrentMethod
    Table formatColumns(String... columnFormats);

    /**
     * Formats an entire row when a specified condition exists.
     *
     * @param condition the condition under which rows will be reformatted, e.g., "Value > 50".
     * @param formula the formula to apply when the condition exists, e.g., "VIVID_PURPLE".
     * @return a new table where rows that meet the supplied condition have the formula applied to them.
     */
    @ConcurrentMethod
    Table formatRowWhere(String condition, String formula);

    /**
     * Applies a formula to an entire column when the supplied condition is met.
     *
     * @param columnName the name of the column to apply the formula to.
     * @param condition the condition under which rows will be reformatted, e.g., "Value > 50".
     * @param formula the formula to apply when the condition exists, e.g., "VIVID_PURPLE".
     * @return a new table where columns that meet the supplied condition have the formula applied to them.
     */
    @ConcurrentMethod
    Table formatColumnWhere(String columnName, String condition, String formula);

    /**
     * Produce a new table with the specified columns moved to the leftmost position. Columns can be renamed with the
     * usual syntax, i.e. {@code "NewColumnName=OldColumnName")}.
     *
     * @param columnsToMove The columns to move to the left (and, optionally, to rename)
     * @return The new table, with the columns rearranged as explained above {@link #moveColumns(int, String...)}
     */
    @ConcurrentMethod
    Table moveColumnsUp(String... columnsToMove);

    /**
     * Produce a new table with the specified columns moved to the rightmost position. Columns can be renamed with the
     * usual syntax, i.e. {@code "NewColumnName=OldColumnName")}.
     *
     * @param columnsToMove The columns to move to the right (and, optionally, to rename)
     * @return The new table, with the columns rearranged as explained above {@link #moveColumns(int, String...)}
     */
    @ConcurrentMethod
    Table moveColumnsDown(String... columnsToMove);

    /**
     * Produce a new table with the specified columns moved to the specified {@code index}. Column indices begin at 0.
     * Columns can be renamed with the usual syntax, i.e. {@code "NewColumnName=OldColumnName")}.
     *
     * @param index The index to which the specified columns should be moved
     * @param columnsToMove The columns to move to the specified index (and, optionally, to rename)
     * @return The new table, with the columns rearranged as explained above
     */
    @ConcurrentMethod
    Table moveColumns(int index, String... columnsToMove);

    /**
     * Produce a new table with the specified columns moved to the specified {@code index}. Column indices begin at 0.
     * Columns can be renamed with the usual syntax, i.e. {@code "NewColumnName=OldColumnName")}.
     *
     * @param index the index to which the specified columns should be moved
     * @param moveToEnd whether to move the specified columns to the end of the table
     * @param columnsToMove the names of the columns to be moved
     * @return a new table with specified columns moved to the specified column index value
     */
    @ConcurrentMethod
    Table moveColumns(int index, boolean moveToEnd, String... columnsToMove);

    /**
     * Produce a new table with the same columns as this table, but with a new column presenting the specified DateTime
     * column as a Long column (with each DateTime represented instead as the corresponding number of nanos since the
     * epoch).
     * <p>
     * NOTE: This is a really just an updateView(), and behaves accordingly for column ordering and (re)placement. This
     * doesn't work on data that has been brought fully into memory (e.g. via select()). Use a view instead.
     *
     * @param dateTimeColumnName Name of date time column
     * @param nanosColumnName Name of nanos column
     * @return The new table, constructed as explained above.
     */
    @ConcurrentMethod
    Table dateTimeColumnAsNanos(String dateTimeColumnName, String nanosColumnName);

    /**
     * @param columnName name of column to convert from DateTime to nanos
     * @return The result of dateTimeColumnAsNanos(columnName, columnName).
     */
    @ConcurrentMethod
    Table dateTimeColumnAsNanos(String columnName);

    // -----------------------------------------------------------------------------------------------------------------
    // Slice Operations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Extracts a subset of a table by row position.
     * <p>
     * If both firstPosition and lastPosition are positive, then the rows are counted from the beginning of the table.
     * The firstPosition is inclusive, and the lastPosition is exclusive. The {@link #head}(N) call is equivalent to
     * slice(0, N). The firstPosition must be less than or equal to the lastPosition.
     * <p>
     * If firstPosition is positive and lastPosition is negative, then the firstRow is counted from the beginning of the
     * table, inclusively. The lastPosition is counted from the end of the table. For example, slice(1, -1) includes all
     * rows but the first and last. If the lastPosition would be before the firstRow, the result is an emptyTable.
     * <p>
     * If firstPosition is negative, and lastPosition is zero, then the firstRow is counted from the end of the table,
     * and the end of the slice is the size of the table. slice(-N, 0) is equivalent to {@link #tail}(N).
     * <p>
     * If the firstPosition is negative and the lastPosition is negative, they are both counted from the end of the
     * table. For example, slice(-2, -1) returns the second to last row of the table.
     *
     * @param firstPositionInclusive the first position to include in the result
     * @param lastPositionExclusive the last position to include in the result
     * @return a new Table, which is the request subset of rows from the original table
     */
    @ConcurrentMethod
    Table slice(long firstPositionInclusive, long lastPositionExclusive);

    /**
     * Provides a head that selects a dynamic number of rows based on a percent.
     *
     * @param percent the fraction of the table to return (0..1), the number of rows will be rounded up. For example if
     *        there are 3 rows, headPct(50) returns the first two rows.
     */
    @ConcurrentMethod
    Table headPct(double percent);

    /**
     * Provides a table with a specific percentage of rows from the end of the source table.
     *
     * @param percent the percentage of rows to return, given as a value from 0.0(0%) to 1.0(100%).
     * @return a new table with a specific percentage of rows from the end of the table.
     */
    @ConcurrentMethod
    Table tailPct(double percent);

    // -----------------------------------------------------------------------------------------------------------------
    // Join Operations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Perform an exact-join with the rightTable.
     *
     * @param rightTable the right side table on the join
     * @param columnsToMatch a comma-separated list of match conditions ("leftColumn=rightColumn" or "columnFoundInBoth")
     * @param columnsToAdd the columns from the right side that need to be added to the left side as a
     *                     result of the match
     * @return the exact-joined table
     */
    Table exactJoin(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd);

    /**
     * Rules for the inexact matching performed on the final column to match by in {@link #aj} and {@link #raj}.
     */
    enum AsOfMatchRule {
        LESS_THAN_EQUAL, LESS_THAN, GREATER_THAN_EQUAL, GREATER_THAN;

        public static AsOfMatchRule of(AsOfJoinRule rule) {
            switch (rule) {
                case LESS_THAN_EQUAL:
                    return Table.AsOfMatchRule.LESS_THAN_EQUAL;
                case LESS_THAN:
                    return Table.AsOfMatchRule.LESS_THAN;
            }
            throw new IllegalStateException("Unexpected rule " + rule);
        }

        public static AsOfMatchRule of(ReverseAsOfJoinRule rule) {
            switch (rule) {
                case GREATER_THAN_EQUAL:
                    return Table.AsOfMatchRule.GREATER_THAN_EQUAL;
                case GREATER_THAN:
                    return Table.AsOfMatchRule.GREATER_THAN;
            }
            throw new IllegalStateException("Unexpected rule " + rule);
        }
    }

    /**
     * Looks up the columns in the rightTable that meet the match conditions in the columnsToMatch list. Matching is
     * done exactly for the first n-1 columns and via a binary search for the last match pair. The columns of the
     * original table are returned intact, together with the columns from rightTable defined in a comma separated list
     * "columnsToAdd"
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth")
     * @param columnsToAdd A comma separated list with the columns from the left side that need to be added to the right
     *        side as a result of the match.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    Table aj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd, AsOfMatchRule asOfMatchRule);

    /**
     * Looks up the columns in the rightTable that meet the match conditions in the columnsToMatch list. Matching is
     * done exactly for the first n-1 columns and via a binary search for the last match pair. The columns of the
     * original table are returned intact, together with the columns from rightTable defined in a comma separated list
     * "columnsToAdd"
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth")
     * @param columnsToAdd A comma separated list with the columns from the left side that need to be added to the right
     *        side as a result of the match.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    Table aj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd);

    /**
     * Looks up the columns in the rightTable that meet the match conditions in the columnsToMatch list. Matching is
     * done exactly for the first n-1 columns and via a binary search for the last match pair. The columns of the
     * original table are returned intact, together with all the columns from rightTable.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth")
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    Table aj(Table rightTable, Collection<String> columnsToMatch);

    /**
     * Just like .aj(), but the matching on the last column is in reverse order, so that you find the row after the
     * given timestamp instead of the row before.
     * <p>
     * Looks up the columns in the rightTable that meet the match conditions in the columnsToMatch list. Matching is
     * done exactly for the first n-1 columns and via a binary search for the last match pair. The columns of the
     * original table are returned intact, together with the columns from rightTable defined in a comma separated list
     * "columnsToAdd"
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth")
     * @param columnsToAdd A comma separated list with the columns from the left side that need to be added to the right
     *        side as a result of the match.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    Table raj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd, AsOfMatchRule asOfMatchRule);

    /**
     * Just like .aj(), but the matching on the last column is in reverse order, so that you find the row after the
     * given timestamp instead of the row before.
     * <p>
     * Looks up the columns in the rightTable that meet the match conditions in the columnsToMatch list. Matching is
     * done exactly for the first n-1 columns and via a binary search for the last match pair. The columns of the
     * original table are returned intact, together with the columns from rightTable defined in a comma separated list
     * "columnsToAdd"
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth")
     * @param columnsToAdd A comma separated list with the columns from the left side that need to be added to the right
     *        side as a result of the match.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    Table raj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd);

    /**
     * Just like .aj(), but the matching on the last column is in reverse order, so that you find the row after the
     * given timestamp instead of the row before.
     * <p>
     * Looks up the columns in the rightTable that meet the match conditions in the columnsToMatch list. Matching is
     * done exactly for the first n-1 columns and via a binary search for the last match pair. The columns of the
     * original table are returned intact, together with the all columns from rightTable.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth")
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    Table raj(Table rightTable, Collection<String> columnsToMatch);

    /**
     * Perform a natural join with the rightTable.
     *
     * @param rightTable the right side table on the join
     * @param columnsToMatch a comma-separated list of match conditions ("leftColumn=rightColumn" or "columnFoundInBoth")
     * @param columnsToAdd a comma-separated list with the columns from the right side that need to be added
     *                     to the left side as a result of the match.
     * @return the natural-joined table
     */
    Table naturalJoin(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd);

    /**
     * Perform a cross join with the right table.
     * <p>
     * Returns a table that is the cartesian product of left rows X right rows, with one column for each of the left
     * table's columns, and one column corresponding to each of the right table's columns. The rows are ordered first by
     * the left table then by the right table.
     * <p>
     * To efficiently produce updates, the bits that represent a key for a given row are split into two. Unless
     * specified, join reserves 16 bits to represent a right row. When there are too few bits to represent all of the
     * right rows for a given aggregation group the table will shift a bit from the left side to the right side. The
     * default of 16 bits was carefully chosen because it results in an efficient implementation to process live
     * updates.
     * <p>
     * An {@link io.deephaven.engine.exceptions.OutOfKeySpaceException} is thrown when the total number of bits needed
     * to express the result table exceeds that needed to represent Long.MAX_VALUE. There are a few work arounds: - If
     * the left table is sparse, consider flattening the left table. - If there are no key-columns and the right table
     * is sparse, consider flattening the right table. - If the maximum size of a right table's group is small, you can
     * reserve fewer bits by setting numRightBitsToReserve on initialization.
     * <p>
     * Note: If you can prove that a given group has at most one right-row then you should prefer using
     * {@link #naturalJoin}.
     *
     * @param rightTable The right side table on the join.
     * @return a new table joined according to the specification with zero key-columns and includes all right columns
     */
    Table join(Table rightTable);

    /**
     * Perform a cross join with the right table.
     * <p>
     * Returns a table that is the cartesian product of left rows X right rows, with one column for each of the left
     * table's columns, and one column corresponding to each of the right table's columns. The rows are ordered first by
     * the left table then by the right table.
     * <p>
     * To efficiently produce updates, the bits that represent a key for a given row are split into two. Unless
     * specified, join reserves 16 bits to represent a right row. When there are too few bits to represent all of the
     * right rows for a given aggregation group the table will shift a bit from the left side to the right side. The
     * default of 16 bits was carefully chosen because it results in an efficient implementation to process live
     * updates.
     * <p>
     * An {@link io.deephaven.engine.exceptions.OutOfKeySpaceException} is thrown when the total number of bits needed
     * to express the result table exceeds that needed to represent Long.MAX_VALUE. There are a few work arounds: - If
     * the left table is sparse, consider flattening the left table. - If there are no key-columns and the right table
     * is sparse, consider flattening the right table. - If the maximum size of a right table's group is small, you can
     * reserve fewer bits by setting numRightBitsToReserve on initialization.
     * <p>
     * Note: If you can prove that a given group has at most one right-row then you should prefer using
     * {@link #naturalJoin}.
     *
     * @param rightTable The right side table on the join.
     * @param numRightBitsToReserve The number of bits to reserve for rightTable groups.
     * @return a new table joined according to the specification with zero key-columns and includes all right columns
     */
    Table join(Table rightTable, int numRightBitsToReserve);

    /**
     * Perform a cross join with the right table.
     * <p>
     * Returns a table that is the cartesian product of left rows X right rows, with one column for each of the left
     * table's columns, and one column corresponding to each of the right table's columns that are not key-columns. The
     * rows are ordered first by the left table then by the right table. If columnsToMatch is non-empty then the product
     * is filtered by the supplied match conditions.
     * <p>
     * To efficiently produce updates, the bits that represent a key for a given row are split into two. Unless
     * specified, join reserves 16 bits to represent a right row. When there are too few bits to represent all of the
     * right rows for a given aggregation group the table will shift a bit from the left side to the right side. The
     * default of 16 bits was carefully chosen because it results in an efficient implementation to process live
     * updates.
     * <p>
     * An {@link io.deephaven.engine.exceptions.OutOfKeySpaceException} is thrown when the total number of bits needed
     * to express the result table exceeds that needed to represent Long.MAX_VALUE. There are a few work arounds: - If
     * the left table is sparse, consider flattening the left table. - If there are no key-columns and the right table
     * is sparse, consider flattening the right table. - If the maximum size of a right table's group is small, you can
     * reserve fewer bits by setting numRightBitsToReserve on initialization.
     * <p>
     * Note: If you can prove that a given group has at most one right-row then you should prefer using
     * {@link #naturalJoin}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth")
     * @param numRightBitsToReserve The number of bits to reserve for rightTable groups.
     * @return a new table joined according to the specification in columnsToMatch and includes all non-key-columns from
     *         the right table
     */
    Table join(Table rightTable, String columnsToMatch, int numRightBitsToReserve);

    /**
     * Perform a cross join with the right table.
     * <p>
     * Returns a table that is the cartesian product of left rows X right rows, with one column for each of the left
     * table's columns, and one column corresponding to each of the right table's columns that are included in the
     * columnsToAdd argument. The rows are ordered first by the left table then by the right table. If columnsToMatch is
     * non-empty then the product is filtered by the supplied match conditions.
     * <p>
     * To efficiently produce updates, the bits that represent a key for a given row are split into two. Unless
     * specified, join reserves 16 bits to represent a right row. When there are too few bits to represent all of the
     * right rows for a given aggregation group the table will shift a bit from the left side to the right side. The
     * default of 16 bits was carefully chosen because it results in an efficient implementation to process live
     * updates.
     * <p>
     * An {@link io.deephaven.engine.exceptions.OutOfKeySpaceException} is thrown when the total number of bits needed
     * to express the result table exceeds that needed to represent Long.MAX_VALUE. There are a few work arounds: - If
     * the left table is sparse, consider flattening the left table. - If there are no key-columns and the right table
     * is sparse, consider flattening the right table. - If the maximum size of a right table's group is small, you can
     * reserve fewer bits by setting numRightBitsToReserve on initialization.
     * <p>
     * Note: If you can prove that a given group has at most one right-row then you should prefer using
     * {@link #naturalJoin}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth")
     * @param columnsToAdd A comma separated list with the columns from the right side that need to be added to the left
     *        side as a result of the match.
     * @param numRightBitsToReserve The number of bits to reserve for rightTable groups.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    Table join(Table rightTable, String columnsToMatch, String columnsToAdd, int numRightBitsToReserve);

    /**
     * Perform a cross join with the right table.
     * <p>
     * Returns a table that is the cartesian product of left rows X right rows, with one column for each of the left
     * table's columns, and one column corresponding to each of the right table's columns that are included in the
     * columnsToAdd argument. The rows are ordered first by the left table then by the right table. If columnsToMatch is
     * non-empty then the product is filtered by the supplied match conditions.
     * <p>
     * To efficiently produce updates, the bits that represent a key for a given row are split into two. Unless
     * specified, join reserves 16 bits to represent a right row. When there are too few bits to represent all of the
     * right rows for a given aggregation group the table will shift a bit from the left side to the right side. The
     * default of 16 bits was carefully chosen because it results in an efficient implementation to process live
     * updates.
     * <p>
     * An {@link io.deephaven.engine.exceptions.OutOfKeySpaceException} is thrown when the total number of bits needed
     * to express the result table exceeds that needed to represent Long.MAX_VALUE. There are a few work arounds: - If
     * the left table is sparse, consider flattening the left table. - If there are no key-columns and the right table
     * is sparse, consider flattening the right table. - If the maximum size of a right table's group is small, you can
     * reserve fewer bits by setting numRightBitsToReserve on initialization.
     * <p>
     * Note: If you can prove that a given group has at most one right-row then you should prefer using
     * {@link #naturalJoin}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch An array of match pair conditions ("leftColumn=rightColumn" or "columnFoundInBoth")
     * @param columnsToAdd An array of the columns from the right side that need to be added to the left side as a
     *        result of the match.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    Table join(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd);

    /**
     * Perform a cross join with the right table.
     * <p>
     * Returns a table that is the cartesian product of left rows X right rows, with one column for each of the left
     * table's columns, and one column corresponding to each of the right table's columns that are included in the
     * columnsToAdd argument. The rows are ordered first by the left table then by the right table. If columnsToMatch is
     * non-empty then the product is filtered by the supplied match conditions.
     * <p>
     * To efficiently produce updates, the bits that represent a key for a given row are split into two. Unless
     * specified, join reserves 16 bits to represent a right row. When there are too few bits to represent all of the
     * right rows for a given aggregation group the table will shift a bit from the left side to the right side. The
     * default of 16 bits was carefully chosen because it results in an efficient implementation to process live
     * updates.
     * <p>
     * An {@link io.deephaven.engine.exceptions.OutOfKeySpaceException} is thrown when the total number of bits needed
     * to express the result table exceeds that needed to represent Long.MAX_VALUE. There are a few work arounds: - If
     * the left table is sparse, consider flattening the left table. - If there are no key-columns and the right table
     * is sparse, consider flattening the right table. - If the maximum size of a right table's group is small, you can
     * reserve fewer bits by setting numRightBitsToReserve on initialization.
     * <p>
     * Note: If you can prove that a given group has at most one right-row then you should prefer using
     * {@link #naturalJoin}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch An array of match pair conditions ("leftColumn=rightColumn" or "columnFoundInBoth")
     * @param columnsToAdd An array of the columns from the right side that need to be added to the left side as a
     *        result of the match.
     * @param numRightBitsToReserve The number of bits to reserve for rightTable groups.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    Table join(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd, int numRightBitsToReserve);

    // -----------------------------------------------------------------------------------------------------------------
    // Aggregation Operations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Returns the first (N) rows for each group.
     *
     * @param nRows the number of rows to return for each group
     * @param groupByColumnNames the column(s) by which to group data
     * @return a new table containing the first (N) rows for each group
     */
    Table headBy(long nRows, Collection<String> groupByColumnNames);

    /**
     * Returns the first (N) rows for each group.
     *
     * @param nRows the number of rows to return for each group
     * @param groupByColumnNames the column(s) by which to group data
     * @return a new table containing the first (N) rows for each group
     */
    Table headBy(long nRows, String... groupByColumnNames);

    /**
     * Returns the last (N) rows for each group.
     *
     * @param nRows the number of rows to return for each group
     * @param groupByColumnNames the column(s) by which to group data
     * @return a new table containing the last (N) rows for each group
     */
    Table tailBy(long nRows, Collection<String> groupByColumnNames);

    /**
     * Returns the last (N) rows for each group.
     *
     * @param nRows the number of rows to return for each group
     * @param groupByColumnNames the column(s) by which to group data
     * @return a new table containing the last (N) rows for each group
     */
    Table tailBy(long nRows, String... groupByColumnNames);

    /**
     * Groups data according to groupByColumns and applies formulaColumn to each of columns not altered by the grouping
     * operation. <code>columnParamName</code> is used as place-holder for the name of each column inside
     * <code>formulaColumn</code>.
     *
     * @param formulaColumn Formula applied to each column
     * @param columnParamName The parameter name used as a placeholder for each column
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy(Collection)}
     */
    @ConcurrentMethod
    Table applyToAllBy(String formulaColumn, String columnParamName, Collection<? extends ColumnName> groupByColumns);

    /**
     * Groups data according to groupByColumns and applies formulaColumn to each of columns not altered by the grouping
     * operation.
     *
     * @param formulaColumn Formula applied to each column, uses parameter <i>each</i> to refer to each colum it being
     *        applied to
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy(Collection)}
     */
    @ConcurrentMethod
    Table applyToAllBy(String formulaColumn, Collection<? extends ColumnName> groupByColumns);

    /**
     * Groups data according to groupByColumns and applies formulaColumn to each of columns not altered by the grouping
     * operation.
     *
     * @param formulaColumn Formula applied to each column, uses parameter <i>each</i> to refer to each colum it being
     *        applied to
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table applyToAllBy(String formulaColumn, String... groupByColumns);

    /**
     * If this table is a stream table, i.e. it has {@link #STREAM_TABLE_ATTRIBUTE} set to {@code true}, return a child
     * without the attribute, restoring standard semantics for aggregation operations.
     *
     * @return A non-stream child table, or this table if it is not a stream table
     */
    @ConcurrentMethod
    Table dropStream();

    // -----------------------------------------------------------------------------------------------------------------
    // Disaggregation Operations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Ungroups all grouped columns except for those specified.
     *
     * @param columnsNotToUngroup the columns not to ungroup
     * @return a new table with all columns ungrouped except those specified
     */
    Table ungroupAllBut(String... columnsNotToUngroup);

    // -----------------------------------------------------------------------------------------------------------------
    // PartitionBy Operations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Create a {@link PartitionedTable} from this table, partitioned according to the specified key columns.
     * <p>
     * The underlying partitioned table backing the result contains each row in {@code this} table in exactly one of the
     * result's constituent tables.
     *
     * @param dropKeys Whether to drop key columns in the output constituent tables
     * @param keyColumnNames The names of the key columns to partition by
     * @return A {@link PartitionedTable} keyed by {@code keyColumnNames}
     */
    @ConcurrentMethod
    PartitionedTable partitionBy(boolean dropKeys, String... keyColumnNames);

    /**
     * Equivalent to {@code partitionBy(false, keyColumnNames)}
     * <p>
     * Create a {@link PartitionedTable} from this table, partitioned according to the specified key columns. Key
     * columns are never dropped from the output constituent tables.
     * <p>
     * The underlying partitioned table backing the result contains each row in {@code this} table in exactly one of the
     * result's constituent tables.
     *
     * @param keyColumnNames The names of the key columns to partition by
     * @return A {@link PartitionedTable} keyed by {@code keyColumnNames}
     */
    @ConcurrentMethod
    PartitionedTable partitionBy(String... keyColumnNames);

    /**
     * Convenience method that performs an {@link #aggBy} and wraps the result in a {@link PartitionedTable}. If
     * {@code aggregations} does not include a {@link io.deephaven.api.agg.Partition partition}, one will be added
     * automatically with the default constituent column name and behavior used in {@link #partitionBy(String...)}.
     *
     * @param aggregations The {@link Aggregation aggregations} to apply
     * @param preserveEmpty Whether to keep result rows for groups that are initially empty or become empty as a result
     *        of updates. Each aggregation operator defines its own value for empty groups.
     * @param initialGroups A table whose distinct combinations of values for the {@code groupByColumns} should be used
     *        to create an initial set of aggregation groups. All other columns are ignored. This is useful in
     *        combination with {@code preserveEmpty == true} to ensure that particular groups appear in the result
     *        table, or with {@code preserveEmpty == false} to control the encounter order for a collection of groups
     *        and thus their relative order in the result. Changes to {@code initialGroups} are not expected or handled;
     *        if {@code initialGroups} is a refreshing table, only its contents at instantiation time will be used. If
     *        {@code initialGroups == null}, the result will be the same as if a table with no rows was supplied.
     * @param keyColumnNames The names of the key columns to aggregate by
     * @return A {@link PartitionedTable} keyed by {@code keyColumnNames}
     */
    @ConcurrentMethod
    PartitionedTable partitionedAggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty,
            Table initialGroups, String... keyColumnNames);

    // -----------------------------------------------------------------------------------------------------------------
    // Hierarchical table operations (rollup and tree).
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Create a rollup table.
     * <p>
     * A rollup table aggregates all rows of the table.
     *
     * @param aggregations The aggregations to perform
     * @return a hierarchical table with the rollup applied
     */
    @ConcurrentMethod
    RollupTable rollup(Collection<? extends Aggregation> aggregations);

    /**
     * Create a rollup table.
     * <p>
     * A rollup table aggregates all rows of the table.
     *
     * @param aggregations The aggregations to perform
     * @param includeConstituents set to true to include the constituent rows at the leaf level
     * @return a hierarchical table with the rollup applied
     */
    @ConcurrentMethod
    RollupTable rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents);

    /**
     * Create a rollup table.
     * <p>
     * A rollup table aggregates by the specified columns, and then creates a hierarchical table which re-aggregates
     * using one less aggregation column on each level. The column that is no longer part of the aggregation key is
     * replaced with null on each level.
     *
     * @param aggregations The aggregations to perform
     * @param groupByColumns the columns to group by
     * @return a hierarchical table with the rollup applied
     */
    @ConcurrentMethod
    RollupTable rollup(Collection<? extends Aggregation> aggregations, String... groupByColumns);

    /**
     * Create a rollup table.
     * <p>
     * A rollup table aggregates by the specified columns, and then creates a hierarchical table which re-aggregates
     * using one less aggregation column on each level. The column that is no longer part of the aggregation key is
     * replaced with null on each level.
     *
     * @param aggregations The aggregations to perform
     * @param groupByColumns the columns to group by
     * @param includeConstituents set to true to include the constituent rows at the leaf level
     * @return a hierarchical table with the rollup applied
     */
    @ConcurrentMethod
    RollupTable rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents,
            String... groupByColumns);

    /**
     * Create a rollup table.
     * <p>
     * A rollup table aggregates by the specified columns, and then creates a hierarchical table which re-aggregates
     * using one less aggregation column on each level. The column that is no longer part of the aggregation key is
     * replaced with null on each level.
     *
     * @param aggregations The aggregations to perform
     * @param groupByColumns the columns to group by
     * @return a hierarchical table with the rollup applied
     */
    @ConcurrentMethod
    RollupTable rollup(Collection<? extends Aggregation> aggregations,
            Collection<? extends ColumnName> groupByColumns);

    /**
     * Create a rollup table.
     * <p>
     * A rollup table aggregates by the specified columns, and then creates a hierarchical table which re-aggregates
     * using one less aggregation column on each level. The column that is no longer part of the aggregation key is
     * replaced with null on each level.
     *
     * @param aggregations The aggregations to perform
     * @param includeConstituents set to true to include the constituent rows at the leaf level
     * @param groupByColumns the columns to group by
     * @return a hierarchical table with the rollup applied
     */
    @ConcurrentMethod
    RollupTable rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents,
            Collection<? extends ColumnName> groupByColumns);

    /**
     * Create a hierarchical tree table.
     * <p>
     * The structure of the table is encoded by an "id" and a "parent" column. The id column should represent a unique
     * identifier for a given row, and the parent column indicates which row is the parent for a given row. Rows that
     * have a {@code null} parent are part of the "root" table.
     * <p>
     * It is possible for rows to be "orphaned" if their parent is non-{@code null} and does not exist in the table. See
     * {@link TreeTable#promoteOrphans(Table, String, String)}.
     *
     * @param idColumn The name of a column containing a unique identifier for a particular row in the table
     * @param parentColumn The name of a column containing the parent's identifier, {@code null} for rows that are part
     *        of the root table
     * @return A {@link TreeTable} organized according to the parent-child relationships expressed by {@code idColumn}
     *         and {@code parentColumn}
     */
    @ConcurrentMethod
    TreeTable tree(String idColumn, String parentColumn);

    // -----------------------------------------------------------------------------------------------------------------
    // Merge Operations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Merge this Table with {@code others}. All rows in this Table will appear before all rows in {@code others}. If
     * Tables in {@code others} are the result of a prior merge operation, they <em>may</em> be expanded in an attempt
     * to avoid deeply nested structures.
     *
     * @apiNote It's best to avoid many chained calls to {@link #mergeBefore(Table...)} and
     *          {@link #mergeAfter(Table...)}, as this may result in deeply-nested data structures. See
     *          TableTools.merge(Table...).
     * @param others The Tables to merge with
     * @return The merged Table
     */
    Table mergeBefore(Table... others);

    /**
     * Merge this Table with {@code others}. All rows in this Table will appear after all rows in {@code others}. If
     * Tables in {@code others} are the result of a prior merge operation, they <em>may</em> be expanded in an attempt
     * to avoid deeply nested structures.
     *
     * @apiNote It's best to avoid many chained calls to {@link #mergeBefore(Table...)} and
     *          {@link #mergeAfter(Table...)}, as this may result in deeply-nested data structures. See
     *          TableTools.merge(Table...).
     * @param others The Tables to merge with
     * @return The merged Table
     */
    Table mergeAfter(Table... others);

    // -----------------------------------------------------------------------------------------------------------------
    // Miscellaneous Operations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Explicitly ensure that any work needed to make a table addressable, iterable, or queryable has been done, and
     * return the coalesced child table if appropriate.
     *
     * @return This table, or a fully-coalesced child
     */
    @ConcurrentMethod
    Table coalesce();

    /**
     * Get a {@link Table} that contains a sub-set of the rows from {@code this}. The result will share the same
     * {@link #getColumnSources() column sources} and {@link #getDefinition() definition} as this table.
     *
     * The result will not update on its own. The caller must also establish an appropriate listener to update
     * {@code rowSet} and propagate {@link TableUpdate updates}.
     *
     * @param rowSet The {@link TrackingRowSet row set} for the result
     * @return A new sub-table
     */
    Table getSubTable(TrackingRowSet rowSet);

    /**
     * Applies a function to this table.
     * <p>
     * This is useful if you have a reference to a table or a proxy and want to run a series of operations against the
     * table without each individual operation resulting in an RMI.
     *
     * @param function the function to run, its single argument will be this table
     * @param <R> the return type of function
     * @return the return value of function
     * @implNote If the UGP is not required the {@link Function#apply(Object)} method should be annotated with
     *           {@link ConcurrentMethod}.
     */
    <R> R apply(Function<Table, R> function);

    /**
     * Creates a version of this table with a flat RowSet.
     */
    @ConcurrentMethod
    Table flatten();

    /**
     * Set the table's key columns.
     *
     * @return A copy of this table with the key columns specified, or this if no change was needed
     */
    @ConcurrentMethod
    Table withKeys(String... columns);

    /**
     * Set the table's key columns and indicate that each key set will be unique.
     *
     * @return A copy of this table with the unique key columns specified, or this if no change was needed
     */
    @ConcurrentMethod
    Table withUniqueKeys(String... columns);

    /**
     * Set a totals table for this Table.
     *
     * @param directive A packed string of totals table instructions
     * @return A copy of this Table with the {@link #TOTALS_TABLE_ATTRIBUTE totals table attribute} set
     */
    @ConcurrentMethod
    Table setTotalsTable(String directive);

    // -----------------------------------------------------------------------------------------------------------------
    // Resource Management
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Release resources held by this table, possibly destructively. This may render the table unsuitable or unsafe for
     * further use.
     *
     * @apiNote In practice, implementations usually just invoke {@link #releaseCachedResources()}.
     */
    void close();

    /**
     * Attempt to release cached resources held by this table. Unlike {@link #close()}, this must not render the table
     * unusable for subsequent read operations. Implementations should be sure to call
     * {@code super.releaseCachedResources()}.
     */
    void releaseCachedResources();

    // -----------------------------------------------------------------------------------------------------------------
    // Methods for refreshing tables
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * <p>
     * Wait for updates to this Table.
     * <p>
     * In some implementations, this call may also terminate in case of interrupt or spurious wakeup (see
     * java.util.concurrent.locks.Condition#await()).
     *
     * @throws InterruptedException In the event this thread is interrupted
     */
    void awaitUpdate() throws InterruptedException;

    /**
     * <p>
     * Wait for updates to this Table.
     * <p>
     * In some implementations, this call may also terminate in case of interrupt or spurious wakeup (see
     * java.util.concurrent.locks.Condition#await()).
     *
     * @param timeout The maximum time to wait in milliseconds.
     * @return false if the timeout elapses without notification, true otherwise.
     * @throws InterruptedException In the event this thread is interrupted
     */
    boolean awaitUpdate(long timeout) throws InterruptedException;

    /**
     * Subscribe for updates to this table. {@code listener} will be invoked via the {@link NotificationQueue}
     * associated with this Table.
     *
     * @param listener listener for updates
     */
    void addUpdateListener(ShiftObliviousListener listener);

    /**
     * Subscribe for updates to this table. After the optional initial image, {@code listener} will be invoked via the
     * {@link NotificationQueue} associated with this Table.
     *
     * @param listener listener for updates
     * @param replayInitialImage true to process updates for all initial rows in the table plus all changes; false to
     *        only process changes
     */
    void addUpdateListener(ShiftObliviousListener listener, boolean replayInitialImage);

    /**
     * Subscribe for updates to this table. {@code listener} will be invoked via the {@link NotificationQueue}
     * associated with this Table.
     *
     * @param listener listener for updates
     */
    void addUpdateListener(TableUpdateListener listener);

    /**
     * Unsubscribe the supplied listener.
     *
     * @param listener listener for updates
     */
    void removeUpdateListener(ShiftObliviousListener listener);

    /**
     * Unsubscribe the supplied listener.
     *
     * @param listener listener for updates
     */
    void removeUpdateListener(TableUpdateListener listener);

    /**
     * @return true if this table is in a failure state.
     */
    boolean isFailed();
}
