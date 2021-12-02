/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table;

import io.deephaven.api.*;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.iterators.*;
import io.deephaven.engine.updategraph.ConcurrentMethod;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.util.systemicmarking.SystemicObject;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
        SystemicObject,
        TableOperations<Table, Table> {

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

    String DO_NOT_MAKE_REMOTE_ATTRIBUTE = "DoNotMakeRemote";
    String INPUT_TABLE_ATTRIBUTE = "InputTable";
    String KEY_COLUMNS_ATTRIBUTE = "keyColumns";
    String UNIQUE_KEYS_ATTRIBUTE = "uniqueKeys";
    String SORTABLE_COLUMNS_ATTRIBUTE = "SortableColumns";
    String FILTERABLE_COLUMNS_ATTRIBUTE = "FilterableColumns";
    String LAYOUT_HINTS_ATTRIBUTE = "LayoutHints";
    String TOTALS_TABLE_ATTRIBUTE = "TotalsTable";
    String TABLE_DESCRIPTION_ATTRIBUTE = "TableDescription";
    String COLUMN_RENDERERS_ATTRIBUTE = "ColumnRenderers";
    String COLUMN_DESCRIPTIONS_ATTRIBUTE = "ColumnDescriptions";
    String ADD_ONLY_TABLE_ATTRIBUTE = "AddOnly";
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
     * <li>{@link #aggBy} is unsupported if {@link Aggregation#AggGroup(String...)} is used
     * <li>{@link #partitionBy} is unsupported</li>
     * <li>{@link #rollup(Collection, boolean, Selectable...) rollup()} is unsupported if
     * {@code includeConstituents == true}</li>
     * <li>{@link #treeTable(String, String) treeTable()} is unsupported</li>
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
    // TODO: Might be good to take a pass through these and see what we can condense into
    // TODO: TreeTableInfo and RollupInfo to reduce the attribute noise.
    String ROLLUP_LEAF_ATTRIBUTE = "RollupLeaf";
    String HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE = "HierarchicalChildrenTableMap";
    String HIERARCHICAL_SOURCE_TABLE_ATTRIBUTE = "HierarchicalSourceTable";
    String TREE_TABLE_FILTER_REVERSE_LOOKUP_ATTRIBUTE = "TreeTableFilterReverseLookup";
    String HIERARCHICAL_SOURCE_INFO_ATTRIBUTE = "HierarchicalSourceTableInfo";
    String REVERSE_LOOKUP_ATTRIBUTE = "ReverseLookup";
    String PREPARED_RLL_ATTRIBUTE = "PreparedRll";
    String PREDEFINED_ROLLUP_ATTRIBUTE = "PredefinedRollup";
    String SNAPSHOT_VIEWPORT_TYPE = "Snapshot";
    /**
     * This attribute is used internally by TableTools.merge to detect successive merges. Its presence indicates that it
     * is safe to decompose the table into its multiple constituent parts.
     */
    String MERGED_TABLE_ATTRIBUTE = "MergedTable";
    /**
     * <p>
     * This attribute is applied to source tables, and takes on Boolean values.
     * <ul>
     * <li>True for post-{@link #coalesce()} source tables and their children if the source table is empty.</li>
     * <li>False for post-{@link #coalesce()} source tables and their children if the source table is non-empty.</li>
     * <li>Missing for all other tables.</li>
     * </ul>
     */
    String EMPTY_SOURCE_TABLE_ATTRIBUTE = "EmptySourceTable";
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
     * Set the value of an attribute.
     *
     * @param key the name of the attribute
     * @param object the value
     */
    @ConcurrentMethod
    void setAttribute(@NotNull String key, @Nullable Object object);

    /**
     * Get the value of the specified attribute.
     *
     * @param key the name of the attribute
     * @return the value, or null if there was none.
     */
    @ConcurrentMethod
    @Nullable
    Object getAttribute(@NotNull String key);

    /**
     * Get a set of all the attributes that have values for this table.
     *
     * @return a set of names
     */
    @ConcurrentMethod
    @NotNull
    Set<String> getAttributeNames();

    /**
     * Check if the specified attribute exists in this table.
     *
     * @param name the name of the attribute
     * @return true if the attribute exists
     */
    @ConcurrentMethod
    boolean hasAttribute(@NotNull String name);

    /**
     * Get all attributes from this Table.
     *
     * @return A map containing all attributes from this Table
     */
    @ConcurrentMethod
    Map<String, Object> getAttributes();

    /**
     * Get all attributes from this Table except the items that appear in excluded.
     *
     * @param excluded A set of attributes to exclude from the result
     * @return This Table's attributes, except the ones present in excluded
     */
    @ConcurrentMethod
    Map<String, Object> getAttributes(Collection<String> excluded);

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

    <TYPE> Iterator<TYPE> columnIterator(@NotNull String columnName);

    ByteColumnIterator byteColumnIterator(@NotNull String columnName);

    CharacterColumnIterator characterColumnIterator(@NotNull String columnName);

    DoubleColumnIterator doubleColumnIterator(@NotNull String columnName);

    FloatColumnIterator floatColumnIterator(@NotNull String columnName);

    IntegerColumnIterator integerColumnIterator(@NotNull String columnName);

    LongColumnIterator longColumnIterator(@NotNull String columnName);

    ShortColumnIterator shortColumnIterator(@NotNull String columnName);

    // -----------------------------------------------------------------------------------------------------------------
    // Convenience data fetching; highly inefficient
    // -----------------------------------------------------------------------------------------------------------------

    Object[] getRecord(long rowNo, String... columnNames);

    // -----------------------------------------------------------------------------------------------------------------
    // Filter Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    Table where(Collection<? extends Filter> filters);

    @ConcurrentMethod
    Table where(Filter... filters);

    @Override
    @ConcurrentMethod
    Table where(String... filters);

    /**
     * A table operation that applies the supplied predicate to each row in the table and produces columns containing
     * the pass/fail result of the predicate application. This is similar to {@link #where(String...)} except that
     * instead of selecting only rows that meet the criteria, new columns are added with the result of the comparison.
     *
     * @return a table with new columns containing the filter result for each row.
     */
    @ConcurrentMethod
    Table wouldMatch(WouldMatchPair... matchers);

    @ConcurrentMethod
    Table wouldMatch(String... expressions);

    @Override
    Table whereIn(Table rightTable, Collection<? extends JoinMatch> columnsToMatch);

    Table whereIn(Table rightTable, String... columnsToMatch);

    @Override
    Table whereNotIn(Table rightTable, Collection<? extends JoinMatch> columnsToMatch);

    Table whereNotIn(Table rightTable, String... columnsToMatch);

    // -----------------------------------------------------------------------------------------------------------------
    // Column Selection Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    Table select(Collection<? extends Selectable> columns);

    Table select(Selectable... columns);

    @Override
    Table select(String... columns);

    Table select();

    @ConcurrentMethod
    Table selectDistinct(Collection<? extends Selectable> columns);

    @ConcurrentMethod
    Table selectDistinct(Selectable... columns);

    @ConcurrentMethod
    Table selectDistinct(String... columns);

    @ConcurrentMethod
    Table selectDistinct();

    @Override
    Table update(Collection<? extends Selectable> newColumns);

    Table update(Selectable... newColumns);

    @Override
    Table update(String... newColumns);

    /**
     * Compute column formulas on demand.
     *
     * <p>
     * Lazy update defers computation until required for a set of values, and caches the results for a set of input
     * values. This uses less RAM than an update statement when you have a smaller set of unique values. Less
     * computation than an updateView is needed, because the results are saved in a cache.
     * </p>
     *
     * <p>
     * If you have many unique values, you should instead use an update statement, which will have more memory efficient
     * structures. Values are never removed from the lazyUpdate cache, so it should be used judiciously on a ticking
     * table.
     * </p>
     *
     * @param newColumns the columns to add
     * @return a new Table with the columns added; to be computed on demand
     */
    Table lazyUpdate(Collection<? extends Selectable> newColumns);

    Table lazyUpdate(Selectable... newColumns);

    Table lazyUpdate(String... newColumns);

    @Override
    @ConcurrentMethod
    Table view(Collection<? extends Selectable> columns);

    @ConcurrentMethod
    Table view(Selectable... columns);

    @Override
    @ConcurrentMethod
    Table view(String... columns);

    @Override
    @ConcurrentMethod
    Table updateView(Collection<? extends Selectable> newColumns);

    @ConcurrentMethod
    Table updateView(Selectable... newColumns);

    @Override
    @ConcurrentMethod
    Table updateView(String... newColumns);

    @ConcurrentMethod
    Table dropColumns(Collection<String> columnNames);

    @ConcurrentMethod
    Table dropColumns(String... columnNames);

    @ConcurrentMethod
    Table dropColumnFormats();

    Table renameColumns(MatchPair... pairs);

    Table renameColumns(Collection<String> columns);

    Table renameColumns(String... columns);

    Table renameAllColumns(UnaryOperator<String> renameFunction);

    @ConcurrentMethod
    Table formatColumns(String... columnFormats);

    @ConcurrentMethod
    Table formatRowWhere(String condition, String formula);

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

    @ConcurrentMethod
    Table head(long size);

    @ConcurrentMethod
    Table tail(long size);

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
     * If the firstPosition is nega tive and the lastPosition is negative, they are both counted from the end of the
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

    @ConcurrentMethod
    Table tailPct(double percent);

    // -----------------------------------------------------------------------------------------------------------------
    // Join Operations
    // -----------------------------------------------------------------------------------------------------------------

    Table exactJoin(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd);

    @Override
    Table exactJoin(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd);

    @Override
    Table exactJoin(Table rightTable, String columnsToMatch, String columnsToAdd);

    @Override
    Table exactJoin(Table rightTable, String columnsToMatch);

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

    @Override
    Table aj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd);

    @Override
    Table aj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, AsOfJoinRule asOfJoinRule);

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

    @Override
    Table aj(Table rightTable, String columnsToMatch, String columnsToAdd);

    @Override
    Table aj(Table rightTable, String columnsToMatch);

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

    @Override
    Table raj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd);

    @Override
    Table raj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, ReverseAsOfJoinRule reverseAsOfJoinRule);

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
    @Override
    Table raj(Table rightTable, String columnsToMatch, String columnsToAdd);

    @Override
    Table raj(Table rightTable, String columnsToMatch);

    Table naturalJoin(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd);

    @Override
    Table naturalJoin(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd);

    @Override
    Table naturalJoin(Table rightTable, String columnsToMatch, String columnsToAdd);

    @Override
    Table naturalJoin(Table rightTable, String columnsToMatch);

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

    @Override
    Table join(Table rightTable, String columnsToMatch);

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

    @Override
    Table join(Table rightTable, String columnsToMatch, String columnsToAdd);

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

    @Override
    Table join(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd);

    @Override
    Table join(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, int numRightBitsToReserve);

    // -----------------------------------------------------------------------------------------------------------------
    // Aggregation Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    Table groupBy(Collection<? extends Selectable> groupByColumns);

    @Override
    @ConcurrentMethod
    Table groupBy(String... groupByColumns);

    @Override
    @ConcurrentMethod
    Table groupBy();

    @Override
    @ConcurrentMethod
    Table aggAllBy(AggSpec spec);

    @Override
    @ConcurrentMethod
    Table aggAllBy(AggSpec spec, String... groupByColumns);

    @Override
    @ConcurrentMethod
    Table aggAllBy(AggSpec spec, Selectable... groupByColumns);

    @Override
    @ConcurrentMethod
    Table aggAllBy(AggSpec spec, Collection<String> groupByColumns);

    @Override
    @ConcurrentMethod
    Table aggBy(Aggregation aggregation);

    @Override
    @ConcurrentMethod
    Table aggBy(Aggregation aggregation, String... groupByColumns);

    @Override
    @ConcurrentMethod
    Table aggBy(Aggregation aggregation, Collection<? extends Selectable> groupByColumns);

    @Override
    @ConcurrentMethod
    Table aggBy(Collection<? extends Aggregation> aggregations, Collection<? extends Selectable> groupByColumns);

    @Override
    @ConcurrentMethod
    Table aggBy(Collection<? extends Aggregation> aggregations, String... groupByColumns);

    @Override
    @ConcurrentMethod
    Table aggBy(Collection<? extends Aggregation> aggregations);

    Table headBy(long nRows, Collection<String> groupByColumnNames);

    Table headBy(long nRows, String... groupByColumnNames);

    Table tailBy(long nRows, Collection<String> groupByColumnNames);

    Table tailBy(long nRows, String... groupByColumnNames);

    /**
     * Groups data according to groupByColumns and applies formulaColumn to each of columns not altered by the grouping
     * operation. <code>columnParamName</code> is used as place-holder for the name of each column inside
     * <code>formulaColumn</code>.
     *
     * @param formulaColumn Formula applied to each column
     * @param columnParamName The parameter name used as a placeholder for each column
     * @param groupByColumns The grouping columns as in {@link Table#groupBy(Collection)}
     */
    @ConcurrentMethod
    Table applyToAllBy(String formulaColumn, String columnParamName, Collection<? extends Selectable> groupByColumns);

    /**
     * Groups data according to groupByColumns and applies formulaColumn to each of columns not altered by the grouping
     * operation.
     *
     * @param formulaColumn Formula applied to each column, uses parameter <i>each</i> to refer to each colum it being
     *        applied to
     * @param groupByColumns The grouping columns as in {@link Table#groupBy(Collection)}
     */
    @ConcurrentMethod
    Table applyToAllBy(String formulaColumn, Collection<? extends Selectable> groupByColumns);

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
     * Groups the data column according to <code>groupByColumns</code> and computes the sum for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table sumBy(Selectable... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the sum for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table sumBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the sum for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table sumBy(Collection<String> groupByColumns);

    /**
     * Produces a single row table with the sum of each column.
     * <p>
     * When the input table is empty, zero output rows are produced.
     */
    @ConcurrentMethod
    Table sumBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the sum of the absolute values for
     * the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table absSumBy(Selectable... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the sum of the absolute values for
     * the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table absSumBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the sum of the absolute values for
     * the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table absSumBy(Collection<String> groupByColumns);

    /**
     * Produces a single row table with the absolute sum of each column.
     * <p>
     * When the input table is empty, zero output rows are produced.
     */
    @ConcurrentMethod
    Table absSumBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the average for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table avgBy(Selectable... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the average for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table avgBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the average for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table avgBy(Collection<String> groupByColumns);

    /**
     * Produces a single row table with the average of each column.
     * <p>
     * When the input table is empty, zero output rows are produced.
     */
    @ConcurrentMethod
    Table avgBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted average using
     * weightColumn for the rest of the fields
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table wavgBy(String weightColumn, Selectable... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted average using
     * weightColumn for the rest of the fields
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table wavgBy(String weightColumn, String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted average using
     * weightColumn for the rest of the fields
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table wavgBy(String weightColumn, Collection<String> groupByColumns);

    /**
     * Produces a single row table with the weighted average using weightColumn for the rest of the fields
     * <p>
     * When the input table is empty, zero output rows are produced.
     *
     * @param weightColumn the column to use for the weight
     */
    @ConcurrentMethod
    Table wavgBy(String weightColumn);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted sum using weightColumn
     * for the rest of the fields
     * <p>
     * If the weight column is a floating point type, all result columns will be doubles. If the weight column is an
     * integral type, all integral input columns will have long results and all floating point input columns will have
     * double results.
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table wsumBy(String weightColumn, Selectable... groupByColumns);

    /**
     * Computes the weighted sum for all rows in the table using weightColumn for the rest of the fields
     * <p>
     * If the weight column is a floating point type, all result columns will be doubles. If the weight column is an
     * integral type, all integral input columns will have long results and all floating point input columns will have
     * double results.
     *
     * @param weightColumn the column to use for the weight
     */
    @ConcurrentMethod
    Table wsumBy(String weightColumn);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted sum using weightColumn
     * for the rest of the fields
     * <p>
     * If the weight column is a floating point type, all result columns will be doubles. If the weight column is an
     * integral type, all integral input columns will have long results and all floating point input columns will have
     * double results.
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table wsumBy(String weightColumn, String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted sum using weightColumn
     * for the rest of the fields
     * <p>
     * If the weight column is a floating point type, all result columns will be doubles. If the weight column is an
     * integral type, all integral input columns will have long results and all floating point input columns will have
     * double results.
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table wsumBy(String weightColumn, Collection<String> groupByColumns);

    @ConcurrentMethod
    Table stdBy(Selectable... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the standard deviation for the rest
     * of the fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table stdBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the standard deviation for the rest
     * of the fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table stdBy(Collection<String> groupByColumns);

    /**
     * Produces a single row table with the standard deviation of each column.
     * <p>
     * When the input table is empty, zero output rows are produced.
     */
    @ConcurrentMethod
    Table stdBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the variance for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table varBy(Selectable... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the variance for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table varBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the variance for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table varBy(Collection<String> groupByColumns);

    /**
     * Produces a single row table with the variance of each column.
     * <p>
     * When the input table is empty, zero output rows are produced.
     */
    @ConcurrentMethod
    Table varBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the last for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table lastBy(Selectable... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the last for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table lastBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the last for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table lastBy(Collection<String> groupByColumns);

    /**
     * Returns the last row of the given table.
     */
    @ConcurrentMethod
    Table lastBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the first for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table firstBy(Selectable... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the first for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table firstBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the first for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table firstBy(Collection<String> groupByColumns);

    /**
     * Returns the first row of the given table.
     */
    @ConcurrentMethod
    Table firstBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the min for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table minBy(Selectable... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the min for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table minBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the min for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy}
     */
    @ConcurrentMethod
    Table minBy(Collection<String> groupByColumns);

    /**
     * Produces a single row table with the minimum of each column.
     * <p>
     * When the input table is empty, zero output rows are produced.
     */
    @ConcurrentMethod
    Table minBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the max for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy} }
     */
    @ConcurrentMethod
    Table maxBy(Selectable... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the max for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy} }
     */
    @ConcurrentMethod
    Table maxBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the max for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy} }
     */
    @ConcurrentMethod
    Table maxBy(Collection<String> groupByColumns);

    /**
     * Produces a single row table with the maximum of each column.
     * <p>
     * When the input table is empty, zero output rows are produced.
     */
    @ConcurrentMethod
    Table maxBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the median for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy} }
     */
    @ConcurrentMethod
    Table medianBy(Selectable... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the median for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy} }
     */
    @ConcurrentMethod
    Table medianBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the median for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link Table#groupBy} }
     */
    @ConcurrentMethod
    Table medianBy(Collection<String> groupByColumns);

    /**
     * Produces a single row table with the median of each column.
     * <p>
     * When the input table is empty, zero output rows are produced.
     */
    @ConcurrentMethod
    Table medianBy();

    @ConcurrentMethod
    Table countBy(String countColumnName, Selectable... groupByColumns);

    @ConcurrentMethod
    Table countBy(String countColumnName, String... groupByColumns);

    @ConcurrentMethod
    Table countBy(String countColumnName, Collection<String> groupByColumns);

    @ConcurrentMethod
    Table countBy(String countColumnName);

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
     * Ungroups a table by converting arrays into columns.
     *
     * @param nullFill indicates if the ungrouped table should allow disparate sized arrays filling shorter columns with
     *        null values. If set to false, then all arrays should be the same length.
     * @param columnsToUngroup the columns to ungroup
     * @return the ungrouped table
     */
    Table ungroup(boolean nullFill, String... columnsToUngroup);

    Table ungroup(String... columnsToUngroup);

    Table ungroupAllBut(String... columnsNotToUngroup);

    Table ungroup();

    Table ungroup(boolean nullFill);

    // -----------------------------------------------------------------------------------------------------------------
    // PartitionBy Operations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Create a {@link TableMap} from this table, keyed by the specified columns.
     *
     * <p>
     * The returned TableMap contains each row in this table in exactly one of the tables within the map. If you have
     * exactly one key column the TableMap is keyed by the value in that column. If you have zero key columns, then the
     * TableMap is keyed by {@code io.deephaven.datastructures.util.SmartKey.EMPTY} (and will contain this table as the
     * value). If you have multiple key columns, then the TableMap is keyed by a
     * {@code io.deephaven.datastructures.util.SmartKey}. The SmartKey will have one value for each of your column
     * values, in the order specified by keyColumnNames.
     * </p>
     *
     * <p>
     * For example if you have a Table keyed by a String column named USym, and a DateTime column named Expiry; a value
     * could be retrieved from the TableMap with
     * {@code tableMap.get(new SmartKey("SPY";, DateTimeUtils.convertDateTime("2020-06-19T16:15:00 NY")))}. For a table
     * with an Integer column named Bucket, you simply use the desired value as in {@code tableMap.get(1)}.
     * </p>
     *
     * @param dropKeys if true, drop key columns in the output Tables
     * @param keyColumnNames the name of the key columns to use.
     * @return a TableMap keyed by keyColumnNames
     */
    @ConcurrentMethod
    TableMap partitionBy(boolean dropKeys, String... keyColumnNames);

    /**
     * Create a {@link TableMap} from this table, keyed by the specified columns.
     *
     * <p>
     * The returned TableMap contains each row in this table in exactly one of the tables within the map. If you have
     * exactly one key column the TableMap is keyed by the value in that column. If you have zero key columns, then the
     * TableMap is keyed by {@code io.deephaven.datastructures.util.SmartKey.EMPTY} (and will contain this table as the
     * value). If you have multiple key columns, then the TableMap is keyed by a
     * {@code io.deephaven.datastructures.util.SmartKey}. The SmartKey will have one value for each of your column
     * values, in the order specified by keyColumnNames.
     * </p>
     *
     * <p>
     * For example if you have a Table keyed by a String column named USym, and a DateTime column named Expiry; a value
     * could be retrieved from the TableMap with
     * {@code tableMap.get(new SmartKey("SPY";, DateTimeUtils.convertDateTime("2020-06-19T16:15:00 NY")))}. For a table
     * with an Integer column named Bucket, you simply use the desired value as in {@code tableMap.get(1)}.
     * </p>
     *
     * @param keyColumnNames the name of the key columns to use.
     * @return a TableMap keyed by keyColumnNames
     */
    @ConcurrentMethod
    TableMap partitionBy(String... keyColumnNames);

    // -----------------------------------------------------------------------------------------------------------------
    // Hierarchical table operations (rollup and treeTable).
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Create a rollup table.
     * <p>
     * A rollup table aggregates by the specified columns, and then creates a hierarchical table which re-aggregates
     * using one less aggregation column on each level. The column that is no longer part of the aggregation key is
     * replaced with null on each level.
     *
     * @param aggregations The aggregations to perform
     * @param columns the columns to group by
     * @return a hierarchical table with the rollup applied
     */
    @ConcurrentMethod
    Table rollup(Collection<? extends Aggregation> aggregations, Collection<String> columns);

    /**
     * Create a rollup table.
     * <p>
     * A rollup table aggregates by the specified columns, and then creates a hierarchical table which re-aggregates
     * using one less aggregation column on each level. The column that is no longer part of the aggregation key is
     * replaced with null on each level.
     *
     * @param aggregations The aggregations to perform
     * @param includeConstituents set to true to include the constituent rows at the leaf level
     * @param columns the columns to group by
     * @return a hierarchical table with the rollup applied
     */
    @ConcurrentMethod
    Table rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents,
            Collection<String> columns);

    /**
     * Create a rollup table.
     * <p>
     * A rollup table aggregates by the specified columns, and then creates a hierarchical table which re-aggregates
     * using one less aggregation column on each level. The column that is no longer part of the aggregation key is
     * replaced with null on each level.
     *
     * @param aggregations The aggregations to perform
     * @param columns the columns to group by
     * @return a hierarchical table with the rollup applied
     */
    @ConcurrentMethod
    Table rollup(Collection<? extends Aggregation> aggregations, String... columns);

    /**
     * Create a rollup table.
     * <p>
     * A rollup table aggregates by the specified columns, and then creates a hierarchical table which re-aggregates
     * using one less aggregation column on each level. The column that is no longer part of the aggregation key is
     * replaced with null on each level.
     *
     * @param aggregations The aggregations to perform
     * @param columns the columns to group by
     * @param includeConstituents set to true to include the constituent rows at the leaf level
     * @return a hierarchical table with the rollup applied
     */
    @ConcurrentMethod
    Table rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents, String... columns);

    /**
     * Create a rollup table.
     * <p>
     * A rollup table aggregates by the specified columns, and then creates a hierarchical table which re-aggregates
     * using one less aggregation column on each level. The column that is no longer part of the aggregation key is
     * replaced with null on each level.
     *
     * @param aggregations The aggregations to perform
     * @param columns the columns to group by
     * @return a hierarchical table with the rollup applied
     */
    @ConcurrentMethod
    Table rollup(Collection<? extends Aggregation> aggregations, Selectable... columns);

    /**
     * Create a rollup table.
     * <p>
     * A rollup table aggregates all rows of the table.
     *
     * @param aggregations The aggregations to perform
     * @return a hierarchical table with the rollup applied
     */
    @ConcurrentMethod
    Table rollup(Collection<? extends Aggregation> aggregations);

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
    Table rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents);

    @ConcurrentMethod
    Table rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents, Selectable... columns);

    /**
     * Create a hierarchical tree table.
     * <p>
     * The structure of the table is encoded by an "id" and a "parent" column. The id column should represent a unique
     * identifier for a given row, and the parent column indicates which row is the parent for a given row. Rows that
     * have a null parent, are shown in the main table. It is possible for rows to be "orphaned", if their parent
     * reference is non-null and does not exist in the table.
     *
     * @param idColumn the name of a column containing a unique identifier for a particular row in the table
     * @param parentColumn the name of a column containing the parent's identifier, null for elements that are part of
     *        the root table
     * @return a hierarchical table grouped according to the parentColumn
     */
    @ConcurrentMethod
    Table treeTable(String idColumn, String parentColumn);

    // -----------------------------------------------------------------------------------------------------------------
    // Sort Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    @ConcurrentMethod
    Table sort(String... columnsToSortBy);

    @Override
    @ConcurrentMethod
    Table sortDescending(String... columnsToSortBy);

    @Override
    @ConcurrentMethod
    Table sort(Collection<SortColumn> columnsToSortBy);

    @Override
    @ConcurrentMethod
    Table reverse();

    /**
     * <p>
     * Disallow sorting on all but the specified columns.
     * </p>
     *
     * @param allowedSortingColumns The columns on which sorting is allowed.
     * @return The same table this was invoked on.
     */
    @ConcurrentMethod
    Table restrictSortTo(String... allowedSortingColumns);

    /**
     * <p>
     * Clear all sorting restrictions that was applied to the current table.
     * </p>
     *
     * <p>
     * Note that this table operates on the table it was invoked on and does not create a new table. So in the following
     * code <code>T1 = baseTable.where(...)
     * T2 = T1.restrictSortTo("C1")
     * T3 = T2.clearSortingRestrictions()
     * </code>
     * <p>
     * T1 == T2 == T3 and the result has no restrictions on sorting.
     * </p>
     *
     * @return The same table this was invoked on.
     */
    @ConcurrentMethod
    Table clearSortingRestrictions();

    // -----------------------------------------------------------------------------------------------------------------
    // Snapshot Operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    Table snapshot(Table baseTable, boolean doInitialSnapshot, String... stampColumns);

    @Override
    Table snapshot(Table baseTable, String... stampColumns);

    Table snapshotIncremental(Table rightTable, boolean doInitialSnapshot, String... stampColumns);

    Table snapshotIncremental(Table rightTable, String... stampColumns);

    Table snapshotHistory(Table rightTable);

    @Override
    Table snapshot(Table baseTable, boolean doInitialSnapshot, Collection<ColumnName> stampColumns);

    // -----------------------------------------------------------------------------------------------------------------
    // Merge Operations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Merge this Table with {@code others}. All rows in this Table will appear before all rows in {@code others}. If
     * Tables in {@code others} are the result of a prior merge operation, they <em>may</em> be expanded in an attempt
     * to avoid deeply nested structures.
     *
     * @apiNote It's best to avoid many chained calls to {@link #mergeBefore(Table...)} and
     *          {@link #mergeAfter(Table...)}, as this may result in deeply-nested data structures
     * @apiNote See TableTools.merge(Table...)
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
     *          {@link #mergeAfter(Table...)}, as this may result in deeply-nested data structures
     * @apiNote See TableTools.merge(Table...)
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
    Table coalesce();

    /**
     * Get a {@link Table} that contains a sub-set of the rows from {@code this}.
     *
     * @param rowSet The {@link TrackingRowSet row set} for the result.
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
     * @return The same table this method was invoked on, with the keyColumns attribute set
     */
    @ConcurrentMethod
    Table withKeys(String... columns);

    /**
     * Set the table's key columns and indicate that each key set will be unique.
     *
     * @return The same table this method was invoked on, with the keyColumns and unique attributes set
     */
    @ConcurrentMethod
    Table withUniqueKeys(String... columns);

    @ConcurrentMethod
    Table withTableDescription(String description);

    /**
     * Add a description for a specific column. You may use {@link #withColumnDescription(Map)} to set several
     * descriptions at once.
     *
     * @param column the name of the column
     * @param description the column description
     * @return a copy of the source table with the description applied
     */
    @ConcurrentMethod
    Table withColumnDescription(String column, String description);

    /**
     * Add a set of column descriptions to the table.
     *
     * @param descriptions a map of Column name to Column description.
     * @return a copy of the table with the descriptions applied.
     */
    @ConcurrentMethod
    Table withColumnDescription(Map<String, String> descriptions);

    /**
     * Set layout hints.
     *
     * @param hints A packed string of layout hints
     * @return A copy of this Table with the {@link #LAYOUT_HINTS_ATTRIBUTE layout hints attribute} set
     */
    @ConcurrentMethod
    Table setLayoutHints(String hints);

    /**
     * Set a totals table for this Table.
     *
     * @param directive A packed string of totals table instructions
     * @return A copy of this Table with the {@link #TOTALS_TABLE_ATTRIBUTE totals table attribute} set
     */
    @ConcurrentMethod
    Table setTotalsTable(String directive);

    /**
     * Set renderers for columns.
     *
     * @param directive A packed string of column rendering instructions
     * @return A copy of this Table with the {@link #COLUMN_RENDERERS_ATTRIBUTE column renderers attribute} set
     */
    @ConcurrentMethod
    Table setColumnRenderers(String directive);

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
    void listenForUpdates(ShiftObliviousListener listener);

    /**
     * Subscribe for updates to this table. After the optional initial image, {@code listener} will be invoked via the
     * {@link NotificationQueue} associated with this Table.
     *
     * @param listener listener for updates
     * @param replayInitialImage true to process updates for all initial rows in the table plus all changes; false to
     *        only process changes
     */
    void listenForUpdates(ShiftObliviousListener listener, boolean replayInitialImage);

    /**
     * Subscribe for updates to this table. {@code listener} will be invoked via the {@link NotificationQueue}
     * associated with this Table.
     *
     * @param listener listener for updates
     */
    void listenForUpdates(TableUpdateListener listener);

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
