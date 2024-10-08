//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.api.*;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.Pair;
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

import javax.annotation.Nullable;
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
     * Provides column metadata in Table form.
     *
     * @return A Table of metadata about this Table's columns.
     */
    @ConcurrentMethod
    Table meta();

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
    /**
     * If this attribute is set, we can only add new row keys, we can never shift them, modify them, or remove them.
     */
    String ADD_ONLY_TABLE_ATTRIBUTE = "AddOnly";
    /**
     * If this attribute is set, we can only append new row keys to the end of the table. We can never shift them,
     * modify them, or remove them.
     */
    String APPEND_ONLY_TABLE_ATTRIBUTE = "AppendOnly";
    String TEST_SOURCE_TABLE_ATTRIBUTE = "TestSource";
    /**
     * If this attribute is present with value {@code true}, this Table is a "blink table".
     * <p>
     * A blink table provides a tabular presentation of rows accumulated from a stream since the previous cycle. Rows
     * added on a particular cycle are always removed on the following cycle. Note that this means any particular row of
     * data (not to be confused with a row key) never exists for more than one cycle. A blink table will never deliver
     * modifies or shifts as part of its {@link TableUpdate updates}, just adds for this cycle's new data and removes
     * for the previous cycle's old data.
     * <p>
     * Aggregation operations (e.g. {@link #aggBy}, {@link #aggAllBy}, {@link #countBy}, etc) on blink tables have
     * special semantics, allowing the result to aggregate over the entire observed stream of rows from the time the
     * operation is initiated. That means, for example, that a {@link #sumBy} on a blink table will contain the result
     * sums for each aggregation group over all observed rows since the {@code sumBy} was applied, rather than just the
     * sums for the current update cycle. This allows for aggregations over the full history of a stream to be performed
     * with greatly reduced memory costs when compared to the alternative strategy of holding the entirety of the stream
     * as an in-memory table.
     * <p>
     * All other operations on blink tables behave exactly as they do on other tables; that is, adds and removes are
     * processed as normal. For example {@link #select select} on a blink table will have only the newly added rows on
     * current update cycle.
     * <p>
     * The special aggregation semantics necessitate a few exclusions, i.e. operations that cannot be supported because
     * they need to keep track of all rows:
     * <ol>
     * <li>{@link #groupBy groupBy} is unsupported
     * <li>{@link #partitionBy partitionBy} is unsupported</li>
     * <li>{@link #partitionedAggBy(Collection, boolean, Table, String...) partitionedAggBy} is unsupported</li>
     * <li>{@link #aggBy aggBy} is unsupported if either {@link io.deephaven.api.agg.spec.AggSpecGroup group} or
     * {@link io.deephaven.api.agg.Partition partition} is used</li>
     * <li>{@link #rollup(Collection, boolean, Collection) rollup} is unsupported if
     * {@code includeConstituents == true}</li>
     * <li>{@link #tree(String, String) tree} is unsupported</li>
     * </ol>
     * <p>
     * To disable these semantics, a {@link #removeBlink() removeBlink} method is offered.
     */
    String BLINK_TABLE_ATTRIBUTE = "BlinkTable";
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
     * Retrieves a {@code ColumnSource}. It is conveniently cast to {@code ColumnSource<Object>} using the type that
     * caller expects. This differs from {@link #getColumnSource(String, Class)} which uses the provided {@link Class}
     * object to verify that the data type is a subclass of the expected class.
     *
     * <p>
     * The success of this call is equivalent to {@code getDefinition().checkColumn(sourceName)}, which is the preferred
     * way to check for compatibility in scenarios where the caller does not want the implementation to potentially
     * invoke {@link #coalesce()}.
     *
     * @param sourceName The name of the column
     * @param <T> The target type, as a type parameter. Inferred from context.
     * @return The column source for {@code sourceName}, parameterized by {@code T}
     * @see TableDefinition#checkHasColumn(String)
     */
    <T> ColumnSource<T> getColumnSource(String sourceName);

    /**
     * Retrieves a {@code ColumnSource} and {@link ColumnSource#cast(Class) casts} it to the target class {@code clazz}.
     *
     * <p>
     * The success of this call is equivalent to {@code getDefinition().checkColumn(sourceName, clazz)}, which is the
     * preferred way to check for compatibility in scenarios where the caller does not want to the implementation to
     * potentially invoke {@link #coalesce()}.
     *
     * @param sourceName The name of the column
     * @param clazz The target type
     * @param <T> The target type, as a type parameter. Intended to be inferred from {@code clazz}.
     * @return The column source for {@code sourceName}, parameterized by {@code T}
     * @see ColumnSource#cast(Class)
     * @see TableDefinition#checkHasColumn(String, Class)
     */
    <T> ColumnSource<T> getColumnSource(String sourceName, Class<? extends T> clazz);

    /**
     * Retrieves a {@code ColumnSource} and {@link ColumnSource#cast(Class, Class)} casts} it to the target class
     * {@code clazz} and {@code componentType}.
     *
     * <p>
     * The success of this call is equivalent to {@code getDefinition().checkColumn(sourceName, clazz, componentType)},
     * which is the preferred way to check for compatibility in scenarios where the caller does not want the
     * implementation to potentially invoke {@link #coalesce()}.
     *
     * @param sourceName The name of the column
     * @param clazz The target type
     * @param componentType The target component type, may be null
     * @param <T> The target type, as a type parameter. Intended to be inferred from {@code clazz}.
     * @return The column source for {@code sourceName}, parameterized by {@code T}
     * @see ColumnSource#cast(Class, Class)
     * @see TableDefinition#checkHasColumn(String, Class, Class)
     */
    <T> ColumnSource<T> getColumnSource(String sourceName, Class<? extends T> clazz, @Nullable Class<?> componentType);

    Map<String, ? extends ColumnSource<?>> getColumnSourceMap();

    Collection<? extends ColumnSource<?>> getColumnSources();

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

    <DATA_TYPE> CloseableIterator<DATA_TYPE> objectColumnIterator(@NotNull String columnName,
            @NotNull Class<? extends DATA_TYPE> clazz);

    // -----------------------------------------------------------------------------------------------------------------
    // Filter Operations
    // -----------------------------------------------------------------------------------------------------------------

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

    // -----------------------------------------------------------------------------------------------------------------
    // Column Selection Operations
    // -----------------------------------------------------------------------------------------------------------------

    @ConcurrentMethod
    Table dropColumnFormats();

    /**
     * Produce a new table with the specified columns renamed using the specified {@link Pair pairs}. The renames are
     * simultaneous and unordered, enabling direct swaps between column names. The resulting table retains the original
     * column ordering after applying the specified renames.
     * <p>
     * {@link IllegalArgumentException} will be thrown:
     * <ul>
     * <li>if a source column does not exist</li>
     * <li>if a source column is used more than once</li>
     * <li>if a destination column is used more than once</li>
     * </ul>
     *
     * @param pairs The columns to rename
     * @return The new table, with the columns renamed
     */
    @ConcurrentMethod
    Table renameColumns(Collection<Pair> pairs);

    /**
     * Produce a new table with the specified columns renamed using the syntax {@code "NewColumnName=OldColumnName"}.
     * The renames are simultaneous and unordered, enabling direct swaps between column names. The resulting table
     * retains the original column ordering after applying the specified renames.
     * <p>
     * {@link IllegalArgumentException} will be thrown:
     * <ul>
     * <li>if a source column does not exist</li>
     * <li>if a source column is used more than once</li>
     * <li>if a destination column is used more than once</li>
     * </ul>
     *
     * @param pairs The columns to rename
     * @return The new table, with the columns renamed
     */
    @ConcurrentMethod
    Table renameColumns(String... pairs);

    /**
     * Produce a new table with the specified columns renamed using the provided function. The renames are simultaneous
     * and unordered, enabling direct swaps between column names. The resulting table retains the original column
     * ordering after applying the specified renames.
     * <p>
     * {@link IllegalArgumentException} will be thrown:
     * <ul>
     * <li>if a destination column is used more than once</li>
     * </ul>
     *
     * @param renameFunction The function to apply to each column name
     * @return The new table, with the columns renamed
     */
    @ConcurrentMethod
    Table renameAllColumns(UnaryOperator<String> renameFunction);

    @ConcurrentMethod
    Table formatColumns(String... columnFormats);

    @ConcurrentMethod
    Table formatRowWhere(String condition, String formula);

    @ConcurrentMethod
    Table formatColumnWhere(String columnName, String condition, String formula);

    /**
     * Produce a new table with the specified columns moved to the leftmost position. Columns can be renamed with the
     * usual syntax, i.e. {@code "NewColumnName=OldColumnName")}. The renames are simultaneous and unordered, enabling
     * direct swaps between column names. All other columns are left in their original order.
     * <p>
     * {@link IllegalArgumentException} will be thrown:
     * <ul>
     * <li>if a source column does not exist</li>
     * <li>if a source column is used more than once</li>
     * <li>if a destination column is used more than once</li>
     * </ul>
     *
     * @param columnsToMove The columns to move to the left (and, optionally, to rename)
     * @return The new table, with the columns rearranged as explained above
     */
    @ConcurrentMethod
    Table moveColumnsUp(String... columnsToMove);

    /**
     * Produce a new table with the specified columns moved to the rightmost position. Columns can be renamed with the
     * usual syntax, i.e. {@code "NewColumnName=OldColumnName")}. The renames are simultaneous and unordered, enabling
     * direct swaps between column names. All other columns are left in their original order.
     * <p>
     * {@link IllegalArgumentException} will be thrown:
     * <ul>
     * <li>if a source column does not exist</li>
     * <li>if a source column is used more than once</li>
     * <li>if a destination column is used more than once</li>
     * </ul>
     *
     * @param columnsToMove The columns to move to the right (and, optionally, to rename)
     * @return The new table, with the columns rearranged as explained above
     */
    @ConcurrentMethod
    Table moveColumnsDown(String... columnsToMove);

    /**
     * Produce a new table with the specified columns moved to the specified {@code index}. Column indices begin at 0.
     * Columns can be renamed with the usual syntax, i.e. {@code "NewColumnName=OldColumnName")}. The renames are
     * simultaneous and unordered, enabling direct swaps between column names. The resulting table retains the original
     * column ordering except for the specified columns, which are inserted at the specified index, in the order of
     * {@code columnsToMove}, after the effects of applying any renames.
     * <p>
     * {@link IllegalArgumentException} will be thrown:
     * <ul>
     * <li>if a source column does not exist</li>
     * <li>if a source column is used more than once</li>
     * <li>if a destination column is used more than once</li>
     * </ul>
     * <p>
     * Values of {@code index} outside the range of 0 to the number of columns in the table (exclusive) will be clamped
     * to the nearest valid index.
     *
     * @param index The index to which the specified columns should be moved
     * @param columnsToMove The columns to move to the specified index (and, optionally, to rename)
     * @return The new table, with the columns rearranged as explained above
     */
    @ConcurrentMethod
    Table moveColumns(int index, String... columnsToMove);

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
     * <p>
     * If firstPosition is negative and lastPosition is positive, then firstPosition is counted from the end of the
     * table, inclusively. The lastPosition is counted from the beginning of the table, exclusively. For example,
     * slice(-3, 5) returns all rows starting from the third-last row to the fifth row of the table. If there are no
     * rows between these positions, the function will return an empty table.
     *
     * @param firstPositionInclusive the first position to include in the result
     * @param lastPositionExclusive the last position to include in the result
     * @return a new Table, which is the request subset of rows from the original table
     */
    @ConcurrentMethod
    Table slice(long firstPositionInclusive, long lastPositionExclusive);

    /**
     * Extracts a subset of a table by row percentages.
     * <p>
     * Returns a subset of table in the range [floor(startPercentInclusive * sizeOfTable), floor(endPercentExclusive *
     * sizeOfTable)). For example, for a table of size 10, slicePct(0.1, 0.7) will return a subset from the second row
     * to the seventh row. Similarly, slicePct(0, 1) would return the entire table (because row positions run from 0 to
     * size-1). The percentage arguments must be in range [0,1], otherwise the function returns an error.
     *
     * @param startPercentInclusive the starting percentage point for rows to include in the result, range [0, 1]
     * @param endPercentExclusive the ending percentage point for rows to include in the result, range [0, 1]
     * @return a new Table, which is the requested subset of rows from the original table
     */
    @ConcurrentMethod
    Table slicePct(double startPercentInclusive, double endPercentExclusive);

    /**
     * Provides a head that selects a dynamic number of rows based on a percent.
     *
     * @param percent the fraction of the table to return between [0, 1]. The number of rows will be rounded up. For
     *        example if there are 3 rows, headPct(50) returns the first two rows. For percent values outside [0, 1],
     *        the function will throw an exception.
     */
    @ConcurrentMethod
    Table headPct(double percent);

    /**
     * Provides a tail that selects a dynamic number of rows based on a percent.
     *
     * @param percent the fraction of the table to return between [0, 1]. The number of rows will be rounded up. For
     *        example if there are 3 rows, tailPct(50) returns the last two rows. For percent values outside [0, 1], the
     *        function will throw an exception.
     */
    @ConcurrentMethod
    Table tailPct(double percent);

    // -----------------------------------------------------------------------------------------------------------------
    // Aggregation Operations
    // -----------------------------------------------------------------------------------------------------------------

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
     * If this table is a blink table, i.e. it has {@link #BLINK_TABLE_ATTRIBUTE} set to {@code true}, return a child
     * without the attribute, restoring standard semantics for aggregation operations.
     *
     * @return A non-blink child table, or this table if it is not a blink table
     */
    @ConcurrentMethod
    Table removeBlink();

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
     * <p>
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
     * Wait for updates to this Table. Should not be invoked from a {@link TableListener} or other
     * {@link io.deephaven.engine.updategraph.NotificationQueue.Notification notification} on this Table's
     * {@link #getUpdateGraph() update graph}. It may be suitable to wait from another update graph if doing so does not
     * introduce any cycles.
     * <p>
     * In some implementations, this call may also terminate in case of interrupt or spurious wakeup.
     *
     * @throws InterruptedException In the event this thread is interrupted
     * @see java.util.concurrent.locks.Condition#await()
     */
    void awaitUpdate() throws InterruptedException;

    /**
     * <p>
     * Wait for updates to this Table. Should not be invoked from a {@link TableListener} or other
     * {@link io.deephaven.engine.updategraph.NotificationQueue.Notification notification} on this Table's
     * {@link #getUpdateGraph() update graph}. It may be suitable to wait from another update graph if doing so does not
     * introduce any cycles.
     * <p>
     * In some implementations, this call may also terminate in case of interrupt or spurious wakeup.
     *
     * @param timeout The maximum time to wait in milliseconds.
     * @return false if the timeout elapses without notification, true otherwise.
     * @throws InterruptedException In the event this thread is interrupted
     * @see java.util.concurrent.locks.Condition#await()
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
     * Subscribe for updates to this table if its last notification step matches {@code requiredLastNotificationStep}.
     * {@code listener} will be invoked via the {@link NotificationQueue} associated with this Table.
     *
     * @param listener listener for updates
     * @param requiredLastNotificationStep the expected last notification step to match
     * @return true if the listener was added, false if the last notification step requirement was not met
     */
    boolean addUpdateListener(final TableUpdateListener listener, final long requiredLastNotificationStep);

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
