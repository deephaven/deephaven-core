/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.util.ConcurrentMethod;

import java.util.Collection;

/**
 * Table operations is a user-accessible api for modifying tables or building up table operations.
 *
 * @param <TOPS> the table operations type
 * @param <TABLE> the table type
 */
public interface TableOperations<TOPS extends TableOperations<TOPS, TABLE>, TABLE> {

    // -------------------------------------------------------------------------------------------

    @ConcurrentMethod
    TOPS head(long size);

    @ConcurrentMethod
    TOPS tail(long size);

    // -------------------------------------------------------------------------------------------

    @ConcurrentMethod
    TOPS reverse();

    // -------------------------------------------------------------------------------------------

    /**
     * Snapshot {@code baseTable}, triggered by {@code this} table, and return a new table as a result. The returned
     * table will include an initial snapshot.
     *
     * <p>
     * Delegates to {@link #snapshot(Object, boolean, Collection)}.
     *
     * @param baseTable The table to be snapshotted
     * @param stampColumns The columns forming the "snapshot key", i.e. some subset of this Table's columns to be
     *        included in the result at snapshot time. As a special case, an empty stampColumns is taken to mean
     *        "include all columns".
     * @return The result table
     */
    TOPS snapshot(TABLE baseTable, String... stampColumns);

    /**
     * Snapshot {@code baseTable}, triggered by {@code this} table, and return a new table as a result.
     *
     * <p>
     * Delegates to {@link #snapshot(Object, boolean, Collection)}.
     *
     * @param baseTable The table to be snapshotted
     * @param doInitialSnapshot Take the first snapshot now (otherwise wait for a change event)
     * @param stampColumns The columns forming the "snapshot key", i.e. some subset of this Table's columns to be
     *        included in the result at snapshot time. As a special case, an empty stampColumns is taken to mean
     *        "include all columns".
     * @return The result table
     */
    TOPS snapshot(TABLE baseTable, boolean doInitialSnapshot, String... stampColumns);

    /**
     * Snapshot {@code baseTable}, triggered by {@code this} table, and return a new table as a result.
     *
     * <p>
     * {@code this} table is the triggering table, i.e. the table whose change events cause a new snapshot to be taken.
     * The result table includes a "snapshot key" which is a subset (possibly all) of {@code this} table's columns. The
     * remaining columns in the result table come from {@code baseTable}, the table being snapshotted.
     *
     * @param baseTable The table to be snapshotted
     * @param doInitialSnapshot Take the first snapshot now (otherwise wait for a change event)
     * @param stampColumns The columns forming the "snapshot key", i.e. some subset of this Table's columns to be
     *        included in the result at snapshot time. As a special case, an empty stampColumns is taken to mean
     *        "include all columns".
     * @return The result table
     */
    TOPS snapshot(TABLE baseTable, boolean doInitialSnapshot, Collection<ColumnName> stampColumns);

    // -------------------------------------------------------------------------------------------

    @ConcurrentMethod
    TOPS sort(String... columnsToSortBy);

    @ConcurrentMethod
    TOPS sortDescending(String... columnsToSortBy);

    @ConcurrentMethod
    TOPS sort(Collection<SortColumn> columnsToSortBy);

    // -------------------------------------------------------------------------------------------

    @ConcurrentMethod
    TOPS where(String... filters);

    @ConcurrentMethod
    TOPS where(Collection<? extends Filter> filters);

    // -------------------------------------------------------------------------------------------

    /**
     * Filters {@code this} table based on the set of values in the {@code rightTable}.
     *
     * <p>
     * Delegates to {@link #whereIn(Object, Collection)}.
     *
     * @param rightTable the filtering table.
     * @param columnsToMatch the columns to match between the two tables
     * @return a new table filtered on right table
     */
    TOPS whereIn(TABLE rightTable, String... columnsToMatch);

    /**
     * Filters {@code this} table based on the set of values in the {@code rightTable}.
     *
     * <p>
     * Note that when the {@code rightTable} ticks, all of the rows in {@code this} table are going to be re-evaluated,
     * thus the intention is that the {@code rightTable} is fairly slow moving compared with {@code this} table.
     *
     * @param rightTable the filtering table.
     * @param columnsToMatch the columns to match between the two tables
     * @return a new table filtered on right table
     */
    TOPS whereIn(TABLE rightTable, Collection<? extends JoinMatch> columnsToMatch);

    // -------------------------------------------------------------------------------------------

    /**
     * Filters {@code this} table based on the set of values <b>not</b> in the {@code rightTable}.
     *
     * <p>
     * Delegates to {@link #whereNotIn(Object, Collection)}.
     *
     * @param rightTable the filtering table.
     * @param columnsToMatch the columns to match between the two tables
     * @return a new table filtered on right table
     */
    TOPS whereNotIn(TABLE rightTable, String... columnsToMatch);

    /**
     * Filters {@code this} table based on the set of values <b>not</b> in the {@code rightTable}.
     *
     * <p>
     * Note that when the {@code rightTable} ticks, all of the rows in {@code this} table are going to be re-evaluated,
     * thus the intention is that the {@code rightTable} is fairly slow moving compared with {@code this} table.
     *
     * @param rightTable the filtering table.
     * @param columnsToMatch the columns to match between the two tables
     * @return a new table filtered on right table
     */
    TOPS whereNotIn(TABLE rightTable, Collection<? extends JoinMatch> columnsToMatch);

    // -------------------------------------------------------------------------------------------

    @ConcurrentMethod
    TOPS view(String... columns);

    @ConcurrentMethod
    TOPS view(Collection<? extends Selectable> columns);

    // -------------------------------------------------------------------------------------------

    @ConcurrentMethod
    TOPS updateView(String... columns);

    @ConcurrentMethod
    TOPS updateView(Collection<? extends Selectable> columns);

    // -------------------------------------------------------------------------------------------

    TOPS update(String... columns);

    TOPS update(Collection<? extends Selectable> columns);

    // -------------------------------------------------------------------------------------------

    /**
     * Compute column formulas on demand.
     *
     * <p>
     * Delegates to {@link #lazyUpdate(Collection)}.
     *
     * @param columns the columns to add
     * @return a new Table with the columns added; to be computed on demand
     */
    TOPS lazyUpdate(String... columns);

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
     * @param columns the columns to add
     * @return a new Table with the columns added; to be computed on demand
     */
    TOPS lazyUpdate(Collection<? extends Selectable> columns);

    // -------------------------------------------------------------------------------------------

    TOPS select(String... columns);

    TOPS select(Collection<? extends Selectable> columns);

    // -------------------------------------------------------------------------------------------

    /**
     * Perform an natural-join with the {@code rightTable}.
     *
     * <p>
     * Delegates to {@link #naturalJoin(Object, Collection, Collection)}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth")
     * @return the natural-joined table
     */
    TOPS naturalJoin(TABLE rightTable, String columnsToMatch);

    /**
     * Perform a natural-join with the {@code rightTable}.
     *
     * <p>
     * Delegates to {@link #naturalJoin(Object, Collection, Collection)}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth")
     * @param columnsToAdd A comma separated list with the columns from the right side that need to be added to the left
     *        side as a result of the match.
     * @return the natural-joined table
     */
    TOPS naturalJoin(TABLE rightTable, String columnsToMatch, String columnsToAdd);

    /**
     * Perform an exact-join with the {@code rightTable}.
     *
     * <p>
     * Requires zero or one match from the {@code rightTable}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch The match pair conditions.
     * @param columnsToAdd The columns from the right side that need to be added to the left side as a result of the
     *        match.
     * @return the natural-joined table
     */
    TOPS naturalJoin(TABLE rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd);

    // -------------------------------------------------------------------------------------------

    /**
     * Perform an exact-join with the {@code rightTable}.
     *
     * <p>
     * Delegates to {@link #exactJoin(Object, Collection, Collection)}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth")
     * @return the exact-joined table
     */
    TOPS exactJoin(TABLE rightTable, String columnsToMatch);

    /**
     * Perform an exact-join with the {@code rightTable}.
     *
     * <p>
     * Delegates to {@link #exactJoin(Object, Collection, Collection)}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth")
     * @param columnsToAdd A comma separated list with the columns from the right side that need to be added to the left
     *        side as a result of the match.
     * @return the exact-joined table
     */
    TOPS exactJoin(TABLE rightTable, String columnsToMatch, String columnsToAdd);

    /**
     * Perform an exact-join with the {@code rightTable}.
     *
     * <p>
     * Similar to {@link #naturalJoin(Object, Collection, Collection)}, but requires that exactly one match from the
     * {@code rightTable}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch The match pair conditions.
     * @param columnsToAdd The columns from the right side that need to be added to the left side as a result of the
     *        match.
     * @return the exact-joined table
     */
    TOPS exactJoin(TABLE rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd);

    // -------------------------------------------------------------------------------------------

    /**
     * Perform a cross join with the {@code rightTable}.
     *
     * <p>
     * Delegates to {@link #join(Object, Collection, Collection, int)}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth")
     * @return a new table joined according to the specification in columnsToMatch and includes all non-key-columns from
     *         the right table
     * @see #join(Object, Collection, Collection, int)
     */
    TOPS join(TABLE rightTable, String columnsToMatch);

    /**
     * Perform a cross join with the {@code rightTable}.
     *
     * <p>
     * Delegates to {@link #join(Object, Collection, Collection, int)}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth")
     * @param columnsToAdd A comma separated list with the columns from the right side that need to be added to the left
     *        side as a result of the match.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     * @see #join(Object, Collection, Collection, int)
     */
    TOPS join(TABLE rightTable, String columnsToMatch, String columnsToAdd);


    /**
     * Perform a cross join with the {@code rightTable}.
     *
     * <p>
     * Delegates to {@link #join(Object, Collection, Collection, int)}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch The match pair conditions.
     * @param columnsToAdd The columns from the right side that need to be added to the left side as a result of the
     *        match.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    TOPS join(TABLE rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd);

    /**
     * Perform a cross join with the {@code rightTable}.
     *
     * <p>
     * Returns a table that is the cartesian product of left rows X right rows, with one column for each of {@code this}
     * table's columns, and one column corresponding to each of the {@code rightTable}'s columns that are included in
     * the {@code columnsToAdd} argument. The rows are ordered first by the {@code this} table then by the
     * {@code rightTable}. If {@code columnsToMatch} is non-empty then the product is filtered by the supplied match
     * conditions.
     *
     * <p>
     * To efficiently produce updates, the bits that represent a key for a given row are split into two. Unless
     * specified, join reserves 16 bits to represent a right row. When there are too few bits to represent all of the
     * right rows for a given aggregation group the table will shift a bit from the left side to the right side. The
     * default of 16 bits was carefully chosen because it results in an efficient implementation to process live
     * updates.
     *
     * <p>
     * An io.deephaven.engine.table.impl.util.OutOfKeySpaceException is thrown when the total number of bits needed to
     * express the result table exceeds that needed to represent Long.MAX_VALUE. There are a few work arounds:
     *
     * <p>
     * - If the left table is sparse, consider flattening the left table.
     * <p>
     * - If there are no key-columns and the right table is sparse, consider flattening the right table.
     * <p>
     * - If the maximum size of a right table's group is small, you can reserve fewer bits by setting
     * {@code reserveBits} on initialization.
     *
     * <p>
     * Note: If you know that a given group has at most one right-row then you should prefer using
     * {@link #naturalJoin(Object, Collection, Collection)}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch The match pair conditions.
     * @param columnsToAdd The columns from the right side that need to be added to the left side as a result of the
     *        match.
     * @param reserveBits The number of bits to reserve for rightTable groups.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    TOPS join(TABLE rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, int reserveBits);

    // -------------------------------------------------------------------------------------------

    /**
     * Perform an as-of join with the {@code rightTable}.
     *
     * <p>
     * Delegates to {@link #aj(Object, Collection, Collection, AsOfJoinRule)}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth").
     * @return a new table joined according to the specification in columnsToMatch
     */
    TOPS aj(TABLE rightTable, String columnsToMatch);

    /**
     * Perform an as-of join with the {@code rightTable}.
     *
     * <p>
     * Delegates to {@link #aj(Object, Collection, Collection, AsOfJoinRule)}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth").
     * @param columnsToAdd A comma separated list with the columns from the left side that need to be added to the right
     *        side as a result of the match.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    TOPS aj(TABLE rightTable, String columnsToMatch, String columnsToAdd);

    /**
     * Perform an as-of join with the {@code rightTable}.
     *
     * <p>
     * Delegates to {@link #aj(Object, Collection, Collection, AsOfJoinRule)}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch The match pair conditions.
     * @param columnsToAdd The columns from the right side that need to be added to the left side as a result of the
     *        match.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    TOPS aj(TABLE rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd);

    /**
     * Perform an as-of join with the {@code rightTable}.
     *
     * <p>
     * Looks up the columns in the {@code rightTable} that meet the match conditions in {@code columnsToMatch}. Matching
     * is done exactly for the first n-1 columns and via a binary search for the last match pair. The columns of the
     * {@code this} table are returned intact, together with the columns from {@code rightTable} defined in the
     * {@code columnsToAdd}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch The match pair conditions.
     * @param columnsToAdd The columns from the right side that need to be added to the left side as a result of the
     *        match.
     * @param asOfJoinRule The binary search operator for the last match pair.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    TOPS aj(TABLE rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, AsOfJoinRule asOfJoinRule);

    // -------------------------------------------------------------------------------------------

    /**
     * Perform an reverse-as-of join with the {@code rightTable}.
     *
     * <p>
     * Delegates to {@link #raj(Object, Collection, Collection, ReverseAsOfJoinRule)}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth").
     * @return a new table joined according to the specification in columnsToMatch
     */
    TOPS raj(TABLE rightTable, String columnsToMatch);

    /**
     * Perform a reverse-as-of join with the {@code rightTable}.
     *
     * <p>
     * Delegates to {@link #raj(Object, Collection, Collection, ReverseAsOfJoinRule)}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch A comma separated list of match conditions ("leftColumn=rightColumn" or
     *        "columnFoundInBoth").
     * @param columnsToAdd A comma separated list with the columns from the left side that need to be added to the right
     *        side as a result of the match.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    TOPS raj(TABLE rightTable, String columnsToMatch, String columnsToAdd);

    /**
     * Perform a reverse-as-of join with the {@code rightTable}.
     *
     * <p>
     * Delegates to {@link #raj(Object, Collection, Collection, ReverseAsOfJoinRule)}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch The match pair conditions.
     * @param columnsToAdd The columns from the right side that need to be added to the left side as a result of the
     *        match.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    TOPS raj(TABLE rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd);

    /**
     * Perform a reverse-as-of join with the {@code rightTable}.
     *
     * <p>
     * Just like {@link #aj(Object, Collection, Collection, AsOfJoinRule)}, but the matching on the last column is in
     * reverse order, so that you find the row after the given timestamp instead of the row before.
     *
     * <p>
     * Looks up the columns in the {@code rightTable} that meet the match conditions in {@code columnsToMatch}. Matching
     * is done exactly for the first n-1 columns and via a binary search for the last match pair. The columns of
     * {@code this} table are returned intact, together with the columns from {@code rightTable} defined in
     * {@code columnsToAdd}.
     *
     * @param rightTable The right side table on the join.
     * @param columnsToMatch The match pair conditions.
     * @param columnsToAdd The columns from the right side that need to be added to the left side as a result of the
     *        match.
     * @param reverseAsOfJoinRule The binary search operator for the last match pair.
     * @return a new table joined according to the specification in columnsToMatch and columnsToAdd
     */
    TOPS raj(TABLE rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, ReverseAsOfJoinRule reverseAsOfJoinRule);

    // -------------------------------------------------------------------------------------------

    @ConcurrentMethod
    TOPS groupBy();

    @ConcurrentMethod
    TOPS groupBy(String... groupByColumns);

    @ConcurrentMethod
    TOPS groupBy(Collection<? extends ColumnName> groupByColumns);

    // -------------------------------------------------------------------------------------------

    @ConcurrentMethod
    TOPS aggAllBy(AggSpec spec);

    @ConcurrentMethod
    TOPS aggAllBy(AggSpec spec, String... groupByColumns);

    @ConcurrentMethod
    TOPS aggAllBy(AggSpec spec, ColumnName... groupByColumns);

    @ConcurrentMethod
    TOPS aggAllBy(AggSpec spec, Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    /**
     * Produce an aggregated result by grouping all rows from {@code this} into a single group of rows and applying
     * {@code aggregation} to the result. The result table will have one row if {@code this} has one or more rows, or
     * else zero rows.
     *
     * @param aggregation The {@link Aggregation aggregation} to apply
     * @return A new table aggregating the rows of {@code this}
     */
    @ConcurrentMethod
    TOPS aggBy(Aggregation aggregation);

    /**
     * Produce an aggregated result by grouping all rows from {@code this} into a single group of rows and applying
     * {@code aggregations} to the result. The result table will have one row if {@code this} has one or more rows, or
     * else zero rows.
     *
     * @param aggregations The {@link Aggregation aggregations} to apply
     * @return A new table aggregating the rows of {@code this}
     */
    @ConcurrentMethod
    TOPS aggBy(Collection<? extends Aggregation> aggregations);

    /**
     * Produce an aggregated result by grouping all rows from {@code this} into a single group of rows and applying
     * {@code aggregations} to the result.
     *
     * @param aggregations The {@link Aggregation aggregations} to apply
     * @param preserveEmpty If {@code preserveEmpty == true}, the result table will always have one row. Otherwise, the
     *        result table will have one row if {@code this} has one or more rows, or else zero rows.
     * @return A new table aggregating the rows of {@code this}
     */
    @ConcurrentMethod
    TOPS aggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty);

    /**
     * Produce an aggregated result by grouping {@code this} according to the {@code groupByColumns} and applying
     * {@code aggregation} to each resulting group of rows. The result table will have one row per group, ordered by the
     * <em>encounter order</em> within {@code this}, thereby ensuring that the row key for a given group never changes.
     * Groups that become empty will be removed from the result.
     *
     * @param aggregation The {@link Aggregation aggregation} to apply
     * @param groupByColumns The columns to group by
     * @return A new table aggregating the rows of {@code this}
     */
    @ConcurrentMethod
    TOPS aggBy(Aggregation aggregation, String... groupByColumns);

    /**
     * Produce an aggregated result by grouping {@code this} according to the {@code groupByColumns} and applying
     * {@code aggregation} to each resulting group of rows. The result table will have one row per group, ordered by the
     * <em>encounter order</em> within {@code this}, thereby ensuring that the row key for a given group never changes.
     * Groups that become empty will be removed from the result.
     *
     * @param aggregation The {@link Aggregation aggregation} to apply
     * @param groupByColumns The {@link ColumnName columns} to group by
     * @return A new table aggregating the rows of {@code this}
     */
    @ConcurrentMethod
    TOPS aggBy(Aggregation aggregation, Collection<? extends ColumnName> groupByColumns);

    /**
     * Produce an aggregated result by grouping {@code this} according to the {@code groupByColumns} and applying
     * {@code aggregations} to each resulting group of rows. The result table will have one row per group, ordered by
     * the <em>encounter order</em> within {@code this}, thereby ensuring that the row key for a given group never
     * changes. Groups that become empty will be removed from the result.
     *
     * @param aggregations The {@link Aggregation aggregations} to apply
     * @param groupByColumns The columns to group by
     * @return A new table aggregating the rows of {@code this}
     */
    @ConcurrentMethod
    TOPS aggBy(Collection<? extends Aggregation> aggregations, String... groupByColumns);

    /**
     * Produce an aggregated result by grouping {@code this} according to the {@code groupByColumns} and applying
     * {@code aggregations} to each resulting group of rows. The result table will have one row per group, ordered by
     * the <em>encounter order</em> within {@code this}, thereby ensuring that the row key for a given group never
     * changes. Groups that become empty will be removed from the result.
     *
     * @param aggregations The {@link Aggregation aggregations} to apply
     * @param groupByColumns The {@link ColumnName columns} to group by
     * @return A new table aggregating the rows of {@code this}
     */
    @ConcurrentMethod
    TOPS aggBy(Collection<? extends Aggregation> aggregations, Collection<? extends ColumnName> groupByColumns);

    /**
     * Produce an aggregated result by grouping {@code this} according to the {@code groupByColumns} and applying
     * {@code aggregations} to each resulting group of rows. The result table will have one row per group, ordered by
     * the <em>encounter order</em> within {@code this}, thereby ensuring that the row key for a given group never
     * changes.
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
     * @param groupByColumns The {@link ColumnName columns} to group by
     * @return A new table aggregating the rows of {@code this}
     */
    @ConcurrentMethod
    TOPS aggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty, TABLE initialGroups,
            Collection<? extends ColumnName> groupByColumns);

    // -------------------------------------------------------------------------------------------

    /**
     * Creates a table with additional columns calculated from window-based aggregations of columns in its parent. The
     * aggregations are defined by the {@code operations}, which support incremental aggregation over the corresponding
     * rows in the parent table. The aggregations will apply position or time-based windowing and compute the results
     * over the entire table.
     *
     * @param operation the operation to apply to the table.
     * @return a table with the same rowset, with the specified operation applied to the entire table
     */
    @ConcurrentMethod
    TOPS updateBy(UpdateByOperation operation);

    /**
     * Creates a table with additional columns calculated from window-based aggregations of columns in its parent. The
     * aggregations are defined by the {@code operations}, which support incremental aggregation over the corresponding
     * rows in the parent table. The aggregations will apply position or time-based windowing and compute the results
     * over the entire table.
     *
     * @param operations the operations to apply to the table.
     * @return a table with the same rowset, with the specified operations applied to the entire table.
     */
    @ConcurrentMethod
    TOPS updateBy(Collection<? extends UpdateByOperation> operations);

    /**
     * Creates a table with additional columns calculated from window-based aggregations of columns in its parent. The
     * aggregations are defined by the {@code operations}, which support incremental aggregation over the corresponding
     * rows in the parent table. The aggregations will apply position or time-based windowing and compute the results
     * over the entire table.
     *
     * @param control the {@link UpdateByControl control} to use when updating the table.
     * @param operations the operations to apply to the table.
     * @return a table with the same rowset, with the specified operations applied to the entire table
     */
    @ConcurrentMethod
    TOPS updateBy(UpdateByControl control, Collection<? extends UpdateByOperation> operations);

    /**
     * Creates a table with additional columns calculated from window-based aggregations of columns in its parent. The
     * aggregations are defined by the {@code operations}, which support incremental aggregation over the corresponding
     * rows in the parent table. The aggregations will apply position or time-based windowing and compute the results
     * for the row group (as determined by the {@code byColumns}).
     *
     * @param operation the operation to apply to the table.
     * @param byColumns the columns to group by before applying.
     * @return a table with the same rowSet, with the specified operation applied to each group defined by the
     *         {@code byColumns}
     */
    @ConcurrentMethod
    TOPS updateBy(UpdateByOperation operation, final String... byColumns);

    /**
     * Creates a table with additional columns calculated from window-based aggregations of columns in its parent. The
     * aggregations are defined by the {@code operations}, which support incremental aggregation over the corresponding
     * rows in the parent table. The aggregations will apply position or time-based windowing and compute the results
     * for the row group (as determined by the {@code byColumns}).
     *
     * @param operations the operations to apply to the table.
     * @param byColumns the columns to group by before applying.
     * @return a table with the same rowSet, with the specified operations applied to each group defined by the
     *         {@code byColumns}
     */
    @ConcurrentMethod
    TOPS updateBy(Collection<? extends UpdateByOperation> operations, final String... byColumns);

    /**
     * Creates a table with additional columns calculated from window-based aggregations of columns in its parent. The
     * aggregations are defined by the {@code operations}, which support incremental aggregation over the corresponding
     * rows in the parent table. The aggregations will apply position or time-based windowing and compute the results
     * for the row group (as determined by the {@code byColumns}).
     *
     * @param operations the operations to apply to the table.
     * @param byColumns the columns to group by before applying.
     * @return a table with the same rowSet, with the specified operations applied to each group defined by the
     *         {@code byColumns}
     */
    @ConcurrentMethod
    TOPS updateBy(Collection<? extends UpdateByOperation> operations, Collection<? extends ColumnName> byColumns);

    /**
     * Creates a table with additional columns calculated from window-based aggregations of columns in its parent. The
     * aggregations are defined by the {@code operations}, which support incremental aggregation over the corresponding
     * rows in the parent table. The aggregations will apply position or time-based windowing and compute the results
     * for the row group (as determined by the {@code byColumns}).
     *
     * @param control the {@link UpdateByControl control} to use when updating the table.
     * @param operations the operations to apply to the table.
     * @param byColumns the columns to group by before applying.
     * @return a table with the same rowSet, with the specified operations applied to each group defined by the
     *         {@code byColumns}
     */
    @ConcurrentMethod
    TOPS updateBy(UpdateByControl control, Collection<? extends UpdateByOperation> operations,
            Collection<? extends ColumnName> byColumns);

    // -------------------------------------------------------------------------------------------

    @ConcurrentMethod
    TOPS selectDistinct();

    @ConcurrentMethod
    TOPS selectDistinct(String... columns);

    @ConcurrentMethod
    TOPS selectDistinct(Selectable... columns);

    @ConcurrentMethod
    TOPS selectDistinct(Collection<? extends Selectable> columns);

    // -------------------------------------------------------------------------------------------

    @ConcurrentMethod
    TOPS countBy(String countColumnName);

    @ConcurrentMethod
    TOPS countBy(String countColumnName, String... groupByColumns);

    @ConcurrentMethod
    TOPS countBy(String countColumnName, ColumnName... groupByColumns);

    @ConcurrentMethod
    TOPS countBy(String countColumnName, Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    /**
     * Returns the first row of the given table.
     */
    @ConcurrentMethod
    TOPS firstBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the first for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS firstBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the first for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS firstBy(ColumnName... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the first for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS firstBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    /**
     * Returns the last row of the given table.
     */
    @ConcurrentMethod
    TOPS lastBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the last for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS lastBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the last for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS lastBy(ColumnName... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and retrieves the last for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS lastBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    /**
     * Produces a single row table with the minimum of each column.
     * <p>
     * When the input table is empty, zero output rows are produced.
     */
    @ConcurrentMethod
    TOPS minBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the min for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS minBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the min for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS minBy(ColumnName... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the min for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS minBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    /**
     * Produces a single row table with the maximum of each column.
     * <p>
     * When the input table is empty, zero output rows are produced.
     */
    @ConcurrentMethod
    TOPS maxBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the max for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy} }
     */
    @ConcurrentMethod
    TOPS maxBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the max for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy} }
     */
    @ConcurrentMethod
    TOPS maxBy(ColumnName... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the max for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy} }
     */
    @ConcurrentMethod
    TOPS maxBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    /**
     * Produces a single row table with the sum of each column.
     * <p>
     * When the input table is empty, zero output rows are produced.
     */
    @ConcurrentMethod
    TOPS sumBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the sum for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS sumBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the sum for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS sumBy(ColumnName... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the sum for the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS sumBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    /**
     * Produces a single row table with the average of each column.
     * <p>
     * When the input table is empty, zero output rows are produced.
     */
    @ConcurrentMethod
    TOPS avgBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the average for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS avgBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the average for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS avgBy(ColumnName... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the average for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS avgBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    /**
     * Produces a single row table with the median of each column.
     * <p>
     * When the input table is empty, zero output rows are produced.
     */
    @ConcurrentMethod
    TOPS medianBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the median for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy} }
     */
    @ConcurrentMethod
    TOPS medianBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the median for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy} }
     */
    @ConcurrentMethod
    TOPS medianBy(ColumnName... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the median for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy} }
     */
    @ConcurrentMethod
    TOPS medianBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    /**
     * Produces a single row table with the standard deviation of each column.
     * <p>
     * When the input table is empty, zero output rows are produced.
     */
    @ConcurrentMethod
    TOPS stdBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the standard deviation for the rest
     * of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS stdBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the standard deviation for the rest
     * of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS stdBy(ColumnName... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the standard deviation for the rest
     * of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS stdBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    /**
     * Produces a single row table with the variance of each column.
     * <p>
     * When the input table is empty, zero output rows are produced.
     */
    @ConcurrentMethod
    TOPS varBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the variance for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS varBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the variance for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS varBy(ColumnName... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the variance for the rest of the
     * fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS varBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    /**
     * Produces a single row table with the absolute sum of each column.
     * <p>
     * When the input table is empty, zero output rows are produced.
     */
    @ConcurrentMethod
    TOPS absSumBy();

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the sum of the absolute values for
     * the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS absSumBy(String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the sum of the absolute values for
     * the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS absSumBy(ColumnName... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the sum of the absolute values for
     * the rest of the fields
     *
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS absSumBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

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
    TOPS wsumBy(String weightColumn);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted sum using weightColumn
     * for the rest of the fields
     * <p>
     * If the weight column is a floating point type, all result columns will be doubles. If the weight column is an
     * integral type, all integral input columns will have long results and all floating point input columns will have
     * double results.
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS wsumBy(String weightColumn, String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted sum using weightColumn
     * for the rest of the fields
     * <p>
     * If the weight column is a floating point type, all result columns will be doubles. If the weight column is an
     * integral type, all integral input columns will have long results and all floating point input columns will have
     * double results.
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS wsumBy(String weightColumn, ColumnName... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted sum using weightColumn
     * for the rest of the fields
     * <p>
     * If the weight column is a floating point type, all result columns will be doubles. If the weight column is an
     * integral type, all integral input columns will have long results and all floating point input columns will have
     * double results.
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS wsumBy(String weightColumn, Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    /**
     * Produces a single row table with the weighted average using weightColumn for the rest of the fields
     * <p>
     * When the input table is empty, zero output rows are produced.
     *
     * @param weightColumn the column to use for the weight
     */
    @ConcurrentMethod
    TOPS wavgBy(String weightColumn);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted average using
     * weightColumn for the rest of the fields
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS wavgBy(String weightColumn, String... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted average using
     * weightColumn for the rest of the fields
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS wavgBy(String weightColumn, ColumnName... groupByColumns);

    /**
     * Groups the data column according to <code>groupByColumns</code> and computes the weighted average using
     * weightColumn for the rest of the fields
     *
     * @param weightColumn the column to use for the weight
     * @param groupByColumns The grouping columns as in {@link TableOperations#groupBy}
     */
    @ConcurrentMethod
    TOPS wavgBy(String weightColumn, Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    /**
     * Ungroups a table by expanding all columns of arrays or vectors into columns of singular values, creating one row
     * in the output table for each value in the columns to be ungrouped. Columns that are not ungrouped have their
     * values duplicated in each output row corresponding to a given input row. All arrays and vectors must be the same
     * size.
     *
     * @return the ungrouped table
     */
    TOPS ungroup();

    /**
     * Ungroups a table by expanding all columns of arrays or vectors into columns of singular values, creating one row
     * in the output table for each value in the columns to be ungrouped. Columns that are not ungrouped have their
     * values duplicated in each output row corresponding to a given input row.
     *
     * @param nullFill indicates if the ungrouped table should allow disparate sized arrays filling shorter columns with
     *        null values. If set to false, then all arrays should be the same length.
     * @return the ungrouped table
     */
    TOPS ungroup(boolean nullFill);

    /**
     * Ungroups a table by expanding columns of arrays or vectors into columns of singular values, creating one row in
     * the output table for each value in the columns to be ungrouped. Columns that are not ungrouped have their values
     * duplicated in each output row corresponding to a given input row. The arrays and vectors must be the same size.
     *
     * @param columnsToUngroup the columns to ungroup
     * @return the ungrouped table
     */
    TOPS ungroup(String... columnsToUngroup);

    /**
     * Ungroups a table by expanding columns of arrays or vectors into columns of singular values, creating one row in
     * the output table for each value in the columns to be ungrouped. Columns that are not ungrouped have their values
     * duplicated in each output row corresponding to a given input row.
     *
     * @param nullFill indicates if the ungrouped table should allow disparate sized arrays filling shorter columns with
     *        null values. If set to false, then all arrays should be the same length.
     * @param columnsToUngroup the columns to ungroup
     * @return the ungrouped table
     */
    TOPS ungroup(boolean nullFill, String... columnsToUngroup);

    /**
     * Ungroups a table by expanding columns of arrays or vectors into columns of singular values, creating one row in
     * the output table for each value in the columns to be ungrouped. Columns that are not ungrouped have their values
     * duplicated in each output row corresponding to a given input row.
     *
     * @param nullFill indicates if the ungrouped table should allow disparate sized arrays filling shorter columns with
     *        null values. If set to false, then all arrays should be the same length.
     * @param columnsToUngroup the columns to ungroup
     * @return the ungrouped table
     */
    TOPS ungroup(boolean nullFill, Collection<? extends ColumnName> columnsToUngroup);

    // -------------------------------------------------------------------------------------------
}
