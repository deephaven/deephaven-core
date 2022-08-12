/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.UpdateByControl;

import java.util.Collection;

/**
 * Table operations is a user-accessible api for modifying tables or building up table operations.
 *
 * @param <TOPS> the table operations type
 * @param <TABLE> the table type
 */
public interface TableOperations<TOPS extends TableOperations<TOPS, TABLE>, TABLE> {

    // -------------------------------------------------------------------------------------------

    TOPS head(long size);

    TOPS tail(long size);

    // -------------------------------------------------------------------------------------------

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

    TOPS sort(String... columnsToSortBy);

    TOPS sortDescending(String... columnsToSortBy);

    TOPS sort(Collection<SortColumn> columnsToSortBy);

    // -------------------------------------------------------------------------------------------

    TOPS where(String... filters);

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

    TOPS view(String... columns);

    TOPS view(Collection<? extends Selectable> columns);

    // -------------------------------------------------------------------------------------------

    TOPS updateView(String... columns);

    TOPS updateView(Collection<? extends Selectable> columns);

    // -------------------------------------------------------------------------------------------

    TOPS update(String... columns);

    TOPS update(Collection<? extends Selectable> columns);

    // -------------------------------------------------------------------------------------------

    TOPS lazyUpdate(String... columns);

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

    TOPS groupBy();

    TOPS groupBy(String... groupByColumns);

    TOPS groupBy(Collection<? extends ColumnName> groupByColumns);

    // -------------------------------------------------------------------------------------------

    TOPS aggAllBy(AggSpec spec);

    TOPS aggAllBy(AggSpec spec, String... groupByColumns);

    TOPS aggAllBy(AggSpec spec, ColumnName... groupByColumns);

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
    TOPS aggBy(Aggregation aggregation);

    /**
     * Produce an aggregated result by grouping all rows from {@code this} into a single group of rows and applying
     * {@code aggregations} to the result. The result table will have one row if {@code this} has one or more rows, or
     * else zero rows.
     *
     * @param aggregations The {@link Aggregation aggregations} to apply
     * @return A new table aggregating the rows of {@code this}
     */
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
    TOPS aggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty, TABLE initialGroups,
            Collection<? extends ColumnName> groupByColumns);

    // -------------------------------------------------------------------------------------------

    TOPS updateBy(UpdateByOperation operation);

    TOPS updateBy(Collection<? extends UpdateByOperation> operations);

    TOPS updateBy(UpdateByControl control, Collection<? extends UpdateByOperation> operations);

    TOPS updateBy(UpdateByOperation operation, final String... byColumns);

    TOPS updateBy(Collection<? extends UpdateByOperation> operations, final String... byColumns);

    TOPS updateBy(Collection<? extends UpdateByOperation> operations, Collection<? extends ColumnName> byColumns);

    TOPS updateBy(UpdateByControl control, Collection<? extends UpdateByOperation> operations,
            Collection<? extends ColumnName> byColumns);

    // -------------------------------------------------------------------------------------------

    TOPS selectDistinct();

    TOPS selectDistinct(String... columns);

    TOPS selectDistinct(Selectable... columns);

    TOPS selectDistinct(Collection<? extends Selectable> columns);

    // -------------------------------------------------------------------------------------------

    TOPS countBy(String countColumnName);

    TOPS countBy(String countColumnName, String... groupByColumns);

    TOPS countBy(String countColumnName, ColumnName... groupByColumns);

    TOPS countBy(String countColumnName, Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    TOPS firstBy();

    TOPS firstBy(String... groupByColumns);

    TOPS firstBy(ColumnName... groupByColumns);

    TOPS firstBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    TOPS lastBy();

    TOPS lastBy(String... groupByColumns);

    TOPS lastBy(ColumnName... groupByColumns);

    TOPS lastBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    TOPS minBy();

    TOPS minBy(String... groupByColumns);

    TOPS minBy(ColumnName... groupByColumns);

    TOPS minBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    TOPS maxBy();

    TOPS maxBy(String... groupByColumns);

    TOPS maxBy(ColumnName... groupByColumns);

    TOPS maxBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    TOPS sumBy();

    TOPS sumBy(String... groupByColumns);

    TOPS sumBy(ColumnName... groupByColumns);

    TOPS sumBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    TOPS avgBy();

    TOPS avgBy(String... groupByColumns);

    TOPS avgBy(ColumnName... groupByColumns);

    TOPS avgBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    TOPS medianBy();

    TOPS medianBy(String... groupByColumns);

    TOPS medianBy(ColumnName... groupByColumns);

    TOPS medianBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    TOPS stdBy();

    TOPS stdBy(String... groupByColumns);

    TOPS stdBy(ColumnName... groupByColumns);

    TOPS stdBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    TOPS varBy();

    TOPS varBy(String... groupByColumns);

    TOPS varBy(ColumnName... groupByColumns);

    TOPS varBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    TOPS absSumBy();

    TOPS absSumBy(String... groupByColumns);

    TOPS absSumBy(ColumnName... groupByColumns);

    TOPS absSumBy(Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    TOPS wsumBy(String weightColumn);

    TOPS wsumBy(String weightColumn, String... groupByColumns);

    TOPS wsumBy(String weightColumn, ColumnName... groupByColumns);

    TOPS wsumBy(String weightColumn, Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------

    TOPS wavgBy(String weightColumn);

    TOPS wavgBy(String weightColumn, String... groupByColumns);

    TOPS wavgBy(String weightColumn, ColumnName... groupByColumns);

    TOPS wavgBy(String weightColumn, Collection<String> groupByColumns);

    // -------------------------------------------------------------------------------------------
}
