package io.deephaven.api;

import io.deephaven.api.agg.Aggregation;

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

    TOPS snapshot(TABLE rightTable, boolean doInitialSnapshot, String... stampColumns);

    TOPS snapshot(TABLE rightTable, String... stampColumns);

    TOPS snapshot(TABLE rightTable, boolean doInitialSnapshot, Collection<ColumnName> stampColumns);

    // -------------------------------------------------------------------------------------------

    TOPS sort(String... columnsToSortBy);

    TOPS sortDescending(String... columnsToSortBy);

    TOPS sort(Collection<SortColumn> columnsToSortBy);

    // -------------------------------------------------------------------------------------------

    TOPS where(String... filters);

    TOPS where(Collection<Filter> filters);

    // -------------------------------------------------------------------------------------------

    TOPS whereIn(TABLE rightTable, String... columnsToMatch);

    TOPS whereIn(TABLE rightTable, Collection<JoinMatch> columnsToMatch);

    // -------------------------------------------------------------------------------------------

    TOPS whereNotIn(TABLE rightTable, String... columnsToMatch);

    TOPS whereNotIn(TABLE rightTable, Collection<JoinMatch> columnsToMatch);

    // -------------------------------------------------------------------------------------------

    TOPS view(String... columns);

    TOPS view(Collection<Selectable> columns);

    // -------------------------------------------------------------------------------------------

    TOPS updateView(String... columns);

    TOPS updateView(Collection<Selectable> columns);

    // -------------------------------------------------------------------------------------------

    TOPS update(String... columns);

    TOPS update(Collection<Selectable> columns);

    // -------------------------------------------------------------------------------------------

    TOPS select(String... columns);

    TOPS select(Collection<Selectable> columns);

    // -------------------------------------------------------------------------------------------

    TOPS naturalJoin(TABLE rightTable, String columnsToMatch);

    TOPS naturalJoin(TABLE rightTable, String columnsToMatch, String columnsToAdd);

    TOPS naturalJoin(TABLE rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    // -------------------------------------------------------------------------------------------

    TOPS exactJoin(TABLE rightTable, String columnsToMatch);

    TOPS exactJoin(TABLE rightTable, String columnsToMatch, String columnsToAdd);

    TOPS exactJoin(TABLE rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    // -------------------------------------------------------------------------------------------

    TOPS leftJoin(TABLE rightTable, String columnsToMatch);

    TOPS leftJoin(TABLE rightTable, String columnsToMatch, String columnsToAdd);

    TOPS leftJoin(TABLE rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    // -------------------------------------------------------------------------------------------

    TOPS join(TABLE rightTable, String columnsToMatch);

    TOPS join(TABLE rightTable, String columnsToMatch, String columnsToAdd);

    TOPS join(TABLE rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    // -------------------------------------------------------------------------------------------

    TOPS aj(TABLE rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    TOPS aj(TABLE rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd, AsOfJoinRule asOfJoinRule);

    // -------------------------------------------------------------------------------------------

    TOPS raj(TABLE rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    TOPS raj(TABLE rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd, RightAsOfJoinRule rightAsOfJoinRule);

    // -------------------------------------------------------------------------------------------

    TOPS by();

    TOPS by(String... groupByColumns);

    TOPS by(Collection<Selectable> groupByColumns);

    TOPS by(Collection<Selectable> groupByColumns, Collection<Aggregation> aggregations);

    // -------------------------------------------------------------------------------------------
}
