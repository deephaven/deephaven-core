package io.deephaven.api;

import io.deephaven.api.agg.Aggregation;
import java.util.Collection;
import java.util.List;

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

    TOPS sort(String... columnsToSortBy);

    TOPS sort(List<String> columnsToSortBy);

    TOPS sortDescending(String... columnsToSortBy);

    TOPS sortDescending(List<String> columnsToSortBy);

    TOPS sort2(List<SortColumn> columnsToSortBy);

    // -------------------------------------------------------------------------------------------

    TOPS where(String... filters);

    TOPS where(Collection<String> filters);

    TOPS where2(Collection<Filter> filters);

    // -------------------------------------------------------------------------------------------

    TOPS whereIn(TABLE rightTable, String... columnsToMatch);

    TOPS whereIn(TABLE rightTable, Collection<String> columnsToMatch);

    TOPS whereIn2(TABLE rightTable, Collection<JoinMatch> columnsToMatch);

    // -------------------------------------------------------------------------------------------

    TOPS whereNotIn(TABLE rightTable, String... columnsToMatch);

    TOPS whereNotIn(TABLE rightTable, Collection<String> columnsToMatch);

    TOPS whereNotIn2(TABLE rightTable, Collection<JoinMatch> columnsToMatch);

    // -------------------------------------------------------------------------------------------

    TOPS view(String... columns);

    TOPS view(Collection<String> columns);

    TOPS view2(Collection<Selectable> columns);

    // -------------------------------------------------------------------------------------------

    TOPS updateView(String... columns);

    TOPS updateView(Collection<String> columns);

    TOPS updateView2(Collection<Selectable> columns);

    // -------------------------------------------------------------------------------------------

    TOPS update(String... columns);

    TOPS update(Collection<String> columns);

    TOPS update2(Collection<Selectable> columns);

    // -------------------------------------------------------------------------------------------

    TOPS select();

    TOPS select(String... columns);

    TOPS select(Collection<String> columns);

    TOPS select2(Collection<Selectable> columns);

    // -------------------------------------------------------------------------------------------

    TOPS naturalJoin(TABLE rightTable, String columnsToMatch);

    TOPS naturalJoin(TABLE rightTable, String columnsToMatch, String columnsToAdd);

    TOPS naturalJoin(TABLE rightTable, Collection<String> columnsToMatch);

    TOPS naturalJoin(TABLE rightTable, Collection<String> columnsToMatch,
        Collection<String> columnsToAdd);

    TOPS naturalJoin2(TABLE rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    // -------------------------------------------------------------------------------------------

    TOPS exactJoin(TABLE rightTable, String columnsToMatch);

    TOPS exactJoin(TABLE rightTable, String columnsToMatch, String columnsToAdd);

    TOPS exactJoin(TABLE rightTable, Collection<String> columnsToMatch);

    TOPS exactJoin(TABLE rightTable, Collection<String> columnsToMatch,
        Collection<String> columnsToAdd);

    TOPS exactJoin2(TABLE rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    // -------------------------------------------------------------------------------------------

    TOPS join(TABLE rightTable, Collection<String> columnsToMatch, Collection<String> columnsToAdd);

    TOPS join(TABLE rightTable, String columnsToMatch);

    TOPS join(TABLE rightTable, String columnsToMatch, String columnsToAdd);

    TOPS join2(TABLE rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    // -------------------------------------------------------------------------------------------

    TOPS by();

    TOPS by(String... groupByColumns);

    TOPS by(Collection<String> groupByColumns);

    TOPS by2(Collection<Selectable> groupByColumns);

    TOPS by(Collection<Selectable> groupByColumns, Collection<Aggregation> aggregations);

    // -------------------------------------------------------------------------------------------

    TABLE toTable();
}
