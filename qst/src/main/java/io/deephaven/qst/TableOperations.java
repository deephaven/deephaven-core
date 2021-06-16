package io.deephaven.qst;

import io.deephaven.qst.table.Filter;
import io.deephaven.qst.table.JoinAddition;
import io.deephaven.qst.table.JoinMatch;
import io.deephaven.qst.table.Selectable;
import io.deephaven.qst.table.agg.Aggregation;
import java.util.Collection;

public interface TableOperations<SELF_TYPE extends TableOperations<SELF_TYPE, TABLE>, TABLE> {

    // -------------------------------------------------------------------------------------------

    SELF_TYPE where(String... filters);

    SELF_TYPE where(Collection<String> filters);

    SELF_TYPE where2(Collection<Filter> filters);

    // -------------------------------------------------------------------------------------------

    SELF_TYPE whereIn(TABLE rightTable, String... columnsToMatch);

    SELF_TYPE whereIn(TABLE rightTable, Collection<String> columnsToMatch);

    SELF_TYPE whereIn2(TABLE rightTable, Collection<JoinMatch> columnsToMatch);

    // -------------------------------------------------------------------------------------------

    SELF_TYPE whereNotIn(TABLE rightTable, String... columnsToMatch);

    SELF_TYPE whereNotIn(TABLE rightTable, Collection<String> columnsToMatch);

    SELF_TYPE whereNotIn2(TABLE rightTable, Collection<JoinMatch> columnsToMatch);

    // -------------------------------------------------------------------------------------------

    SELF_TYPE head(long size);

    SELF_TYPE tail(long size);

    // -------------------------------------------------------------------------------------------

    SELF_TYPE view(String... columns);

    SELF_TYPE view(Collection<String> columns);

    SELF_TYPE view2(Collection<Selectable> columns);

    // -------------------------------------------------------------------------------------------

    SELF_TYPE updateView(String... columns);

    SELF_TYPE updateView(Collection<String> columns);

    SELF_TYPE updateView2(Collection<Selectable> columns);

    // -------------------------------------------------------------------------------------------

    SELF_TYPE update(String... columns);

    SELF_TYPE update(Collection<String> columns);

    SELF_TYPE update2(Collection<Selectable> columns);

    // -------------------------------------------------------------------------------------------

    SELF_TYPE select();

    SELF_TYPE select(String... columns);

    SELF_TYPE select(Collection<String> columns);

    SELF_TYPE select2(Collection<Selectable> columns);

    // -------------------------------------------------------------------------------------------

    SELF_TYPE naturalJoin2(TABLE rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    SELF_TYPE naturalJoin(TABLE rightTable, Collection<String> columnsToMatch,
        Collection<String> columnsToAdd);

    SELF_TYPE naturalJoin(TABLE rightTable, Collection<String> columnsToMatch);

    SELF_TYPE naturalJoin(TABLE rightTable, String columnsToMatch);

    SELF_TYPE naturalJoin(TABLE rightTable, String columnsToMatch, String columnsToAdd);

    // -------------------------------------------------------------------------------------------

    SELF_TYPE exactJoin2(TABLE rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    SELF_TYPE exactJoin(TABLE rightTable, Collection<String> columnsToMatch,
        Collection<String> columnsToAdd);

    SELF_TYPE exactJoin(TABLE rightTable, Collection<String> columnsToMatch);

    SELF_TYPE exactJoin(TABLE rightTable, String columnsToMatch);

    SELF_TYPE exactJoin(TABLE rightTable, String columnsToMatch, String columnsToAdd);

    // -------------------------------------------------------------------------------------------

    SELF_TYPE join2(TABLE rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    SELF_TYPE join(TABLE rightTable, Collection<String> columnsToMatch,
        Collection<String> columnsToAdd);

    SELF_TYPE join(TABLE rightTable, String columnsToMatch);

    SELF_TYPE join(TABLE rightTable, String columnsToMatch, String columnsToAdd);

    // -------------------------------------------------------------------------------------------

    SELF_TYPE by();

    SELF_TYPE by(String... groupByColumns);

    SELF_TYPE by(Collection<String> groupByColumns);

    SELF_TYPE by2(Collection<Selectable> groupByColumns);

    SELF_TYPE by(Collection<Selectable> groupByColumns, Collection<Aggregation> aggregations);

    // -------------------------------------------------------------------------------------------

    TABLE toTable();
}
