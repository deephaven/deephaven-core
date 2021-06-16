package io.deephaven.qst;

import io.deephaven.qst.table.Filter;
import io.deephaven.qst.table.JoinAddition;
import io.deephaven.qst.table.JoinMatch;
import io.deephaven.qst.table.Selectable;
import io.deephaven.qst.table.agg.Aggregation;
import java.util.Collection;

public interface TableOperations<SELF extends TableOperations<SELF, TABLE>, TABLE> {

    // -------------------------------------------------------------------------------------------

    SELF where(String... filters);

    SELF where(Collection<String> filters);

    SELF where2(Collection<Filter> filters);

    // -------------------------------------------------------------------------------------------

    SELF whereIn(TABLE rightTable, String... columnsToMatch);

    SELF whereIn(TABLE rightTable, Collection<String> columnsToMatch);

    SELF whereIn2(TABLE rightTable, Collection<JoinMatch> columnsToMatch);

    // -------------------------------------------------------------------------------------------

    SELF whereNotIn(TABLE rightTable, String... columnsToMatch);

    SELF whereNotIn(TABLE rightTable, Collection<String> columnsToMatch);

    SELF whereNotIn2(TABLE rightTable, Collection<JoinMatch> columnsToMatch);

    // -------------------------------------------------------------------------------------------

    SELF head(long size);

    SELF tail(long size);

    // -------------------------------------------------------------------------------------------

    SELF view(String... columns);

    SELF view(Collection<String> columns);

    SELF view2(Collection<Selectable> columns);

    // -------------------------------------------------------------------------------------------

    SELF updateView(String... columns);

    SELF updateView(Collection<String> columns);

    SELF updateView2(Collection<Selectable> columns);

    // -------------------------------------------------------------------------------------------

    SELF update(String... columns);

    SELF update(Collection<String> columns);

    SELF update2(Collection<Selectable> columns);

    // -------------------------------------------------------------------------------------------

    SELF select();

    SELF select(String... columns);

    SELF select(Collection<String> columns);

    SELF select2(Collection<Selectable> columns);

    // -------------------------------------------------------------------------------------------

    SELF naturalJoin2(TABLE rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    SELF naturalJoin(TABLE rightTable, Collection<String> columnsToMatch,
        Collection<String> columnsToAdd);

    SELF naturalJoin(TABLE rightTable, Collection<String> columnsToMatch);

    SELF naturalJoin(TABLE rightTable, String columnsToMatch);

    SELF naturalJoin(TABLE rightTable, String columnsToMatch, String columnsToAdd);

    // -------------------------------------------------------------------------------------------

    SELF exactJoin2(TABLE rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    SELF exactJoin(TABLE rightTable, Collection<String> columnsToMatch,
        Collection<String> columnsToAdd);

    SELF exactJoin(TABLE rightTable, Collection<String> columnsToMatch);

    SELF exactJoin(TABLE rightTable, String columnsToMatch);

    SELF exactJoin(TABLE rightTable, String columnsToMatch, String columnsToAdd);

    // -------------------------------------------------------------------------------------------

    SELF join2(TABLE rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd);

    SELF join(TABLE rightTable, Collection<String> columnsToMatch, Collection<String> columnsToAdd);

    SELF join(TABLE rightTable, String columnsToMatch);

    SELF join(TABLE rightTable, String columnsToMatch, String columnsToAdd);

    // -------------------------------------------------------------------------------------------

    SELF by();

    SELF by(String... groupByColumns);

    SELF by(Collection<String> groupByColumns);

    SELF by2(Collection<Selectable> groupByColumns);

    SELF by(Collection<Selectable> groupByColumns, Collection<Aggregation> aggregations);

    // -------------------------------------------------------------------------------------------

    TABLE toTable();
}
