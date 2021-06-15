package io.deephaven.qst;

import io.deephaven.qst.table.JoinAddition;
import io.deephaven.qst.table.JoinMatch;
import java.util.Collection;

public interface TableOperations<SELF extends TableOperations<SELF, TABLE>, TABLE> {

    // -------------------------------------------------------------------------------------------

    SELF where(String... filters);

    SELF where(Collection<String> filters);

    // -------------------------------------------------------------------------------------------

    SELF head(long size);

    SELF tail(long size);

    // -------------------------------------------------------------------------------------------

    SELF view(String... columns);

    SELF view(Collection<String> columns);

    // -------------------------------------------------------------------------------------------

    SELF updateView(String... columns);

    SELF updateView(Collection<String> columns);

    // -------------------------------------------------------------------------------------------

    SELF update(String... columns);

    SELF update(Collection<String> columns);

    // -------------------------------------------------------------------------------------------

    SELF select();

    SELF select(String... columns);

    SELF select(Collection<String> columns);

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

    TABLE toTable();
}
