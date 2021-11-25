/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.select;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This will filter a table on a DateTime column for all rows greater than "now" according to a supplied clock. It
 * requires sorting of the input table according to the specified timestamp column, leveraging this for a very efficient
 * implementation (albeit one that requires sorting first) and an output sequence that is monotonically nondecreasing in
 * the specified column.
 */
public class SortedClockFilter extends ClockFilter {

    private boolean sortingDone;
    private Range range;

    public SortedClockFilter(@NotNull final String columnName,
            @NotNull final Clock clock,
            final boolean refreshing) {
        super(columnName, clock, refreshing);
    }

    @Override
    public SortedClockFilter copy() {
        return new SortedClockFilter(columnName, clock, isRefreshing());
    }

    @Override
    public boolean requiresSorting() {
        return !sortingDone;
    }

    @Override
    public String[] getSortColumns() {
        return new String[] {columnName};
    }

    @Override
    public void sortingDone() {
        sortingDone = true;
    }

    @Override
    @Nullable
    protected WritableRowSet initializeAndGetInitialIndex(@NotNull final RowSet selection,
            @NotNull final RowSet fullSet,
            @NotNull final Table table) {
        // External code is required to have sorted according to column before calling this, so we expect the input to
        // be flat. This is not actually a guarantee of the sort() method, but is something that happens to be true
        // because the input table must be historical, and the historical sort implementation uses a
        // ContiguousRowRedirection.
        Require.requirement(table.isFlat(), "table.isFlat()");
        // This must be the first filter in a where-clause of its own, again because of the sort, hence selection must
        // be equal to fullSet.
        // This test as implemented only works because the table is flat.
        Require.requirement(selection.size() == fullSet.size()
                && selection.size() == selection.lastRowKey() - selection.firstRowKey() + 1
                && fullSet.size() == fullSet.lastRowKey() - fullSet.firstRowKey() + 1,
                "selection.size() == fullSet.size() && selection.size() == selection.lastRowKey() - selection.firstRowKey() + 1 && fullSet.size() == fullSet.lastRowKey() - fullSet.firstRowKey() + 1");

        range = new Range(selection.firstRowKey(), selection.lastRowKey());
        return updateAndGetAddedIndex();
    }

    @Override
    @Nullable
    protected WritableRowSet updateAndGetAddedIndex() {
        if (range.isEmpty()) {
            return null;
        }
        final RowSetBuilderRandom addedBuilder =
                range.consumeKeysAndAppendAdded(nanosColumnSource, clock.currentTimeMicros() * 1000L, null);
        return addedBuilder == null ? null : addedBuilder.build();
    }
}
