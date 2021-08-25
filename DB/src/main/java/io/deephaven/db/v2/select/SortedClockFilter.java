/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.select;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This will filter a table on a DBDateTime column for all rows greater than "now" according to a supplied clock. It
 * requires sorting of the input table according to the specified timestamp column, leveraging this for a very efficient
 * implementation (albeit one that requires sorting first) and an output sequence that is monotonically nondecreasing in
 * the specified column.
 */
public class SortedClockFilter extends ClockFilter {

    private boolean sortingDone;
    private Range range;

    public SortedClockFilter(@NotNull final String columnName,
            @NotNull final Clock clock,
            final boolean live) {
        super(columnName, clock, live);
    }

    @Override
    public SortedClockFilter copy() {
        return new SortedClockFilter(columnName, clock, isLive());
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
    protected @Nullable Index initializeAndGetInitialIndex(@NotNull final Index selection, @NotNull final Index fullSet,
            @NotNull final Table table) {
        // External code is required to have sorted according to column before calling this, so we expect the input to
        // be flat. This is not actually a guarantee of the sort() method, but is something that happens to be true
        // because the input table must be historical, and the historical sort implementation uses a
        // ContiguousRedirectionIndex.
        Require.requirement(table.isFlat(), "table.isFlat()");
        // This must be the first filter in a where-clause of its own, again because of the sort, hence selection must
        // be equal to fullSet.
        // This test as implemented only works because the table is flat.
        Require.requirement(selection.size() == fullSet.size()
                && selection.size() == selection.lastKey() - selection.firstKey() + 1
                && fullSet.size() == fullSet.lastKey() - fullSet.firstKey() + 1,
                "selection.size() == fullSet.size() && selection.size() == selection.lastKey() - selection.firstKey() + 1 && fullSet.size() == fullSet.lastKey() - fullSet.firstKey() + 1");

        range = new Range(selection.firstKey(), selection.lastKey());
        return updateAndGetAddedIndex();
    }

    @Override
    protected @Nullable Index updateAndGetAddedIndex() {
        if (range.isEmpty()) {
            return null;
        }
        final Index.RandomBuilder addedBuilder =
                range.consumeKeysAndAppendAdded(nanosColumnSource, clock.currentTimeMicros() * 1000L, null);
        return addedBuilder == null ? null : addedBuilder.getIndex();
    }
}
