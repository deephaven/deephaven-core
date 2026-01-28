//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * This will filter a table on an Instant column for all rows greater than "now" according to a supplied clock. It
 * requires sorting of the input table according to the specified timestamp column, leveraging this for a very efficient
 * implementation (albeit one that requires sorting first) and an output sequence that is monotonically nondecreasing in
 * the specified column.
 */
public class SortedClockFilter extends ClockFilter {

    private boolean sortingDone;
    /**
     * the currently active range we are working on; this must be the first range in the table (because we are
     * guaranteed to be sorted).
     */
    private Range range;
    /**
     * A list of ranges that were not immediately mergeable with the first range. They are ordered according to key, and
     * we pop one off at a time until all the rows are consumed. We can expect at most one range per initialization
     * thread to be in this list.
     */
    private final List<Range> pendingRanges = new LinkedList<>();

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

        // we must have a contiguous region in our rowset, there may be multiple ranges that are created; but they
        // should all merge together, with only the first range actually producing any useful result
        Require.requirement(selection.isContiguous(), "selection.isContiguous()");

        synchronized (this) {
            if (range == null) {
                range = new Range(selection.firstRowKey(), selection.lastRowKey());
            } else if (!range.merge(selection.firstRowKey(), selection.lastRowKey())) {
                if (range.isBefore(selection.firstRowKey())) {
                    pendingRanges.add(new Range(selection.firstRowKey(), selection.lastRowKey()));
                } else {
                    pendingRanges.add(range);
                    range = new Range(selection.firstRowKey(), selection.lastRowKey());
                }
            }
            return updateAndGetAddedIndex();
        }
    }

    @Override
    @Nullable
    protected synchronized WritableRowSet updateAndGetAddedIndex() {
        if (range == null || range.isEmpty()) {
            return null;
        }
        final RowSetBuilderRandom addedBuilder =
                range.consumeKeysAndAppendAdded(nanosColumnSource, clock.currentTimeNanos(), null);
        if (range.isEmpty()) {
            pendingRanges.sort(Comparator.comparingLong(Range::firstKey));
            while (range.isEmpty() && !pendingRanges.isEmpty()) {
                range = pendingRanges.remove(0);
                range.consumeKeysAndAppendAdded(nanosColumnSource, clock.currentTimeNanos(), addedBuilder);
            }
        }
        return addedBuilder == null ? null : addedBuilder.build();
    }
}
