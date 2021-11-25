/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.select;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * This will filter a table on a DateTime column for all rows greater than "now" according to a supplied clock. It does
 * not require any pre-sorting of the input table, instead preserving relative order in the initial output and each
 * subsequent run. Relative to SortedClockFilter, this implementation may require less overall storage and do less
 * overall work for tables with relatively few monotonically nondecreasing ranges (that is, m (number of ranges)
 * &lt;&lt;&lt; n (size in rows)), but it will do more work on run().
 */
public class UnsortedClockFilter extends ClockFilter {

    private static final int INITIAL_RANGE_QUEUE_CAPACITY = 1024;

    private Queue<Range> rangesByNextTime;

    public UnsortedClockFilter(@NotNull final String columnName,
            @NotNull final Clock clock,
            final boolean refreshing) {
        super(columnName, clock, refreshing);
    }

    @Override
    public boolean requiresSorting() {
        return false;
    }

    @Override
    public String[] getSortColumns() {
        return null;
    }

    @Override
    public void sortingDone() {}

    @Override
    public UnsortedClockFilter copy() {
        return new UnsortedClockFilter(columnName, clock, isRefreshing());
    }

    private class RangeComparator implements Comparator<Range> {

        @Override
        public int compare(final Range r1, final Range r2) {
            Assert.assertion(!r1.isEmpty(), "!r1.isEmpty()");
            Assert.assertion(!r2.isEmpty(), "!r2.isEmpty()");
            return QueryLanguageFunctionUtils.compareTo(nanosColumnSource.getLong(r1.nextKey),
                    nanosColumnSource.getLong(r2.nextKey));
        }
    }

    @Override
    @Nullable
    protected WritableRowSet initializeAndGetInitialIndex(@NotNull final RowSet selection,
            @NotNull final RowSet fullSet,
            @NotNull final Table table) {
        rangesByNextTime = new PriorityQueue<>(INITIAL_RANGE_QUEUE_CAPACITY, new RangeComparator());

        if (selection.isEmpty()) {
            return null;
        }

        final RowSetBuilderSequential addedBuilder = RowSetFactory.builderSequential();

        final long nowNanos = clock.currentTimeMicros() * 1000L;
        final RowSet.Iterator selectionIterator = selection.iterator();

        // Initial current range begins and ends at the first key in the selection (which must exist because we've
        // already tested non-emptiness).
        long activeRangeFirstKey = selectionIterator.nextLong();
        long activeRangeLastKey = activeRangeFirstKey;
        long previousValue = nanosColumnSource.getLong(activeRangeFirstKey);
        boolean activeRangeIsDeferred = QueryLanguageFunctionUtils.greater(previousValue, nowNanos);

        while (selectionIterator.hasNext()) {
            final long currentKey = selectionIterator.nextLong();
            final long currentValue = nanosColumnSource.getLong(currentKey);
            final boolean currentIsDeferred = QueryLanguageFunctionUtils.greater(currentValue, nowNanos);

            // If we observe a change in deferral status, a discontinuity in the keys, or a decrease in the values, we
            // have entered a new range
            if (currentIsDeferred != activeRangeIsDeferred || currentKey != activeRangeLastKey + 1
                    || QueryLanguageFunctionUtils.less(currentValue, previousValue)) {
                // Add the current range, as appropriate
                if (activeRangeIsDeferred) {
                    rangesByNextTime.add(new Range(activeRangeFirstKey, activeRangeLastKey));
                } else {
                    addedBuilder.appendRange(activeRangeFirstKey, activeRangeLastKey);
                }
                // Start the new range
                activeRangeFirstKey = currentKey;
                activeRangeIsDeferred = currentIsDeferred;
            }

            activeRangeLastKey = currentKey;
            previousValue = currentValue;
        }

        // Add the final range, as appropriate
        if (activeRangeIsDeferred) {
            rangesByNextTime.add(new Range(activeRangeFirstKey, activeRangeLastKey));
        } else {
            addedBuilder.appendRange(activeRangeFirstKey, activeRangeLastKey);
        }

        return addedBuilder.build();
    }

    @Override
    @Nullable
    protected WritableRowSet updateAndGetAddedIndex() {
        if (rangesByNextTime.isEmpty()) {
            return null;
        }
        final long nowNanos = clock.currentTimeMicros() * 1000L;
        RowSetBuilderRandom addedBuilder = null;
        Range nextRange;
        RowSetBuilderRandom resultBuilder;
        while ((nextRange = rangesByNextTime.peek()) != null && (resultBuilder =
                nextRange.consumeKeysAndAppendAdded(nanosColumnSource, nowNanos, addedBuilder)) != null) {
            addedBuilder = resultBuilder;
            Assert.eq(nextRange, "nextRange", rangesByNextTime.remove(), "rangesByNextTime.remove()");
            if (!nextRange.isEmpty()) {
                rangesByNextTime.add(nextRange);
            }
        }
        return addedBuilder == null ? null : addedBuilder.build();
    }
}
