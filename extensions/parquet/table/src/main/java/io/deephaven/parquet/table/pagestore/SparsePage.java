//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

/**
 * The decoded values of some of a page's rows, cached in place of the whole page. Immutable.
 */
final class SparsePage<ATTR extends Any> extends PageCache.IntrusivePage<ATTR> {

    /**
     * The number of ranges of {@link #rows} between checkpoints, from the {@code SparsePage.checkpointInterval}
     * configuration property: smaller finds positions in fewer steps, at eight bytes per checkpoint.
     */
    static final int CHECKPOINT_INTERVAL = Math.max(1, Configuration.getInstance()
            .getIntegerForClassWithDefault(SparsePage.class, "checkpointInterval", 32));

    /** Page-relative rows. */
    final RowSet rows;
    /** An array of the values of {@link #rows}, in order. */
    final Object values;
    /**
     * The first row of every {@link #CHECKPOINT_INTERVAL}th range of {@link #rows}, starting with the first, so that
     * finding a row's position takes a binary search and a few steps: {@link RowSet#find} scans a small set linearly.
     */
    private final int[] checkpointRows;
    /** The position in {@link #rows} of each of {@link #checkpointRows}. */
    private final int[] checkpointPositions;

    SparsePage(@NotNull final RowSet rows, @NotNull final Object values) {
        this.rows = rows;
        this.values = values;
        int rangeCount = 0;
        try (final RowSet.RangeIterator ranges = rows.rangeIterator()) {
            for (; ranges.hasNext(); ++rangeCount) {
                ranges.next();
            }
        }
        final int checkpoints = (rangeCount + CHECKPOINT_INTERVAL - 1) / CHECKPOINT_INTERVAL;
        checkpointRows = new int[checkpoints];
        checkpointPositions = new int[checkpoints];
        try (final RowSet.RangeIterator ranges = rows.rangeIterator()) {
            int position = 0;
            for (int range = 0; ranges.hasNext(); ++range) {
                ranges.next();
                if (range % CHECKPOINT_INTERVAL == 0) {
                    checkpointRows[range / CHECKPOINT_INTERVAL] = Math.toIntExact(ranges.currentRangeStart());
                    checkpointPositions[range / CHECKPOINT_INTERVAL] = position;
                }
                position += (int) (ranges.currentRangeEnd() - ranges.currentRangeStart() + 1);
            }
        }
    }

    /**
     * @return The index of the last checkpoint at or before {@code row}, or {@code -1} if none is
     */
    int checkpointBefore(final long row) {
        int lo = 0;
        int hi = checkpointRows.length;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (checkpointRows[mid] <= row) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo - 1;
    }

    long checkpointRow(final int checkpoint) {
        return checkpointRows[checkpoint];
    }

    long checkpointPosition(final int checkpoint) {
        return checkpointPositions[checkpoint];
    }
}
