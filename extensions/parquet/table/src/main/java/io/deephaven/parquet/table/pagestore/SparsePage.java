//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;

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
    private final RowSet rows;
    /** An array of the values of {@link #rows}, in order. */
    private final Object values;
    /**
     * The first row of every {@link #CHECKPOINT_INTERVAL}th range of {@link #rows}, starting with the first, so that
     * finding a row's position takes a binary search and a few steps, rather than a walk over every earlier range: a
     * deep viewport scroll over sorted, scattered rows reads 2-3x slower without them.
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
     * @return The number of cached rows
     */
    int size() {
        return rows.intSize();
    }

    /**
     * @return An array of the cached values, in row order; positions come from {@link Cursor#position()}
     */
    Object values() {
        return values;
    }

    /**
     * @return A cursor over the cached rows, to be closed after use
     */
    Cursor cursor() {
        return new Cursor(this);
    }

    /**
     * @param rows Page-relative rows; ownership passes to this method
     * @param values An array of at least {@code rows.size()} values, in the order of {@code rows}
     * @return A page holding this page's rows and {@code rows}
     */
    SparsePage<ATTR> merge(
            @NotNull final WritableRowSet rows,
            @NotNull final Object values,
            @NotNull final ChunkType chunkType,
            @NotNull final RunCopyKernel kernel) {
        final WritableRowSet union = this.rows.union(rows);
        final Object merged = Array.newInstance(values.getClass().getComponentType(), union.intSize());
        final WritableChunk<ATTR> mergedChunk = chunkType.writableChunkWrap(merged, 0, union.intSize());
        try (final RowSet currentSource = RowSetFactory.flat(this.rows.size());
                final RowSet currentDestination = union.invert(this.rows);
                final RowSet rowsSource = RowSetFactory.flat(rows.size());
                final RowSet rowsDestination = union.invert(rows)) {
            copyPositions(kernel, this.values, currentSource, mergedChunk, currentDestination);
            copyPositions(kernel, values, rowsSource, mergedChunk, rowsDestination);
        }
        rows.close();
        return new SparsePage<>(union, merged);
    }

    /**
     * Copy the values at {@code sourcePositions} of {@code source} to the corresponding {@code destinationPositions} of
     * {@code destination}. The two position sets have the same size.
     */
    private static void copyPositions(
            @NotNull final RunCopyKernel kernel,
            @NotNull final Object source,
            @NotNull final RowSet sourcePositions,
            @NotNull final WritableChunk<?> destination,
            @NotNull final RowSet destinationPositions) {
        try (final RowSet.RangeIterator sourceRanges = sourcePositions.rangeIterator();
                final RowSet.RangeIterator destinationRanges = destinationPositions.rangeIterator();
                final RunBatch runs = new RunBatch()) {
            runs.begin(kernel, source, destination);
            // Positions in an array and a chunk, so they fit in an int.
            int sourcePos = 0;
            int sourceEnd = -1;
            int destinationPos = 0;
            int destinationEnd = -1;
            while (sourcePos <= sourceEnd || sourceRanges.hasNext()) {
                if (sourcePos > sourceEnd) {
                    sourceRanges.next();
                    sourcePos = (int) sourceRanges.currentRangeStart();
                    sourceEnd = (int) sourceRanges.currentRangeEnd();
                }
                if (destinationPos > destinationEnd) {
                    destinationRanges.next();
                    destinationPos = (int) destinationRanges.currentRangeStart();
                    destinationEnd = (int) destinationRanges.currentRangeEnd();
                }
                final int length = Math.min(sourceEnd - sourcePos, destinationEnd - destinationPos) + 1;
                runs.add(sourcePos, destinationPos, length);
                sourcePos += length;
                destinationPos += length;
            }
            runs.finish();
        }
    }

    /**
     * @return The index of the last checkpoint at or before {@code row}, or {@code -1} if none is
     */
    private int checkpointBefore(final int row) {
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

    /**
     * Splits requested rows, in increasing order, into segments that are all cached or all not, tracking each cached
     * segment's position in {@link #values()}. Steps over nearby ranges, summing their sizes, and jumps to the last
     * checkpoint before a far row.
     */
    static final class Cursor implements SafeCloseable {
        private final SparsePage<?> page;
        private final RowSet.RangeIterator ranges;
        /** The current range, or {@code -1} before the first seek. */
        private int start = -1;
        private int end = -1;
        /** The position of {@link #start} in the page's rows. */
        private int startPosition;
        private boolean exhausted;
        private boolean segmentCached;
        private int segmentPosition;

        private Cursor(@NotNull final SparsePage<?> page) {
            this.page = page;
            ranges = page.rows.rangeIterator();
        }

        /**
         * Find the segment that starts at {@code first}, which must not precede an earlier segment's.
         *
         * @return The segment's last row, at most {@code last}
         */
        int segment(final int first, final int last) {
            final boolean found = seek(first);
            if (found && start <= first) {
                segmentCached = true;
                segmentPosition = startPosition + first - start;
                return Math.min(last, end);
            }
            segmentCached = false;
            return found ? Math.min(last, start - 1) : last;
        }

        /**
         * @return Whether the last {@link #segment}'s rows are cached
         */
        boolean cached() {
            return segmentCached;
        }

        /**
         * @return The position in {@link #values()} of the last {@link #segment}'s first row, if it is cached
         */
        int position() {
            return segmentPosition;
        }

        /**
         * Move to the first range that ends at or after {@code key}, which must not precede an earlier seek's.
         *
         * @return Whether there is such a range
         */
        private boolean seek(final int key) {
            if (end >= key) {
                return true;
            }
            if (exhausted) {
                return false;
            }
            final int checkpoint = page.checkpointBefore(key);
            if (checkpoint >= 0 && page.checkpointRows[checkpoint] > start) {
                // Past the current range: jump, rather than step over up to a checkpoint interval's ranges.
                // A checkpoint row starts one of the page's ranges, so the advance finds it.
                Assert.assertion(ranges.advance(page.checkpointRows[checkpoint]), "ranges.advance(checkpointRow)");
                readRange();
                startPosition = page.checkpointPositions[checkpoint];
            } else if (start < 0) {
                ranges.next();
                readRange();
                startPosition = 0;
            }
            while (end < key) {
                if (!ranges.hasNext()) {
                    exhausted = true;
                    return false;
                }
                startPosition += end - start + 1;
                ranges.next();
                readRange();
            }
            return true;
        }

        private void readRange() {
            // Page-relative, so they fit in an int.
            start = (int) ranges.currentRangeStart();
            end = (int) ranges.currentRangeEnd();
        }

        @Override
        public void close() {
            ranges.close();
        }
    }
}
