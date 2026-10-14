//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.ssmpercentile;

import io.deephaven.base.ArrayUtil;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.impl.sort.IntSortKernel;
import io.deephaven.engine.table.impl.sort.permute.PermuteKernel;
import io.deephaven.engine.table.impl.sort.timsort.IntIntTimsortKernel;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.table.impl.util.compact.CompactKernel;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.compare.ObjectComparisons;

/**
 * Stages the Object values that a cycle adds to, or removes from, the destinations of a percentile operator, as
 * (destination, value, count) entries sorted by destination and then by value.
 * <p>
 * Values arrive in a window. When the window fills, its values are grouped by destination, each destination's values
 * are sorted and counted, and the result is merged into the entries. The window starts at {@link #INITIAL_WINDOW}
 * values and doubles whenever the entries outnumber it, so each merge costs about as much as the window it absorbs and
 * staging n values costs O(n log n). With few distinct values per destination, the entries and the window both stay
 * small however many values are staged.
 * <p>
 * Null values are dropped. When both the entries and a window hold values for the same destination that compare equal,
 * the merged entry keeps the value from the entries, which the destination received first. {@link #release()} frees
 * every buffer.
 */
final class PercentileStager {
    static final int INITIAL_WINDOW = 1024;

    private static final ChunkType chunkType = ChunkType.Object;
    private static final PermuteKernel permuteKernel = PermuteKernel.makePermuteKernel(ChunkType.Object);

    private final CompactKernel compactKernel;

    private int windowCapacity = INITIAL_WINDOW;
    private WritableChunk<Values> windowValues;
    private WritableIntChunk<RowKeys> windowDestinations;

    // scratch for flushing the window, sized to the window capacity
    private WritableIntChunk<ChunkPositions> positions;
    private IntSortKernel<RowKeys, ChunkPositions> sortContext;
    private WritableChunk<Values> groupedValues;
    private WritableIntChunk<ChunkLengths> groupedCounts;
    private WritableIntChunk<ChunkPositions> runStarts;
    private WritableIntChunk<ChunkLengths> runLengths;

    private WritableChunk<Values> values;
    private WritableIntChunk<ChunkLengths> counts;
    private WritableIntChunk<RowKeys> destinations;


    /**
     * @param compactKernel the Object compact kernel that sorts and counts each destination's values
     */
    PercentileStager(final CompactKernel compactKernel) {
        this.compactKernel = compactKernel;
    }

    /**
     * Stage {@code source[start, start + length)} for {@code destination}.
     */
    void stage(final int destination, final Chunk<? extends Values> source, int start, int length) {
        while (length > 0) {
            if (windowValues == null) {
                windowValues = chunkType.makeWritableChunk(windowCapacity);
                windowValues.setSize(0);
                windowDestinations = WritableIntChunk.makeWritableChunk(windowCapacity);
                windowDestinations.setSize(0);
            }
            final int size = windowValues.size();
            final int taken = Math.min(length, windowCapacity - size);
            windowValues.setSize(size + taken);
            windowValues.copyFromChunk(source, start, size, taken);
            windowDestinations.setSize(size + taken);
            windowDestinations.fillWithValue(size, taken, destination);
            start += taken;
            length -= taken;
            if (size + taken == windowCapacity) {
                flush();
            }
        }
    }

    /**
     * Merge the window into the entries, leaving the window empty.
     */
    void flush() {
        final int size = windowValues == null ? 0 : windowValues.size();
        if (size == 0) {
            return;
        }
        if (groupedCounts == null) {
            groupedCounts = WritableIntChunk.makeWritableChunk(windowCapacity);
            runStarts = WritableIntChunk.makeWritableChunk(windowCapacity);
            runLengths = WritableIntChunk.makeWritableChunk(windowCapacity);
        }

        // group the window's values by destination; a window with one destination is already grouped
        final WritableChunk<Values> grouped;
        if (windowDestinations.get(0) == windowDestinations.get(size - 1) && singleDestination(size)) {
            grouped = windowValues;
        } else {
            if (positions == null) {
                positions = WritableIntChunk.makeWritableChunk(windowCapacity);
                sortContext = IntIntTimsortKernel.createContext(windowCapacity);
                groupedValues = chunkType.makeWritableChunk(windowCapacity);
            }
            positions.setSize(size);
            ChunkUtils.fillInOrder(positions);
            sortContext.sort(positions, windowDestinations);
            groupedValues.setSize(size);
            permuteKernel.permuteInput(windowValues, positions, groupedValues);
            grouped = groupedValues;
        }

        // sort and count each destination's values in place
        runStarts.setSize(0);
        runLengths.setSize(0);
        int runStart = 0;
        for (int ii = 1; ii <= size; ++ii) {
            if (ii == size || windowDestinations.get(ii) != windowDestinations.get(runStart)) {
                runStarts.add(runStart);
                runLengths.add(ii - runStart);
                runStart = ii;
            }
        }
        groupedCounts.setSize(size);
        // Count NaN, but not NULL values
        compactKernel.compactAndCount(grouped, groupedCounts, runStarts, runLengths, false, true);

        // pack the counted runs together, with their destinations
        int packed = 0;
        for (int ri = 0; ri < runStarts.size(); ++ri) {
            final int start = runStarts.get(ri);
            final int length = runLengths.get(ri);
            final int destination = windowDestinations.get(start);
            if (packed != start) {
                grouped.copyFromChunk(grouped, start, packed, length);
                groupedCounts.copyFromChunk(groupedCounts, start, packed, length);
            }
            windowDestinations.fillWithValue(packed, length, destination);
            packed += length;
        }
        grouped.setSize(packed);
        groupedCounts.setSize(packed);
        windowDestinations.setSize(packed);

        if (values == null) {
            values = chunkType.makeWritableChunk(packed);
            counts = WritableIntChunk.makeWritableChunk(packed);
            destinations = WritableIntChunk.makeWritableChunk(packed);
            values.copyFromChunk(grouped, 0, 0, packed);
            counts.copyFromChunk(groupedCounts, 0, 0, packed);
            destinations.copyFromChunk(windowDestinations, 0, 0, packed);
        } else {
            final int mergedCapacity = values.size() + packed;
            final WritableChunk<Values> mergedValues = chunkType.makeWritableChunk(mergedCapacity);
            final WritableIntChunk<ChunkLengths> mergedCounts = WritableIntChunk.makeWritableChunk(mergedCapacity);
            final WritableIntChunk<RowKeys> mergedDestinations = WritableIntChunk.makeWritableChunk(mergedCapacity);
            merge(values.asObjectChunk(), counts, destinations, grouped.asObjectChunk(), groupedCounts,
                    windowDestinations, mergedValues.asWritableObjectChunk(), mergedCounts, mergedDestinations);
            SafeCloseable.closeAll(values, counts, destinations);
            values = mergedValues;
            counts = mergedCounts;
            destinations = mergedDestinations;
        }

        windowValues.setSize(0);
        windowDestinations.setSize(0);
        if (values.size() > windowCapacity && windowCapacity < ArrayUtil.MAX_ARRAY_SIZE) {
            windowCapacity = (int) Math.min(ArrayUtil.MAX_ARRAY_SIZE, 2L * windowCapacity);
            releaseWindow();
        }
    }

    /**
     * Merge two sequences of (destination, value, count) entries, each sorted by destination and then by value with no
     * two entries for one destination comparing equal, into one such sequence. Entries for one destination with values
     * that compare equal become one entry with the sum of their counts and the value from the first sequence.
     */
    private static void merge(final ObjectChunk<Object, Values> firstValues,
            final IntChunk<ChunkLengths> firstCounts, final IntChunk<RowKeys> firstDestinations,
            final ObjectChunk<Object, Values> secondValues, final IntChunk<ChunkLengths> secondCounts,
            final IntChunk<RowKeys> secondDestinations, final WritableObjectChunk<Object, Values> mergedValues,
            final WritableIntChunk<ChunkLengths> mergedCounts, final WritableIntChunk<RowKeys> mergedDestinations) {
        final int firstSize = firstValues.size();
        final int secondSize = secondValues.size();
        mergedValues.setSize(firstSize + secondSize);
        mergedCounts.setSize(firstSize + secondSize);
        mergedDestinations.setSize(firstSize + secondSize);
        int fi = 0;
        int si = 0;
        int mi = 0;
        while (fi < firstSize && si < secondSize) {
            final int firstDestination = firstDestinations.get(fi);
            final int secondDestination = secondDestinations.get(si);
            int comparison = Integer.compare(firstDestination, secondDestination);
            if (comparison == 0) {
                comparison = ObjectComparisons.compare(firstValues.get(fi), secondValues.get(si));
            }
            if (comparison < 0) {
                mergedValues.set(mi, firstValues.get(fi));
                mergedCounts.set(mi, firstCounts.get(fi));
                mergedDestinations.set(mi++, firstDestination);
                ++fi;
            } else if (comparison > 0) {
                mergedValues.set(mi, secondValues.get(si));
                mergedCounts.set(mi, secondCounts.get(si));
                mergedDestinations.set(mi++, secondDestination);
                ++si;
            } else {
                mergedValues.set(mi, firstValues.get(fi));
                mergedCounts.set(mi, firstCounts.get(fi) + secondCounts.get(si));
                mergedDestinations.set(mi++, firstDestination);
                ++fi;
                ++si;
            }
        }
        final int firstRemaining = firstSize - fi;
        mergedValues.copyFromChunk(firstValues, fi, mi, firstRemaining);
        mergedCounts.copyFromChunk(firstCounts, fi, mi, firstRemaining);
        mergedDestinations.copyFromChunk(firstDestinations, fi, mi, firstRemaining);
        mi += firstRemaining;
        final int secondRemaining = secondSize - si;
        mergedValues.copyFromChunk(secondValues, si, mi, secondRemaining);
        mergedCounts.copyFromChunk(secondCounts, si, mi, secondRemaining);
        mergedDestinations.copyFromChunk(secondDestinations, si, mi, secondRemaining);
        mi += secondRemaining;
        mergedValues.setSize(mi);
        mergedCounts.setSize(mi);
        mergedDestinations.setSize(mi);
    }

    private boolean singleDestination(final int size) {
        final int destination = windowDestinations.get(0);
        for (int ii = 1; ii < size - 1; ++ii) {
            if (windowDestinations.get(ii) != destination) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return the number of entries; valid after {@link #flush()}
     */
    int size() {
        return values == null ? 0 : values.size();
    }

    /**
     * @return the entries' values; valid after {@link #flush()}
     */
    WritableChunk<Values> values() {
        return values;
    }

    /**
     * @return the entries' counts, parallel to {@link #values()}; valid after {@link #flush()}
     */
    WritableIntChunk<ChunkLengths> counts() {
        return counts;
    }

    /**
     * @return the entries' destinations, parallel to {@link #values()}; valid after {@link #flush()}
     */
    WritableIntChunk<RowKeys> destinations() {
        return destinations;
    }

    private void releaseWindow() {
        SafeCloseable.closeAll(windowValues, windowDestinations, positions, sortContext, groupedValues,
                groupedCounts, runStarts, runLengths);
        windowValues = null;
        windowDestinations = null;
        positions = null;
        sortContext = null;
        groupedValues = null;
        groupedCounts = null;
        runStarts = null;
        runLengths = null;
    }

    /**
     * Free every buffer, dropping any staged values, and start the next cycle with the initial window.
     */
    void release() {
        releaseWindow();
        SafeCloseable.closeAll(values, counts, destinations);
        values = null;
        counts = null;
        destinations = null;
        windowCapacity = INITIAL_WINDOW;
    }
}
