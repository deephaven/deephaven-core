//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharNetChangeKernel and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmpercentile;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.ByteComparisons;

/**
 * {@link NetChangeKernel} for byte values, ordered by {@link ByteComparisons#compare}. Only values with the same
 * representation are netted; values that compare equal but differ in representation are both kept, so that the SSM
 * replaces its representative when the old one is removed.
 */
public class ByteNetChangeKernel implements NetChangeKernel {
    static final ByteNetChangeKernel INSTANCE = new ByteNetChangeKernel();

    private ByteNetChangeKernel() {}

    @Override
    public void net(final WritableChunk<Values> removes, final WritableIntChunk<ChunkLengths> removeCounts,
            final WritableChunk<Values> adds, final WritableIntChunk<ChunkLengths> addCounts) {
        net(removes.asWritableByteChunk(), removeCounts, adds.asWritableByteChunk(), addCounts);
    }

    private static void net(final WritableByteChunk<Values> removes, final WritableIntChunk<ChunkLengths> removeCounts,
            final WritableByteChunk<Values> adds, final WritableIntChunk<ChunkLengths> addCounts) {
        final int removeSize = removes.size();
        final int addSize = adds.size();
        // read and write positions in each input; an entry moves down only after an earlier entry has been dropped
        int ri = 0;
        int ai = 0;
        int rw = 0;
        int aw = 0;
        while (ri < removeSize && ai < addSize) {
            final byte removed = removes.get(ri);
            final byte added = adds.get(ai);
            final int comparison = ByteComparisons.compare(removed, added);
            if (comparison < 0) {
                keep(removes, removeCounts, ri++, rw++);
            } else if (comparison > 0) {
                keep(adds, addCounts, ai++, aw++);
            } else {
                int removeCount = removeCounts.get(ri++);
                int addCount = addCounts.get(ai++);
                // compactAndCount leaves one value for each run of values that compare equal, so only the first value
                // of each run is compared. Runs whose first values differ in representation are not netted even when
                // other values in the runs match; those values are removed from and reinserted into the SSMs, which
                // costs work but leaves the same counts.
                if (sameRepresentation(removed, added)) {
                    final int common = Math.min(removeCount, addCount);
                    removeCount -= common;
                    addCount -= common;
                }
                if (removeCount > 0) {
                    removes.set(rw, removed);
                    removeCounts.set(rw++, removeCount);
                }
                if (addCount > 0) {
                    adds.set(aw, added);
                    addCounts.set(aw++, addCount);
                }
            }
        }
        final int removesKept = keepTail(removes, removeCounts, ri, rw, removeSize);
        final int addsKept = keepTail(adds, addCounts, ai, aw, addSize);
        removes.setSize(removesKept);
        removeCounts.setSize(removesKept);
        adds.setSize(addsKept);
        addCounts.setSize(addsKept);
    }

    private static void keep(final WritableByteChunk<Values> values, final WritableIntChunk<ChunkLengths> counts,
            final int read, final int write) {
        if (read != write) {
            values.set(write, values.get(read));
            counts.set(write, counts.get(read));
        }
    }

    /**
     * Keep the entries from {@code read} to {@code size}, moving them down to {@code write}.
     *
     * @return the number of entries kept
     */
    private static int keepTail(final WritableByteChunk<Values> values, final WritableIntChunk<ChunkLengths> counts,
            int read, int write, final int size) {
        if (read == write) {
            return size;
        }
        while (read < size) {
            values.set(write, values.get(read));
            counts.set(write++, counts.get(read++));
        }
        return write;
    }

    private static boolean sameRepresentation(final byte lhs, final byte rhs) {
        // region sameRepresentation
        return lhs == rhs;
        // endregion sameRepresentation
    }
}
