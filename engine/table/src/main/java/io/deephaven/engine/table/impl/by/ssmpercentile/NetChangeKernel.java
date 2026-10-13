//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.ssmpercentile;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;

/**
 * Nets the values removed from a bucket against the values added to it, so that a value both removed and added is
 * neither removed from nor inserted into the bucket's sets.
 */
public interface NetChangeKernel {
    /**
     * Subtract the common occurrences of each value from both inputs, then drop the values whose count reaches zero.
     * Both inputs must be sorted and free of duplicates, as {@code CompactKernel.compactAndCount} leaves them, with
     * parallel counts. On return both inputs are still sorted, and every count is positive.
     *
     * @param removes the values removed, input and output
     * @param removeCounts the number of times each value in {@code removes} was removed, input and output
     * @param adds the values added, input and output
     * @param addCounts the number of times each value in {@code adds} was added, input and output
     */
    void net(WritableChunk<Values> removes, WritableIntChunk<ChunkLengths> removeCounts,
            WritableChunk<Values> adds, WritableIntChunk<ChunkLengths> addCounts);

    static NetChangeKernel make(final ChunkType chunkType) {
        switch (chunkType) {
            case Char:
                return CharNetChangeKernel.INSTANCE;
            case Byte:
                return ByteNetChangeKernel.INSTANCE;
            case Short:
                return ShortNetChangeKernel.INSTANCE;
            case Int:
                return IntNetChangeKernel.INSTANCE;
            case Long:
                return LongNetChangeKernel.INSTANCE;
            case Float:
                return FloatNetChangeKernel.INSTANCE;
            case Double:
                return DoubleNetChangeKernel.INSTANCE;
            case Object:
                return ObjectNetChangeKernel.INSTANCE;
            default:
                throw new UnsupportedOperationException("No net change kernel for " + chunkType);
        }
    }
}
