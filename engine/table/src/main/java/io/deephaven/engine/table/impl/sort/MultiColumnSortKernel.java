//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.table.Context;

/**
 * The MultiColumnSortKernel sorts a set of parallel value chunks — comparing each column in turn, as a lexicographic
 * sort key — together with a parallel LongChunk of row keys, in a single pass rather than sorting one column at a time
 * with run finding in between.
 */
public interface MultiColumnSortKernel<PERMUTE_VALUES_ATTR extends Any> extends Context {
    /**
     * Sort the values in the valuesToSort chunks, comparing each column in turn, permuting the valuesToPermute chunk in
     * the same way.
     *
     * @param valuesToPermute the row keys (or other values) permuted alongside the sort key columns
     * @param valuesToSort one chunk per sort key column, each of the specialized writable chunk type this kernel was
     *        created for; all chunks must have the same size as valuesToPermute
     */
    void sort(WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute, WritableChunk<? extends Any>[] valuesToSort);

    /**
     * Sort a range of the caller's positions chunk such that the values it points to are ordered by this kernel's sort
     * key; the value chunks are not modified. Used by the parallel sort driver, which sorts disjoint ranges of one
     * positions chunk from separate tasks, each with its own context.
     *
     * @param positions positions into the valuesToSort chunks; only [offset, offset + length) is sorted
     * @param valuesToSort one chunk per sort key column
     * @param offset the first position index to sort
     * @param length the number of position entries to sort; must not exceed the size this context was created with
     */
    default void sortPositions(WritableIntChunk<ChunkPositions> positions,
            WritableChunk<? extends Any>[] valuesToSort, int offset, int length) {
        throw new UnsupportedOperationException("parallel sorting is not supported by " + getClass());
    }

    /**
     * Merge two adjacent sorted runs of the caller's positions chunk, [start1, start1 + length1) and [start1 + length1,
     * start1 + length1 + length2), into a single sorted run.
     *
     * @param positions positions into the valuesToSort chunks
     * @param valuesToSort one chunk per sort key column
     * @param start1 the first position index of the first run
     * @param length1 the length of the first run
     * @param length2 the length of the second run; length1 + length2 must not exceed the size this context was created
     *        with
     */
    default void mergePositions(WritableIntChunk<ChunkPositions> positions,
            WritableChunk<? extends Any>[] valuesToSort, int start1, int length1, int length2) {
        throw new UnsupportedOperationException("parallel sorting is not supported by " + getClass());
    }
}
