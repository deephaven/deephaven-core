//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.radix.BooleanLongRadixSortKernel;
import io.deephaven.engine.table.impl.sort.timsort.*;
import io.deephaven.chunk.*;

/**
 * The SortKernel sorts a chunk of primitive value together with a parallel LongChunk of RowKeys.
 */
public interface LongSortKernel<SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> extends Context {
    /**
     * Creates a SortKernel for the given chunkType.
     *
     * @param chunkType the type of chunk we are sorting
     * @param order whether we should sort in an ascending or descending direction
     * @param size how many values we will be sorting
     * @param preserveValues if the output chunk of our sort should contain the values in sorted order; otherwise the
     *        kernel need only permute the input indices into sorted order
     * @return a SortKernel suitable for the given type, order, and size
     */
    static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> LongSortKernel<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> makeContext(
            ChunkType chunkType, SortingOrder order, int size, boolean preserveValues) {
        switch (chunkType) {
            case Char:
                if (order == SortingOrder.Ascending) {
                    return NullAwareCharLongTimsortKernel.createContext(size);
                } else {
                    return NullAwareCharLongTimsortDescendingKernel.createContext(size);
                }
            case Byte:
                if (order == SortingOrder.Ascending) {
                    return ByteLongTimsortKernel.createContext(size);
                } else {
                    return ByteLongTimsortDescendingKernel.createContext(size);
                }
            case Short:
                if (order == SortingOrder.Ascending) {
                    return ShortLongTimsortKernel.createContext(size);
                } else {
                    return ShortLongTimsortDescendingKernel.createContext(size);
                }
            case Int:
                if (order == SortingOrder.Ascending) {
                    return IntLongTimsortKernel.createContext(size);
                } else {
                    return IntLongTimsortDescendingKernel.createContext(size);
                }
                // region lngcase
            case Long:
                if (order == SortingOrder.Ascending) {
                    return LongLongTimsortKernel.createContext(size);
                } else {
                    return LongLongTimsortDescendingKernel.createContext(size);
                }
                // endregion lngcase
            case Float:
                if (order == SortingOrder.Ascending) {
                    return FloatLongTimsortKernel.createContext(size);
                } else {
                    return FloatLongTimsortDescendingKernel.createContext(size);
                }
            case Double:
                if (order == SortingOrder.Ascending) {
                    return DoubleLongTimsortKernel.createContext(size);
                } else {
                    return DoubleLongTimsortDescendingKernel.createContext(size);
                }
            case Boolean:
                return BooleanLongRadixSortKernel.createContext(size, order, preserveValues);
            case Object:

                if (order == SortingOrder.Ascending) {
                    return ObjectLongTimsortKernel.createContext(size);
                } else {
                    return ObjectLongTimsortDescendingKernel.createContext(size);
                }
        }
        throw new IllegalStateException("Did not match chunk type: " + chunkType);
    }

    /**
     * Sort the values in valuesToSort permuting the valuesToPermute chunk in the same way.
     */
    void sort(WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute, WritableChunk<SORT_VALUES_ATTR> valuesToSort);

    /**
     * Sort the values in valuesToSort permuting the valuesToPermute chunk in the same way.
     * <p>
     * The offsetsIn chunk is contains the offsets of runs to sort in valuesToPermute; and the lengthsIn contains the
     * lengths of the runs. This allows the kernel to be used for a secondary column sort, chaining it together with
     * smaller runs sorted on each pass.
     */
    void sort(WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute, WritableChunk<SORT_VALUES_ATTR> valuesToSort,
            IntChunk<? extends ChunkPositions> offsetsIn, IntChunk<? extends ChunkLengths> lengthsIn);

    /**
     * Sort a range of the values in valuesToSort, permuting the same range of the valuesToPermute chunk in the same
     * way. Used by the parallel sort driver, which sorts disjoint ranges of the chunks from separate tasks, each with
     * its own kernel context.
     *
     * @param valuesToPermute the values to permute alongside the sorted values
     * @param valuesToSort the values to sort; only [offset, offset + length) is sorted
     * @param offset the first index to sort
     * @param length the number of entries to sort; must not exceed the size this kernel context was created with
     */
    default void sort(WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableChunk<SORT_VALUES_ATTR> valuesToSort, int offset, int length) {
        throw new UnsupportedOperationException("parallel sorting is not supported by " + getClass());
    }

    /**
     * Merge two adjacent sorted runs, [start1, start1 + length1) and [start1 + length1, start1 + length1 + length2), of
     * the values in valuesToSort into a single sorted run, permuting the valuesToPermute chunk in the same way.
     *
     * @param valuesToPermute the values permuted alongside the sorted values
     * @param valuesToSort the values to merge
     * @param start1 the first index of the first run
     * @param length1 the length of the first run
     * @param length2 the length of the second run; length1 + length2 must not exceed the size this kernel context was
     *        created with
     */
    default void merge(WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            WritableChunk<SORT_VALUES_ATTR> valuesToSort, int start1, int length1, int length2) {
        throw new UnsupportedOperationException("parallel sorting is not supported by " + getClass());
    }
}
