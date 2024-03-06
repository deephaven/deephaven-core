//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit LongSortKernel and run "./gradlew replicateSortKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sort;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.radix.BooleanIntRadixSortKernel;
import io.deephaven.engine.table.impl.sort.timsort.*;
import io.deephaven.chunk.*;

/**
 * The SortKernel sorts a chunk of primitive value together with a parallel IntChunk of RowKeys.
 */
public interface IntSortKernel<SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> extends Context {
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
    static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> IntSortKernel<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> makeContext(
            ChunkType chunkType, SortingOrder order, int size, boolean preserveValues) {
        switch (chunkType) {
            case Char:
                if (order == SortingOrder.Ascending) {
                    return NullAwareCharIntTimsortKernel.createContext(size);
                } else {
                    return NullAwareCharIntTimsortDescendingKernel.createContext(size);
                }
            case Byte:
                if (order == SortingOrder.Ascending) {
                    return ByteIntTimsortKernel.createContext(size);
                } else {
                    return ByteIntTimsortDescendingKernel.createContext(size);
                }
            case Short:
                if (order == SortingOrder.Ascending) {
                    return ShortIntTimsortKernel.createContext(size);
                } else {
                    return ShortIntTimsortDescendingKernel.createContext(size);
                }
            case Int:
                if (order == SortingOrder.Ascending) {
                    return IntIntTimsortKernel.createContext(size);
                } else {
                    return IntIntTimsortDescendingKernel.createContext(size);
                }
                // region lngcase
                case Long:
                if (order == SortingOrder.Ascending) {
                    return LongIntTimsortKernel.createContext(size);
                } else {
                    return LongIntTimsortDescendingKernel.createContext(size);
                }
                // endregion lngcase
            case Float:
                if (order == SortingOrder.Ascending) {
                    return FloatIntTimsortKernel.createContext(size);
                } else {
                    return FloatIntTimsortDescendingKernel.createContext(size);
                }
            case Double:
                if (order == SortingOrder.Ascending) {
                    return DoubleIntTimsortKernel.createContext(size);
                } else {
                    return DoubleIntTimsortDescendingKernel.createContext(size);
                }
            case Boolean:
                return BooleanIntRadixSortKernel.createContext(size, order, preserveValues);
            case Object:

                if (order == SortingOrder.Ascending) {
                    return ObjectIntTimsortKernel.createContext(size);
                } else {
                    return ObjectIntTimsortDescendingKernel.createContext(size);
                }
        }
        throw new IllegalStateException("Did not match chunk type: " + chunkType);
    }

    /**
     * Sort the values in valuesToSort permuting the valuesToPermute chunk in the same way.
     */
    void sort(WritableIntChunk<PERMUTE_VALUES_ATTR> valuesToPermute, WritableChunk<SORT_VALUES_ATTR> valuesToSort);

    /**
     * Sort the values in valuesToSort permuting the valuesToPermute chunk in the same way.
     * <p>
     * The offsetsIn chunk is contains the offsets of runs to sort in valuesToPermute; and the lengthsIn contains the
     * lengths of the runs. This allows the kernel to be used for a secondary column sort, chaining it together with
     * smaller runs sorted on each pass.
     */
    void sort(WritableIntChunk<PERMUTE_VALUES_ATTR> valuesToPermute, WritableChunk<SORT_VALUES_ATTR> valuesToSort,
            IntChunk<? extends ChunkPositions> offsetsIn, IntChunk<? extends ChunkLengths> lengthsIn);
}
