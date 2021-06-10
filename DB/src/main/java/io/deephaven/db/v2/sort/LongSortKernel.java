package io.deephaven.db.v2.sort;

import io.deephaven.db.tables.SortingOrder;
import io.deephaven.db.v2.sort.radix.BooleanLongRadixSortKernel;
import io.deephaven.db.v2.sort.timsort.*;
import io.deephaven.db.v2.sources.chunk.*;

import static io.deephaven.db.v2.sources.chunk.Attributes.*;

/**
 * The SortKernel sorts a chunk of primitive value together with a parallel LongChunk of KeyIndices.
 */
public interface LongSortKernel<ATTR extends Any, KEY_INDICES extends Keys> extends Context {
    /**
     * Creates a SortKernel for the given chunkType.
     *
     * @param chunkType the type of chunk we are sorting
     * @param order whether we should sort in an ascending or descending direction
     * @param size how many values we will be sorting
     * @param preserveValues if the output chunk of our sort should contain the values in sorted order; otherwise the
     *                       kernel need only permute the input indices into sorted order
     * @return a SortKernel suitable for the given type, order, and size
     */
    static <ATTR extends Any, KEY_INDICES extends Keys> LongSortKernel<ATTR, KEY_INDICES> makeContext(ChunkType chunkType, SortingOrder order, int size, boolean preserveValues) {
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
     * Sort the values in valuesToSort permuting the indexKeys chunk in the same way.
     */
    void sort(WritableLongChunk<KEY_INDICES> indexKeys, WritableChunk<ATTR> valuesToSort);

    /**
     * Sort the values in valuesToSort permuting the indexKeys chunk in the same way.
     *
     * The offsetsIn chunk is contains the offset of runs to sort in indexKeys; and the lengthsIn contains the length
     * of the runs.  This allows the kernel to be used for a secondary column sort, chaining it together with smaller
     * runs sorted on each pass.
     */
    void sort(WritableLongChunk<KEY_INDICES> indexKeys, WritableChunk<ATTR> valuesToSort, IntChunk<? extends ChunkPositions> offsetsIn, IntChunk<? extends ChunkLengths> lengthsIn);
}
