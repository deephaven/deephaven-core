/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit LongSortKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sort;

import io.deephaven.db.tables.SortingOrder;
import io.deephaven.db.v2.sort.radix.BooleanIntRadixSortKernel;
import io.deephaven.db.v2.sort.timsort.*;
import io.deephaven.db.v2.sources.chunk.*;

import static io.deephaven.db.v2.sources.chunk.Attributes.*;

/**
 * The SortKernel sorts a chunk of primitive value together with a parallel IntChunk of KeyIndices.
 */
public interface IntSortKernel<ATTR extends Any, KEY_INDICES extends Keys> extends Context {
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
    static <ATTR extends Any, KEY_INDICES extends Keys> IntSortKernel<ATTR, KEY_INDICES> makeContext(ChunkType chunkType, SortingOrder order, int size, boolean preserveValues) {
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
     * Sort the values in valuesToSort permuting the indexKeys chunk in the same way.
     */
    void sort(WritableIntChunk<KEY_INDICES> indexKeys, WritableChunk<ATTR> valuesToSort);

    /**
     * Sort the values in valuesToSort permuting the indexKeys chunk in the same way.
     *
     * The offsetsIn chunk is contains the offset of runs to sort in indexKeys; and the lengthsIn contains the length
     * of the runs.  This allows the kernel to be used for a secondary column sort, chaining it together with smaller
     * runs sorted on each pass.
     */
    void sort(WritableIntChunk<KEY_INDICES> indexKeys, WritableChunk<ATTR> valuesToSort, IntChunk<? extends ChunkPositions> offsetsIn, IntChunk<? extends ChunkLengths> lengthsIn);
}
