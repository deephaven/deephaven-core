package io.deephaven.engine.table.impl.sort;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Indices;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.megamerge.*;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.chunk.*;

/**
 * The LongMegaMergeKernel merges sorted chunks into a column source, with a parallel long array of row keys.
 *
 * The intention is that you will use a Timsort kernel to sort maximally sized chunks, then merge the result into a
 * ColumnSource using the MegaMergeKernel only if necessary.
 */
public interface LongMegaMergeKernel<ATTR extends Any, KEY_INDICES extends Indices> extends Context {
    /**
     * Creates a SortKernel for the given chunkType.
     *
     * @param chunkType the type of chunk we are sorting
     * @param order whether we should sort in an ascending or descending direction
     * @return a SortKernel suitable for the given type, order, and size
     */
    static <ATTR extends Any, KEY_INDICES extends Indices> LongMegaMergeKernel<ATTR, KEY_INDICES> makeContext(ChunkType chunkType, SortingOrder order) {
        switch (chunkType) {
            case Char:
                if (order == SortingOrder.Ascending) {
                    return CharLongMegaMergeKernel.createContext();
                } else {
                    return CharLongMegaMergeDescendingKernel.createContext();
                }
            case Byte:
                if (order == SortingOrder.Ascending) {
                    return ByteLongMegaMergeKernel.createContext();
                } else {
                    return ByteLongMegaMergeDescendingKernel.createContext();
                }
            case Short:
                if (order == SortingOrder.Ascending) {
                    return ShortLongMegaMergeKernel.createContext();
                } else {
                    return ShortLongMegaMergeDescendingKernel.createContext();
                }
            case Int:
                if (order == SortingOrder.Ascending) {
                    return IntLongMegaMergeKernel.createContext();
                } else {
                    return IntLongMegaMergeDescendingKernel.createContext();
                }
            case Long:
                if (order == SortingOrder.Ascending) {
                    return LongLongMegaMergeKernel.createContext();
                } else {
                    return LongLongMegaMergeDescendingKernel.createContext();
                }
            case Float:
                if (order == SortingOrder.Ascending) {
                    return FloatLongMegaMergeKernel.createContext();
                } else {
                    return FloatLongMegaMergeDescendingKernel.createContext();
                }
            case Double:
                if (order == SortingOrder.Ascending) {
                    return DoubleLongMegaMergeKernel.createContext();
                } else {
                    return DoubleLongMegaMergeDescendingKernel.createContext();
                }
            case Boolean:
                throw new UnsupportedOperationException();
            case Object:
                if (order == SortingOrder.Ascending) {
                    return ObjectLongMegaMergeKernel.createContext();
                } else {
                    return ObjectLongMegaMergeDescendingKernel.createContext();
                }
        }
        throw new IllegalStateException("Did not match chunk type: " + chunkType);
    }

    void merge(LongArraySource indexDestinationSource, ArrayBackedColumnSource<?> valuesDestinationSource,
               long destinationOffset, long destinationSize,
               LongChunk<KEY_INDICES> indexKeys, Chunk<ATTR> valuesToMerge);
}
