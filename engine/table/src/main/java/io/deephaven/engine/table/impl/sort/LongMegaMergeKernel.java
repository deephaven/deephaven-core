//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Indices;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.megamerge.*;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.chunk.*;
import io.deephaven.base.verify.Assert;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;

/**
 * The LongMegaMergeKernel merges sorted chunks into a column source, with a parallel long array of row keys.
 *
 * The intention is that you will use a Timsort kernel to sort maximally sized chunks, then merge the result into a
 * ColumnSource using the MegaMergeKernel only if necessary.
 */
public interface LongMegaMergeKernel<ATTR extends Any, KEY_INDICES extends Indices> {
    /**
     * Creates a SortKernel for the given chunkType.
     *
     * @param chunkType the type of chunk we are sorting
     * @param order whether we should sort in an ascending or descending direction
     * @return a SortKernel suitable for the given type, order, and size
     */
    static <ATTR extends Any, KEY_INDICES extends Indices> LongMegaMergeKernel<ATTR, KEY_INDICES> makeContext(
            ChunkType chunkType, SortingOrder order) {
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

    /**
     * Creates a merge kernel for the given chunkType that orders values with comparator, or in their natural order when
     * comparator is null.
     *
     * @param chunkType the type of chunk we are sorting; must be Object when comparator is not null
     * @param order whether we should sort in an ascending or descending direction
     * @param comparator the comparator for the values, or null for their natural order
     * @return a merge kernel suitable for the given type, order, and comparator
     */
    static <ATTR extends Any, KEY_INDICES extends Indices> LongMegaMergeKernel<ATTR, KEY_INDICES> makeContext(
            ChunkType chunkType, SortingOrder order, @Nullable Comparator<?> comparator) {
        if (comparator == null) {
            return makeContext(chunkType, order);
        }
        Assert.eq(chunkType, "chunkType", ChunkType.Object, "ChunkType.Object");
        return ComparatorLongMegaMergeKernel.createContext(
                order == SortingOrder.Ascending ? comparator : comparator.reversed());
    }

    void merge(LongArraySource indexDestinationSource, WritableColumnSource<?> valuesDestinationSource,
            long destinationOffset, long destinationSize,
            LongChunk<KEY_INDICES> indexKeys, Chunk<ATTR> valuesToMerge);
}
