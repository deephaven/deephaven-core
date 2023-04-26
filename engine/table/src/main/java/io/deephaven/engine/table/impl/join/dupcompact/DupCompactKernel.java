/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.join.dupcompact;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.jetbrains.annotations.NotNull;

public interface DupCompactKernel {
    static DupCompactKernel makeDupCompact(ChunkType chunkType, boolean reverse) {
        if (reverse) {
            switch (chunkType) {
                case Char:
                    return NullAwareCharReverseDupCompactKernel.INSTANCE;
                case Byte:
                    return ByteReverseDupCompactKernel.INSTANCE;
                case Short:
                    return ShortReverseDupCompactKernel.INSTANCE;
                case Int:
                    return IntReverseDupCompactKernel.INSTANCE;
                case Long:
                    return LongReverseDupCompactKernel.INSTANCE;
                case Float:
                    return FloatReverseDupCompactKernel.INSTANCE;
                case Double:
                    return DoubleReverseDupCompactKernel.INSTANCE;
                case Object:
                    return ObjectReverseDupCompactKernel.INSTANCE;
                case Boolean:
                default:
                    throw new UnsupportedOperationException();
            }
        } else {
            switch (chunkType) {
                case Char:
                    return NullAwareCharDupCompactKernel.INSTANCE;
                case Byte:
                    return ByteDupCompactKernel.INSTANCE;
                case Short:
                    return ShortDupCompactKernel.INSTANCE;
                case Int:
                    return IntDupCompactKernel.INSTANCE;
                case Long:
                    return LongDupCompactKernel.INSTANCE;
                case Float:
                    return FloatDupCompactKernel.INSTANCE;
                case Double:
                    return DoubleDupCompactKernel.INSTANCE;
                case Object:
                    return ObjectDupCompactKernel.INSTANCE;
                case Boolean:
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    /**
     * Remove all adjacent values from chunkToCompact, except the last value in any adjacent run. The keyIndices are
     * parallel to the chunkToCompact; and when a value is removed from chunkToCompact it is also removed from
     * keyIndices
     * <p>
     * Additionally, verify that the elements are properly ordered; returning the first position of an out-of-order
     * element.
     *
     * @param chunkToCompact The values to remove duplicates from
     * @param keyIndices The key indices parallel to chunkToCompact
     *
     * @return The first position of an out-of-order element, or -1 if all elements are in order
     */
    int compactDuplicates(
            @NotNull WritableChunk<? extends Any> chunkToCompact,
            @NotNull WritableLongChunk<RowKeys> keyIndices);
}
