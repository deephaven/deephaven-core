//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.join.dupcompact;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.jetbrains.annotations.NotNull;

public interface DupCompactKernel {

    static DupCompactKernel makeDupCompactDeephavenOrdering(@NotNull final ChunkType chunkType, final boolean reverse) {
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

    static DupCompactKernel makeDupCompactNaturalOrdering(@NotNull final ChunkType chunkType, final boolean reverse) {
        if (reverse) {
            switch (chunkType) {
                case Char:
                    return CharReverseDupCompactKernel.INSTANCE;
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
                    return CharDupCompactKernel.INSTANCE;
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
     * Remove all adjacent values from {@code chunkToCompact}, except the <em>last</em> value in any adjacent run. The
     * {@code rowKeys} are parallel to the {@code chunkToCompact}. When a value is removed from {@code chunkToCompact},
     * it is also removed from {@code rowKeys}.
     * <p>
     * Additionally, verifies that the elements in {@code chunkToCompact} are properly ordered. Upon encountering an
     * out-of-order element, the operation stops compacting and returns the position of the out-of-order element.
     *
     * @param chunkToCompact The values to remove duplicates from
     * @param rowKeys The row keys parallel to {@code chunkToCompact}
     *
     * @return The first position of an out-of-order element, or -1 if all elements are in order
     */
    int compactDuplicates(
            @NotNull WritableChunk<? extends Any> chunkToCompact,
            @NotNull WritableLongChunk<RowKeys> rowKeys);

    /**
     * Remove all adjacent values from {@code chunkToCompact}, except the <em>first</em> value in any adjacent run. The
     * {@code chunkPositions} are parallel to the {@code chunkToCompact}. When a value is removed from
     * {@code chunkToCompact}, it is also removed from {@code chunkPositions}.
     * <p>
     * Additionally, verifies that the elements in {@code chunkToCompact} are properly ordered. Upon encountering an
     * out-of-order element, the operation stops compacting and returns the position of the out-of-order element.
     *
     * @param chunkToCompact The values to remove duplicates from
     * @param chunkPositions The chunk positions parallel to {@code chunkToCompact}
     *
     * @return The first position of an out-of-order element, or -1 if all elements are in order
     */
    int compactDuplicatesPreferFirst(
            @NotNull WritableChunk<? extends Any> chunkToCompact,
            @NotNull WritableIntChunk<ChunkPositions> chunkPositions);
}
