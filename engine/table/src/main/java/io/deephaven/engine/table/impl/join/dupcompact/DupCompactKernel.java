//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.join.dupcompact;

import io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper;
import io.deephaven.util.compare.ObjectComparisons;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.jetbrains.annotations.NotNull;

public interface DupCompactKernel {

    /**
     * Make a DupCompactKernel that orders values as Deephaven sorts them.
     *
     * @param chunkType the chunk type of the values
     * @param dataType the data type of the values; for Object values whose natural ordering is consistent with equals
     *        (see {@link BinarySearchKernelHelper#compareConsistentWithEquality(Class)}) the kernel tests equality with
     *        {@code equals}, otherwise with {@link ObjectComparisons#compareEquals(Object, Object)}
     * @param reverse true for descending values
     * @return the DupCompactKernel
     */
    static DupCompactKernel makeDupCompactDeephavenOrdering(@NotNull final ChunkType chunkType,
            @NotNull final Class<?> dataType, final boolean reverse) {
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
                    return BinarySearchKernelHelper.compareConsistentWithEquality(dataType)
                            ? EqualsConsistentObjectReverseDupCompactKernel.INSTANCE
                            : ObjectReverseDupCompactKernel.INSTANCE;
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
                    return BinarySearchKernelHelper.compareConsistentWithEquality(dataType)
                            ? EqualsConsistentObjectDupCompactKernel.INSTANCE
                            : ObjectDupCompactKernel.INSTANCE;
                case Boolean:
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    /**
     * Make a DupCompactKernel that orders values by their natural ordering.
     *
     * @param chunkType the chunk type of the values
     * @param dataType the data type of the values; for Object values whose natural ordering is consistent with equals
     *        (see {@link BinarySearchKernelHelper#compareConsistentWithEquality(Class)}) the kernel tests equality with
     *        {@code equals}, otherwise with {@link ObjectComparisons#compareEquals(Object, Object)}
     * @param reverse true for descending values
     * @return the DupCompactKernel
     */
    static DupCompactKernel makeDupCompactNaturalOrdering(@NotNull final ChunkType chunkType,
            @NotNull final Class<?> dataType, final boolean reverse) {
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
                    return BinarySearchKernelHelper.compareConsistentWithEquality(dataType)
                            ? EqualsConsistentObjectReverseDupCompactKernel.INSTANCE
                            : ObjectReverseDupCompactKernel.INSTANCE;
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
                    return BinarySearchKernelHelper.compareConsistentWithEquality(dataType)
                            ? EqualsConsistentObjectDupCompactKernel.INSTANCE
                            : ObjectDupCompactKernel.INSTANCE;
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
