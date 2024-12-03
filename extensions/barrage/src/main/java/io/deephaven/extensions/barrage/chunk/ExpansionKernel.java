//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface ExpansionKernel<T> {

    /**
     * This expands the source from a {@code V} per element to a flat {@code T} per element. The kernel records the
     * number of consecutive elements that belong to a row in {@code offsetDest}. The returned chunk is owned by the
     * caller.
     * <p>
     * If a non-zero {@code fixedSizeLength} is provided, then each row will be truncated or null-appended as
     * appropriate to match the fixed size.
     *
     * @param source the source chunk of V to expand
     * @param fixedSizeLength the length of each array, which is fixed for all rows
     * @param offsetDest the destination IntChunk for which {@code dest.get(i + 1) - dest.get(i)} is equivalent to
     *        {@code source.get(i).length}
     * @return an unrolled/flattened chunk of T
     */
    <A extends Any> WritableChunk<A> expand(
            @NotNull ObjectChunk<T, A> source,
            int fixedSizeLength,
            @Nullable WritableIntChunk<ChunkPositions> offsetDest);

    /**
     * This contracts the source from a pair of {@code LongChunk} and {@code Chunk<T>} and produces a {@code Chunk<V>}.
     * The returned chunk is owned by the caller.
     * <p>
     * The method of determining the length of each row is determined by whether {@code offsets} and {@code lengths} are
     * {@code null} or not. If offsets is {@code null}, then the length of each row is assumed to be
     * {@code sizePerElement}. If {@code lengths} is {@code null}, then the length of each row is determined by adjacent
     * elements in {@code offsets}. If both are non-{@code null}, then the length of each row is determined by
     * {@code lengths}.
     *
     * @param source the source chunk of T to contract
     * @param sizePerElement the length of each array, which is fixed for all rows
     * @param offsets the source IntChunk to determine the start location of each row
     * @param lengths the source IntChunk to determine the length of each row
     * @param outChunk the returned chunk from an earlier record batch
     * @param outOffset the offset to start writing into {@code outChunk}
     * @param totalRows the total known rows for this column; if known (else 0)
     * @return a result chunk of {@code V}
     */
    <A extends Any> WritableObjectChunk<T, A> contract(
            @NotNull Chunk<A> source,
            int sizePerElement,
            @Nullable IntChunk<ChunkPositions> offsets,
            @Nullable IntChunk<ChunkLengths> lengths,
            @Nullable WritableChunk<A> outChunk,
            int outOffset,
            int totalRows);

    /**
     * This computes the length of a given index depending on whether this is an Arrow FixedSizeList, List, or ListView.
     * <p>
     * If {@code offsets} is {@code null}, then the length of each row is assumed to be {@code sizePerOffset}. If
     * {@code lengths} is {@code null}, then the length of each row is determined by adjacent elements in
     * {@code offsets}. If both are non-{@code null}, then the length of each row is determined by {@code lengths}.
     *
     * @param ii the index to compute the size for
     * @param sizePerOffset the size of each offset when fixed
     * @param offsets the source IntChunk to determine the start location of each row
     * @param lengths the source IntChunk to determine the length of each row
     * @return the length of the given index
     */
    @FinalDefault
    default int computeSize(
            final int ii,
            final int sizePerOffset,
            @Nullable final IntChunk<ChunkPositions> offsets,
            @Nullable final IntChunk<ChunkLengths> lengths) {
        if (offsets == null) {
            return sizePerOffset;
        }
        if (lengths == null) {
            return offsets.get(ii + 1) - offsets.get(ii);
        }
        return lengths.get(ii);
    }
}
