//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
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

/**
 * The {@code ExpansionKernel} interface provides methods for transforming chunks containing complex or nested data
 * structures into flattened representations, and vice versa. This enables efficient handling of columnar data in
 * scenarios involving arrays, or {@link io.deephaven.vector.Vector vectors}, particularly within the Deephaven Barrage
 * extensions for Flight/Barrage streams.
 * <p>
 * An {@code ExpansionKernel} supports two primary operations:
 * <ul>
 * <li><b>Expansion:</b> Converts nested or multi-element data into a flattened form, along with metadata (e.g., row
 * offsets) describing the original structure.</li>
 * <li><b>Contraction:</b> Reconstructs the original nested data structure from a flattened representation and
 * associated metadata.</li>
 * </ul>
 *
 * @param <T> The type of data being processed by this kernel.
 */
public interface ExpansionKernel<T> {

    /**
     * Expands a chunk of nested or multi-element data ({@code T[]} or {@code Vector<T>}) into a flattened chunk of
     * elements ({@code T}), along with metadata describing the structure of the original data.
     * <p>
     * The expansion involves unrolling arrays, or {@link io.deephaven.vector.Vector vectors}, or other multi-element
     * types into a single contiguous chunk. The number of elements belonging to each original row is recorded in
     * {@code offsetDest}, which allows reconstructing the original structure when needed.
     * <p>
     * If a non-zero {@code fixedSizeLength} is provided, each row will be truncated or padded with nulls to match the
     * fixed size. A negative {@code fixedSizeLength} will pick elements from the end of the array/vector.
     *
     * @param source The source chunk containing nested or multi-element data to expand.
     * @param fixedSizeLength The fixed size for each row, or 0 for variable-length rows.
     * @param offsetDest The destination {@link WritableIntChunk} to store row offsets, or {@code null} if not needed.
     * @param <A> The attribute type of the source chunk.
     * @return A flattened {@link WritableChunk} containing the expanded elements.
     */
    <A extends Any> WritableChunk<A> expand(
            @NotNull ObjectChunk<T, A> source,
            int fixedSizeLength,
            @Nullable WritableIntChunk<ChunkPositions> offsetDest);

    /**
     * Contracts a flattened chunk of elements ({@code T}) back into a chunk of nested or multi-element data
     * ({@code T[]} or {@code Vector<T>}), using provided metadata (e.g., row offsets or lengths) to reconstruct the
     * original structure.
     * <p>
     * The contraction process supports multiple configurations:
     * <ul>
     * <li>If {@code offsets} is {@code null}, each row is assumed to have a fixed size defined by
     * {@code sizePerElement}.</li>
     * <li>If {@code lengths} is {@code null}, each row's size is determined by differences between adjacent elements in
     * {@code offsets}. An element's length is determined by subtracting its offset from the next offset, therefore
     * offsets must contain one more element than the number of elements.</li>
     * <li>If both {@code offsets} and {@code lengths} are provided, {@code lengths} determines the row sizes.</li>
     * </ul>
     *
     * @param source The source chunk containing flattened data to contract.
     * @param sizePerElement The fixed size for each row, or 0 for variable-length rows.
     * @param offsets An {@link IntChunk} describing row start positions, or {@code null}.
     * @param lengths An {@link IntChunk} describing row lengths, or {@code null}.
     * @param outChunk A reusable {@link WritableChunk} to store the contracted result, or {@code null}.
     * @param outOffset The starting position for writing into {@code outChunk}.
     * @param totalRows The total number of rows, or 0 if unknown.
     * @param <A> The attribute type of the source chunk.
     * @return A {@link WritableObjectChunk} containing the reconstructed nested or multi-element data.
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
     * Computes the length of a row at the specified index, based on provided metadata (offsets and lengths).
     * <p>
     * The size computation follows these rules:
     * <ul>
     * <li>If {@code offsets} is {@code null}, each row is assumed to have a fixed size of {@code sizePerOffset}.</li>
     * <li>If {@code lengths} is {@code null}, the size is calculated from adjacent elements in {@code offsets}.</li>
     * <li>If both {@code offsets} and {@code lengths} are provided, {@code lengths} determines the row size.</li>
     * </ul>
     *
     * @param ii The row index for which to compute the size.
     * @param sizePerOffset The fixed size for each row, if applicable.
     * @param offsets An {@link IntChunk} describing row start positions, or {@code null}.
     * @param lengths An {@link IntChunk} describing row lengths, or {@code null}.
     * @return The size of the row at the specified index.
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
