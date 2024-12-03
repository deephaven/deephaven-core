//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk.array;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ObjectArrayExpansionKernel<T> implements ArrayExpansionKernel<T[]> {

    private final Class<T> componentType;

    public ObjectArrayExpansionKernel(final Class<T> componentType) {
        this.componentType = componentType;
    }

    @Override
    public <A extends Any> WritableChunk<A> expand(
            @NotNull final ObjectChunk<T[], A> source,
            final int fixedSizeLength,
            @Nullable final WritableIntChunk<ChunkPositions> offsetsDest) {
        if (source.size() == 0) {
            if (offsetsDest != null) {
                offsetsDest.setSize(0);
            }
            return WritableObjectChunk.makeWritableChunk(0);
        }

        final ObjectChunk<T[], A> typedSource = source.asObjectChunk();

        long totalSize = 0;
        for (int ii = 0; ii < typedSource.size(); ++ii) {
            final T[] row = typedSource.get(ii);
            int rowLen = row == null ? 0 : row.length;
            if (fixedSizeLength > 0) {
                rowLen = Math.min(rowLen, fixedSizeLength);
            }
            totalSize += rowLen;
        }
        final WritableObjectChunk<T, A> result = WritableObjectChunk.makeWritableChunk(
                LongSizedDataStructure.intSize("ExpansionKernel", totalSize));

        int lenWritten = 0;
        if (offsetsDest != null) {
            offsetsDest.setSize(source.size() + 1);
        }
        for (int ii = 0; ii < typedSource.size(); ++ii) {
            final T[] row = typedSource.get(ii);
            if (offsetsDest != null) {
                offsetsDest.set(ii, lenWritten);
            }
            if (row == null) {
                continue;
            }
            int rowLen = row.length;
            if (fixedSizeLength > 0) {
                rowLen = Math.min(rowLen, fixedSizeLength);
            }
            result.copyFromArray(row, 0, lenWritten, rowLen);
            lenWritten += rowLen;
        }
        if (offsetsDest != null) {
            offsetsDest.set(typedSource.size(), lenWritten);
        }

        return result;
    }

    @Override
    public <A extends Any> WritableObjectChunk<T[], A> contract(
            @NotNull final Chunk<A> source,
            final int sizePerElement,
            @Nullable final IntChunk<ChunkPositions> offsets,
            @Nullable final IntChunk<ChunkLengths> lengths,
            @Nullable final WritableChunk<A> outChunk,
            final int outOffset,
            final int totalRows) {
        if (lengths != null && lengths.size() == 0
                || lengths == null && offsets != null && offsets.size() <= 1) {
            if (outChunk != null) {
                return outChunk.asWritableObjectChunk();
            }
            final WritableObjectChunk<T[], A> chunk = WritableObjectChunk.makeWritableChunk(totalRows);
            chunk.fillWithNullValue(0, totalRows);
            return chunk;
        }

        final int itemsInBatch = offsets == null
                ? source.size() / sizePerElement
                : (offsets.size() - (lengths == null ? 1 : 0));
        final ObjectChunk<T, A> typedSource = source.asObjectChunk();
        final WritableObjectChunk<T[], A> result;
        if (outChunk != null) {
            result = outChunk.asWritableObjectChunk();
        } else {
            final int numRows = Math.max(itemsInBatch, totalRows);
            result = WritableObjectChunk.makeWritableChunk(numRows);
            result.setSize(numRows);
        }

        int lenRead = 0;
        for (int ii = 0; ii < itemsInBatch; ++ii) {
            final int rowLen = computeSize(ii, sizePerElement, offsets, lengths);
            // noinspection unchecked
            final T[] row = (T[]) ArrayReflectUtil.newInstance(componentType, rowLen);
            if (rowLen != 0) {
                typedSource.copyToArray(lenRead, row, 0, rowLen);
                lenRead += rowLen;
            }
            result.set(outOffset + ii, row);
        }

        return result;
    }
}
