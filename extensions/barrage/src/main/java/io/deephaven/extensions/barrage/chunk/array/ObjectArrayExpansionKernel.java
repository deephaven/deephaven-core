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
    private static final String DEBUG_NAME = "ObjectArrayExpansionKernel";

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
            final int rowLen;
            if (fixedSizeLength != 0) {
                rowLen = Math.abs(fixedSizeLength);
            } else {
                final T[] row = typedSource.get(ii);
                rowLen = row == null ? 0 : row.length;
            }
            totalSize += rowLen;
        }
        final WritableObjectChunk<T, A> result = WritableObjectChunk.makeWritableChunk(
                LongSizedDataStructure.intSize(DEBUG_NAME, totalSize));

        int lenWritten = 0;
        if (offsetsDest != null) {
            offsetsDest.setSize(source.size() + 1);
        }
        for (int ii = 0; ii < typedSource.size(); ++ii) {
            final T[] row = typedSource.get(ii);
            if (offsetsDest != null) {
                offsetsDest.set(ii, lenWritten);
            }
            int written = 0;
            if (row != null) {
                int offset = 0;
                if (fixedSizeLength != 0) {
                    // limit length to fixedSizeLength
                    written = Math.min(row.length, Math.abs(fixedSizeLength));
                    if (fixedSizeLength < 0 && written < row.length) {
                        // read from the end of the array when fixedSizeLength is negative
                        offset = row.length - written;
                    }
                } else {
                    written = row.length;
                }
                // copy the row into the result
                result.copyFromArray(row, 0, lenWritten, written);
            }
            if (fixedSizeLength != 0) {
                final int toNull = LongSizedDataStructure.intSize(
                        DEBUG_NAME, Math.max(0, Math.abs(fixedSizeLength) - written));
                if (toNull > 0) {
                    // fill the rest of the row with nulls
                    result.fillWithNullValue(lenWritten + written, toNull);
                    written += toNull;
                }
            }
            lenWritten += written;
        }
        if (offsetsDest != null) {
            offsetsDest.set(typedSource.size(), lenWritten);
        }

        return result;
    }

    @Override
    public <A extends Any> WritableObjectChunk<T[], A> contract(
            @NotNull final Chunk<A> source,
            int sizePerElement,
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

        sizePerElement = Math.abs(sizePerElement);
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
