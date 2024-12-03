//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharArrayExpansionKernel and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk.array;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class IntArrayExpansionKernel implements ArrayExpansionKernel<int[]> {
    private final static int[] ZERO_LEN_ARRAY = new int[0];
    public final static IntArrayExpansionKernel INSTANCE = new IntArrayExpansionKernel();

    @Override
    public <A extends Any> WritableChunk<A> expand(
            @NotNull final ObjectChunk<int[], A> source,
            final int fixedSizeLength,
            @Nullable final WritableIntChunk<ChunkPositions> offsetsDest) {
        if (source.size() == 0) {
            if (offsetsDest != null) {
                offsetsDest.setSize(0);
            }
            return WritableIntChunk.makeWritableChunk(0);
        }

        long totalSize = 0;
        for (int ii = 0; ii < source.size(); ++ii) {
            final int[] row = source.get(ii);
            int rowLen = row == null ? 0 : row.length;
            if (fixedSizeLength > 0) {
                rowLen = Math.min(rowLen, fixedSizeLength);
            }
            totalSize += rowLen;
        }
        final WritableIntChunk<A> result = WritableIntChunk.makeWritableChunk(
                LongSizedDataStructure.intSize("ExpansionKernel", totalSize));

        int lenWritten = 0;
        if (offsetsDest != null) {
            offsetsDest.setSize(source.size() + 1);
        }
        for (int ii = 0; ii < source.size(); ++ii) {
            final int[] row = source.get(ii);
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
            offsetsDest.set(source.size(), lenWritten);
        }

        return result;
    }

    @Override
    public <A extends Any> WritableObjectChunk<int[], A> contract(
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
            final WritableObjectChunk<int[], A> chunk = WritableObjectChunk.makeWritableChunk(totalRows);
            chunk.fillWithNullValue(0, totalRows);
            return chunk;
        }

        final int itemsInBatch = offsets == null
                ? source.size() / sizePerElement
                : (offsets.size() - (lengths == null ? 1 : 0));
        final IntChunk<A> typedSource = source.asIntChunk();
        final WritableObjectChunk<int[], A> result;
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
            if (rowLen == 0) {
                result.set(outOffset + ii, ZERO_LEN_ARRAY);
            } else {
                final int[] row = new int[rowLen];
                typedSource.copyToArray(lenRead, row, 0, rowLen);
                lenRead += rowLen;
                result.set(outOffset + ii, row);
            }
        }

        return result;
    }
}
