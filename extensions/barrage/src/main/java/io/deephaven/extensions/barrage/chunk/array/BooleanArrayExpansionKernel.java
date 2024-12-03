//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk.array;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class BooleanArrayExpansionKernel implements ArrayExpansionKernel<boolean[]> {
    private final static boolean[] ZERO_LEN_ARRAY = new boolean[0];
    public final static BooleanArrayExpansionKernel INSTANCE = new BooleanArrayExpansionKernel();

    @Override
    public <A extends Any> WritableChunk<A> expand(
            @NotNull final ObjectChunk<boolean[], A> source,
            final int fixedSizeLength,
            @Nullable final WritableIntChunk<ChunkPositions> offsetsDest) {
        if (source.size() == 0) {
            if (offsetsDest != null) {
                offsetsDest.setSize(0);
            }
            return WritableByteChunk.makeWritableChunk(0);
        }

        final ObjectChunk<boolean[], A> typedSource = source.asObjectChunk();

        long totalSize = 0;
        for (int ii = 0; ii < typedSource.size(); ++ii) {
            final boolean[] row = typedSource.get(ii);
            int rowLen = row == null ? 0 : row.length;
            if (fixedSizeLength > 0) {
                rowLen = Math.min(rowLen, fixedSizeLength);
            }
            totalSize += rowLen;
        }
        final WritableByteChunk<A> result = WritableByteChunk.makeWritableChunk(
                LongSizedDataStructure.intSize("ExpansionKernel", totalSize));

        int lenWritten = 0;
        if (offsetsDest != null) {
            offsetsDest.setSize(source.size() + 1);
        }
        for (int ii = 0; ii < typedSource.size(); ++ii) {
            final boolean[] row = typedSource.get(ii);
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
            for (int j = 0; j < rowLen; ++j) {
                final byte value = row[j] ? BooleanUtils.TRUE_BOOLEAN_AS_BYTE : BooleanUtils.FALSE_BOOLEAN_AS_BYTE;
                result.set(lenWritten + j, value);
            }
            lenWritten += rowLen;
        }
        if (offsetsDest != null) {
            offsetsDest.set(typedSource.size(), lenWritten);
        }

        return result;
    }

    @Override
    public <A extends Any> WritableObjectChunk<boolean[], A> contract(
            @NotNull final Chunk<A> source,
            final int sizePerElement,
            @Nullable final IntChunk<ChunkPositions> offsets,
            @Nullable final IntChunk<ChunkLengths> lengths,
            @Nullable final WritableChunk<A> outChunk,
            final int outOffset,
            final int totalRows) {
        if (source.size() == 0) {
            if (outChunk != null) {
                return outChunk.asWritableObjectChunk();
            }
            return WritableObjectChunk.makeWritableChunk(totalRows);
        }

        final int itemsInBatch = offsets == null
                ? source.size() / sizePerElement
                : (offsets.size() - (lengths == null ? 1 : 0));
        final ByteChunk<A> typedSource = source.asByteChunk();
        final WritableObjectChunk<boolean[], A> result;
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
                final boolean[] row = new boolean[rowLen];
                for (int j = 0; j < rowLen; ++j) {
                    row[j] = typedSource.get(lenRead + j) > 0;
                }
                lenRead += rowLen;
                result.set(outOffset + ii, row);
            }
        }

        return result;
    }
}
