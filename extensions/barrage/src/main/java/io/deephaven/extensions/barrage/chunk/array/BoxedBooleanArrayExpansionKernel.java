//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
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
import io.deephaven.extensions.barrage.chunk.BaseChunkReader;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class BoxedBooleanArrayExpansionKernel implements ArrayExpansionKernel<Boolean[]> {
    public static final BoxedBooleanArrayExpansionKernel INSTANCE = new BoxedBooleanArrayExpansionKernel();

    private static final String DEBUG_NAME = "BoxedBooleanArrayExpansionKernel";
    private static final Boolean[] ZERO_LEN_ARRAY = new Boolean[0];

    @Override
    public <A extends Any> WritableChunk<A> expand(
            @NotNull final ObjectChunk<Boolean[], A> source,
            final int fixedSizeLength,
            @Nullable final WritableIntChunk<ChunkPositions> offsetsDest) {
        if (source.size() == 0) {
            if (offsetsDest != null) {
                offsetsDest.setSize(0);
            }
            return WritableByteChunk.makeWritableChunk(0);
        }

        final ObjectChunk<Boolean[], A> typedSource = source.asObjectChunk();

        long totalSize = 0;
        if (fixedSizeLength != 0) {
            totalSize = source.size() * (long) fixedSizeLength;
        } else {
            for (int ii = 0; ii < source.size(); ++ii) {
                final Boolean[] row = typedSource.get(ii);
                final int rowLen = row == null ? 0 : row.length;
                totalSize += rowLen;
            }
        }
        final WritableByteChunk<A> result = WritableByteChunk.makeWritableChunk(
                LongSizedDataStructure.intSize(DEBUG_NAME, totalSize));

        int lenWritten = 0;
        if (offsetsDest != null) {
            offsetsDest.setSize(source.size() + 1);
        }
        for (int ii = 0; ii < typedSource.size(); ++ii) {
            final Boolean[] row = typedSource.get(ii);
            if (offsetsDest != null) {
                offsetsDest.set(ii, lenWritten);
            }
            int written = 0;
            if (row != null) {
                if (fixedSizeLength != 0) {
                    // limit length to fixedSizeLength
                    written = Math.min(row.length, fixedSizeLength);
                } else {
                    written = row.length;
                }

                // copy the row into the result
                for (int j = 0; j < written; ++j) {
                    final byte value = BooleanUtils.booleanAsByte(row[j]);
                    result.set(lenWritten + j, value);
                }
            }
            if (fixedSizeLength != 0) {
                final int toNull = LongSizedDataStructure.intSize(
                        DEBUG_NAME, Math.max(0, fixedSizeLength - written));
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
    public <A extends Any> WritableObjectChunk<Boolean[], A> contract(
            @NotNull final Chunk<A> source,
            final int sizePerElement,
            @Nullable final IntChunk<ChunkPositions> offsets,
            @Nullable final IntChunk<ChunkLengths> lengths,
            @Nullable final WritableChunk<A> outChunk,
            final int outOffset,
            final int totalRows) {
        final int itemsInBatch = offsets == null
                ? source.size() / sizePerElement
                : (offsets.size() - (lengths == null ? 1 : 0));
        final ByteChunk<A> typedSource = source.asByteChunk();
        final WritableObjectChunk<Boolean[], A> result = BaseChunkReader.castOrCreateChunk(
                outChunk,
                outOffset,
                Math.max(totalRows, itemsInBatch),
                WritableObjectChunk::makeWritableChunk,
                WritableChunk::asWritableObjectChunk);

        for (int ii = 0; ii < itemsInBatch; ++ii) {
            final int offset = offsets == null ? ii * sizePerElement : offsets.get(ii);
            final int rowLen = computeSize(ii, sizePerElement, offsets, lengths);
            if (rowLen == 0) {
                result.set(outOffset + ii, ZERO_LEN_ARRAY);
            } else {
                final Boolean[] row = new Boolean[rowLen];
                int numSent = Math.min(rowLen, typedSource.size() - offset);
                for (int j = 0; j < numSent; ++j) {
                    row[j] = BooleanUtils.byteAsBoolean(typedSource.get(offset + j));
                }
                result.set(outOffset + ii, row);
            }
        }

        return result;
    }
}
