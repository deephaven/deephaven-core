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
        for (int ii = 0; ii < typedSource.size(); ++ii) {
            int rowLen;
            if (fixedSizeLength > 0) {
                rowLen = Math.abs(fixedSizeLength);
            } else {
                final Boolean[] row = typedSource.get(ii);
                rowLen = row == null ? 0 : row.length;
            }
            totalSize += rowLen;
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
                for (int j = 0; j < written; ++j) {
                    final byte value = BooleanUtils.booleanAsByte(row[j]);
                    result.set(lenWritten + j, value);
                }
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
    public <A extends Any> WritableObjectChunk<Boolean[], A> contract(
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
        final WritableObjectChunk<Boolean[], A> result;
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
                final Boolean[] row = new Boolean[rowLen];
                for (int j = 0; j < rowLen; ++j) {
                    row[j] = BooleanUtils.byteAsBoolean(typedSource.get(lenRead + j));
                }
                lenRead += rowLen;
                result.set(outOffset + ii, row);
            }
        }

        return result;
    }
}
