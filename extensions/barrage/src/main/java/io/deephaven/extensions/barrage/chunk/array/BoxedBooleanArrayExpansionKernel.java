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
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.datastructures.LongSizedDataStructure;

public class BoxedBooleanArrayExpansionKernel implements ArrayExpansionKernel {
    private final static Boolean[] ZERO_LEN_ARRAY = new Boolean[0];
    public final static BoxedBooleanArrayExpansionKernel INSTANCE = new BoxedBooleanArrayExpansionKernel();

    @Override
    public <T, A extends Any> WritableChunk<A> expand(final ObjectChunk<T, A> source,
            final WritableIntChunk<ChunkPositions> perElementLengthDest) {
        if (source.size() == 0) {
            perElementLengthDest.setSize(0);
            return WritableByteChunk.makeWritableChunk(0);
        }

        final ObjectChunk<Boolean[], A> typedSource = source.asObjectChunk();

        long totalSize = 0;
        for (int i = 0; i < typedSource.size(); ++i) {
            final Boolean[] row = typedSource.get(i);
            totalSize += row == null ? 0 : row.length;
        }
        final WritableByteChunk<A> result = WritableByteChunk.makeWritableChunk(
                LongSizedDataStructure.intSize("ExpansionKernel", totalSize));

        int lenWritten = 0;
        perElementLengthDest.setSize(source.size() + 1);
        for (int i = 0; i < typedSource.size(); ++i) {
            final Boolean[] row = typedSource.get(i);
            perElementLengthDest.set(i, lenWritten);
            if (row == null) {
                continue;
            }
            for (int j = 0; j < row.length; ++j) {
                final byte value = BooleanUtils.booleanAsByte(row[j]);
                result.set(lenWritten + j, value);
            }
            lenWritten += row.length;
        }
        perElementLengthDest.set(typedSource.size(), lenWritten);

        return result;
    }

    @Override
    public <T, A extends Any> WritableObjectChunk<T, A> contract(
            final Chunk<A> source, final IntChunk<ChunkPositions> perElementLengthDest,
            final WritableChunk<A> outChunk, final int outOffset, final int totalRows) {
        if (perElementLengthDest.size() == 0) {
            if (outChunk != null) {
                return outChunk.asWritableObjectChunk();
            }
            return WritableObjectChunk.makeWritableChunk(totalRows);
        }

        final int itemsInBatch = perElementLengthDest.size() - 1;
        final ByteChunk<A> typedSource = source.asByteChunk();
        final WritableObjectChunk<Object, A> result;
        if (outChunk != null) {
            result = outChunk.asWritableObjectChunk();
        } else {
            final int numRows = Math.max(itemsInBatch, totalRows);
            result = WritableObjectChunk.makeWritableChunk(numRows);
            result.setSize(numRows);
        }

        int lenRead = 0;
        for (int i = 0; i < itemsInBatch; ++i) {
            final int rowLen = perElementLengthDest.get(i + 1) - perElementLengthDest.get(i);
            if (rowLen == 0) {
                result.set(outOffset + i, ZERO_LEN_ARRAY);
            } else {
                final Boolean[] row = new Boolean[rowLen];
                for (int j = 0; j < rowLen; ++j) {
                    row[j] = BooleanUtils.byteAsBoolean(typedSource.get(lenRead + j));
                }
                lenRead += rowLen;
                result.set(outOffset + i, row);
            }
        }

        // noinspection unchecked
        return (WritableObjectChunk<T, A>) result;
    }
}
