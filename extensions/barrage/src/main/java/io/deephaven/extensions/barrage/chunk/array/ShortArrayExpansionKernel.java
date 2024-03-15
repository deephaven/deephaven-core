//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharArrayExpansionKernel and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk.array;

import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.util.datastructures.LongSizedDataStructure;

public class ShortArrayExpansionKernel implements ArrayExpansionKernel {
    private final static short[] ZERO_LEN_ARRAY = new short[0];
    public final static ShortArrayExpansionKernel INSTANCE = new ShortArrayExpansionKernel();

    @Override
    public <T, A extends Any> WritableChunk<A> expand(final ObjectChunk<T, A> source,
            final WritableIntChunk<ChunkPositions> perElementLengthDest) {
        if (source.size() == 0) {
            perElementLengthDest.setSize(0);
            return WritableShortChunk.makeWritableChunk(0);
        }

        final ObjectChunk<short[], A> typedSource = source.asObjectChunk();

        long totalSize = 0;
        for (int i = 0; i < typedSource.size(); ++i) {
            final short[] row = typedSource.get(i);
            totalSize += row == null ? 0 : row.length;
        }
        final WritableShortChunk<A> result = WritableShortChunk.makeWritableChunk(
                LongSizedDataStructure.intSize("ExpansionKernel", totalSize));

        int lenWritten = 0;
        perElementLengthDest.setSize(source.size() + 1);
        for (int i = 0; i < typedSource.size(); ++i) {
            final short[] row = typedSource.get(i);
            perElementLengthDest.set(i, lenWritten);
            if (row == null) {
                continue;
            }
            result.copyFromArray(row, 0, lenWritten, row.length);
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
        final ShortChunk<A> typedSource = source.asShortChunk();
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
                final short[] row = new short[rowLen];
                typedSource.copyToArray(lenRead, row, 0, rowLen);
                lenRead += rowLen;
                result.set(outOffset + i, row);
            }
        }

        // noinspection unchecked
        return (WritableObjectChunk<T, A>) result;
    }
}
