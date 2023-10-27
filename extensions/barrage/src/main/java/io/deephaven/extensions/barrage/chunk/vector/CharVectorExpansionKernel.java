/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.barrage.chunk.vector;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.primitive.function.CharConsumer;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfChar;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.CharVectorDirect;
import io.deephaven.vector.Vector;

import static io.deephaven.vector.CharVectorDirect.ZERO_LENGTH_VECTOR;

public class CharVectorExpansionKernel implements VectorExpansionKernel {
    public final static CharVectorExpansionKernel INSTANCE = new CharVectorExpansionKernel();

    @Override
    public <A extends Any> WritableChunk<A> expand(
            final ObjectChunk<Vector<?>, A> source, final WritableIntChunk<ChunkPositions> perElementLengthDest) {
        if (source.size() == 0) {
            perElementLengthDest.setSize(0);
            return WritableCharChunk.makeWritableChunk(0);
        }

        final ObjectChunk<CharVector, A> typedSource = source.asObjectChunk();

        long totalSize = 0;
        for (int i = 0; i < typedSource.size(); ++i) {
            final CharVector row = typedSource.get(i);
            totalSize += row == null ? 0 : row.size();
        }
        final WritableCharChunk<A> result = WritableCharChunk.makeWritableChunk(
                LongSizedDataStructure.intSize("ExpansionKernel", totalSize));
        result.setSize(0);

        perElementLengthDest.setSize(source.size() + 1);
        for (int i = 0; i < typedSource.size(); ++i) {
            final CharVector row = typedSource.get(i);
            perElementLengthDest.set(i, result.size());
            if (row == null) {
                continue;
            }
            final CharConsumer consumer = result::add;
            try (final CloseablePrimitiveIteratorOfChar iter = row.iterator()) {
                iter.forEachRemaining(consumer);
            }
        }
        perElementLengthDest.set(typedSource.size(), result.size());

        return result;
    }

    @Override
    public <A extends Any> WritableObjectChunk<Vector<?>, A> contract(
            final Chunk<A> source, final IntChunk<ChunkPositions> perElementLengthDest,
            final WritableChunk<A> outChunk, final int outOffset, final int totalRows) {
        if (perElementLengthDest.size() == 0) {
            if (outChunk != null) {
                return outChunk.asWritableObjectChunk();
            }
            return WritableObjectChunk.makeWritableChunk(totalRows);
        }

        final int itemsInBatch = perElementLengthDest.size() - 1;
        final CharChunk<A> typedSource = source.asCharChunk();
        final WritableObjectChunk<Vector<?>, A> result;
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
                result.set(outOffset + i, ZERO_LENGTH_VECTOR);
            } else {
                final char[] row = new char[rowLen];
                typedSource.copyToArray(lenRead, row, 0, rowLen);
                lenRead += rowLen;
                result.set(outOffset + i, new CharVectorDirect(row));
            }
        }

        return result;
    }
}
