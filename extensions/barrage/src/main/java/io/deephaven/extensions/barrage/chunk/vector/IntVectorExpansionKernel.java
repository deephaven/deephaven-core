/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharVectorExpansionKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.extensions.barrage.chunk.vector;

import java.util.function.IntConsumer;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfInt;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.IntVectorDirect;
import io.deephaven.vector.Vector;

import static io.deephaven.vector.IntVectorDirect.ZERO_LENGTH_VECTOR;

public class IntVectorExpansionKernel implements VectorExpansionKernel {
    public final static IntVectorExpansionKernel INSTANCE = new IntVectorExpansionKernel();

    @Override
    public <A extends Any> WritableChunk<A> expand(
            final ObjectChunk<Vector<?>, A> source, final WritableIntChunk<ChunkPositions> perElementLengthDest) {
        if (source.size() == 0) {
            perElementLengthDest.setSize(0);
            return WritableIntChunk.makeWritableChunk(0);
        }

        final ObjectChunk<IntVector, A> typedSource = source.asObjectChunk();

        long totalSize = 0;
        for (int i = 0; i < typedSource.size(); ++i) {
            final IntVector row = typedSource.get(i);
            totalSize += row == null ? 0 : row.size();
        }
        final WritableIntChunk<A> result = WritableIntChunk.makeWritableChunk(
                LongSizedDataStructure.intSize("ExpansionKernel", totalSize));
        result.setSize(0);

        perElementLengthDest.setSize(source.size() + 1);
        for (int i = 0; i < typedSource.size(); ++i) {
            final IntVector row = typedSource.get(i);
            perElementLengthDest.set(i, result.size());
            if (row == null) {
                continue;
            }
            final IntConsumer consumer = result::add;
            try (final CloseablePrimitiveIteratorOfInt iter = row.iterator()) {
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
        final IntChunk<A> typedSource = source.asIntChunk();
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
                final int[] row = new int[rowLen];
                typedSource.copyToArray(lenRead, row, 0, rowLen);
                lenRead += rowLen;
                result.set(outOffset + i, new IntVectorDirect(row));
            }
        }

        return result;
    }
}
