//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorExpansionKernel and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk.vector;

import java.util.function.DoubleConsumer;

import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfDouble;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.DoubleVectorDirect;
import io.deephaven.vector.Vector;

import static io.deephaven.vector.DoubleVectorDirect.ZERO_LENGTH_VECTOR;

public class DoubleVectorExpansionKernel implements VectorExpansionKernel {
    public final static DoubleVectorExpansionKernel INSTANCE = new DoubleVectorExpansionKernel();

    @Override
    public <A extends Any> WritableChunk<A> expand(
            final ObjectChunk<Vector<?>, A> source, final WritableIntChunk<ChunkPositions> perElementLengthDest) {
        if (source.size() == 0) {
            perElementLengthDest.setSize(0);
            return WritableDoubleChunk.makeWritableChunk(0);
        }

        final ObjectChunk<DoubleVector, A> typedSource = source.asObjectChunk();

        long totalSize = 0;
        for (int i = 0; i < typedSource.size(); ++i) {
            final DoubleVector row = typedSource.get(i);
            totalSize += row == null ? 0 : row.size();
        }
        final WritableDoubleChunk<A> result = WritableDoubleChunk.makeWritableChunk(
                LongSizedDataStructure.intSize("ExpansionKernel", totalSize));
        result.setSize(0);

        perElementLengthDest.setSize(source.size() + 1);
        for (int i = 0; i < typedSource.size(); ++i) {
            final DoubleVector row = typedSource.get(i);
            perElementLengthDest.set(i, result.size());
            if (row == null) {
                continue;
            }
            final DoubleConsumer consumer = result::add;
            try (final CloseablePrimitiveIteratorOfDouble iter = row.iterator()) {
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
        final DoubleChunk<A> typedSource = source.asDoubleChunk();
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
                final double[] row = new double[rowLen];
                typedSource.copyToArray(lenRead, row, 0, rowLen);
                lenRead += rowLen;
                result.set(outOffset + i, new DoubleVectorDirect(row));
            }
        }

        return result;
    }
}
