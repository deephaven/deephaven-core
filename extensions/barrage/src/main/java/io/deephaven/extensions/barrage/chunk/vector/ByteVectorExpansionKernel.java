//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorExpansionKernel and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk.vector;

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
import io.deephaven.engine.primitive.function.ByteConsumer;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfByte;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.ByteVectorDirect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.vector.ByteVectorDirect.ZERO_LENGTH_VECTOR;

public class ByteVectorExpansionKernel implements VectorExpansionKernel<ByteVector> {
    public final static ByteVectorExpansionKernel INSTANCE = new ByteVectorExpansionKernel();

    @Override
    public <A extends Any> WritableChunk<A> expand(
            @NotNull final ObjectChunk<ByteVector, A> source,
            final int fixedSizeLength,
            @Nullable final WritableIntChunk<ChunkPositions> offsetsDest) {
        if (source.size() == 0) {
            if (offsetsDest != null) {
                offsetsDest.setSize(0);
            }
            return WritableByteChunk.makeWritableChunk(0);
        }

        final ObjectChunk<ByteVector, A> typedSource = source.asObjectChunk();

        long totalSize = 0;
        for (int ii = 0; ii < typedSource.size(); ++ii) {
            final ByteVector row = typedSource.get(ii);
            long rowLen = row == null ? 0 : row.size();
            if (fixedSizeLength > 0) {
                rowLen = Math.min(rowLen, fixedSizeLength);
            }
            totalSize += rowLen;
        }
        final WritableByteChunk<A> result = WritableByteChunk.makeWritableChunk(
                LongSizedDataStructure.intSize("ExpansionKernel", totalSize));
        result.setSize(0);

        if (offsetsDest != null) {
            offsetsDest.setSize(source.size() + 1);
        }
        for (int ii = 0; ii < typedSource.size(); ++ii) {
            final ByteVector row = typedSource.get(ii);
            if (offsetsDest != null) {
                offsetsDest.set(ii, result.size());
            }
            if (row == null) {
                continue;
            }
            final ByteConsumer consumer = result::add;
            try (final CloseablePrimitiveIteratorOfByte iter = row.iterator()) {
                if (fixedSizeLength > 0) {
                    iter.stream().limit(fixedSizeLength).forEach(consumer::accept);
                } else {
                    iter.forEachRemaining(consumer);
                }
            }
        }
        if (offsetsDest != null) {
            offsetsDest.set(typedSource.size(), result.size());
        }

        return result;
    }

    @Override
    public <A extends Any> WritableObjectChunk<ByteVector, A> contract(
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
        final WritableObjectChunk<ByteVector, A> result;
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
                result.set(outOffset + ii, ZERO_LENGTH_VECTOR);
            } else {
                final byte[] row = new byte[rowLen];
                typedSource.copyToArray(lenRead, row, 0, rowLen);
                lenRead += rowLen;
                result.set(outOffset + ii, new ByteVectorDirect(row));
            }
        }

        return result;
    }
}
