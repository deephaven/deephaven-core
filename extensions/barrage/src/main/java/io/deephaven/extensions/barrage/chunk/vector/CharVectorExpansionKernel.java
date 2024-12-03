//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.primitive.function.CharConsumer;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfChar;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.CharVectorDirect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.vector.CharVectorDirect.ZERO_LENGTH_VECTOR;

public class CharVectorExpansionKernel implements VectorExpansionKernel<CharVector> {
    public final static CharVectorExpansionKernel INSTANCE = new CharVectorExpansionKernel();

    @Override
    public <A extends Any> WritableChunk<A> expand(
            @NotNull final ObjectChunk<CharVector, A> source,
            final int fixedSizeLength,
            @Nullable final WritableIntChunk<ChunkPositions> offsetsDest) {
        if (source.size() == 0) {
            if (offsetsDest != null) {
                offsetsDest.setSize(0);
            }
            return WritableCharChunk.makeWritableChunk(0);
        }

        final ObjectChunk<CharVector, A> typedSource = source.asObjectChunk();

        long totalSize = 0;
        for (int ii = 0; ii < typedSource.size(); ++ii) {
            final CharVector row = typedSource.get(ii);
            long rowLen = row == null ? 0 : row.size();
            if (fixedSizeLength > 0) {
                rowLen = Math.min(rowLen, fixedSizeLength);
            }
            totalSize += rowLen;
        }
        final WritableCharChunk<A> result = WritableCharChunk.makeWritableChunk(
                LongSizedDataStructure.intSize("ExpansionKernel", totalSize));
        result.setSize(0);

        if (offsetsDest != null) {
            offsetsDest.setSize(source.size() + 1);
        }
        for (int ii = 0; ii < typedSource.size(); ++ii) {
            final CharVector row = typedSource.get(ii);
            if (offsetsDest != null) {
                offsetsDest.set(ii, result.size());
            }
            if (row == null) {
                continue;
            }
            final CharConsumer consumer = result::add;
            try (final CloseablePrimitiveIteratorOfChar iter = row.iterator()) {
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
    public <A extends Any> WritableObjectChunk<CharVector, A> contract(
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
        final CharChunk<A> typedSource = source.asCharChunk();
        final WritableObjectChunk<CharVector, A> result;
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
                final char[] row = new char[rowLen];
                typedSource.copyToArray(lenRead, row, 0, rowLen);
                lenRead += rowLen;
                result.set(outOffset + ii, new CharVectorDirect(row));
            }
        }

        return result;
    }
}
