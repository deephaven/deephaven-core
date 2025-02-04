//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
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

import java.util.stream.Stream;

import static io.deephaven.vector.CharVectorDirect.ZERO_LENGTH_VECTOR;

public class CharVectorExpansionKernel implements VectorExpansionKernel<CharVector> {
    public final static CharVectorExpansionKernel INSTANCE = new CharVectorExpansionKernel();

    private static final String DEBUG_NAME = "CharVectorExpansionKernel";

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
            long rowLen;
            if (fixedSizeLength != 0) {
                rowLen = Math.abs(fixedSizeLength);
            } else {
                rowLen = row == null ? 0 : row.size();
            }
            totalSize += rowLen;
        }
        final WritableCharChunk<A> result = WritableCharChunk.makeWritableChunk(
                LongSizedDataStructure.intSize(DEBUG_NAME, totalSize));
        result.setSize(0);

        if (offsetsDest != null) {
            offsetsDest.setSize(source.size() + 1);
        }
        for (int ii = 0; ii < typedSource.size(); ++ii) {
            final CharVector row = typedSource.get(ii);
            if (offsetsDest != null) {
                offsetsDest.set(ii, result.size());
            }
            if (row != null) {
                final CharConsumer consumer = result::add;
                try (final CloseablePrimitiveIteratorOfChar iter = row.iterator()) {
                    Stream<Character> stream = iter.stream();
                    if (fixedSizeLength > 0) {
                        // limit length to fixedSizeLength
                        stream = stream.limit(fixedSizeLength);
                    } else if (fixedSizeLength < 0) {
                        final long numToSkip = Math.max(0, row.size() + fixedSizeLength);
                        if (numToSkip > 0) {
                            // read from the end of the array when fixedSizeLength is negative
                            stream = stream.skip(numToSkip);
                        }
                    }
                    // copy the row into the result
                    stream.forEach(consumer::accept);
                }
            }
            if (fixedSizeLength != 0) {
                final int toNull = LongSizedDataStructure.intSize(
                        DEBUG_NAME, Math.max(0, Math.abs(fixedSizeLength) - (row == null ? 0 : row.size())));
                if (toNull > 0) {
                    // fill the rest of the row with nulls
                    result.fillWithNullValue(result.size(), toNull);
                    result.setSize(result.size() + toNull);
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
            int sizePerElement,
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

        sizePerElement = Math.abs(sizePerElement);
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

        for (int ii = 0; ii < itemsInBatch; ++ii) {
            final int offset = offsets == null ? ii * sizePerElement : offsets.get(ii);
            final int rowLen = computeSize(ii, sizePerElement, offsets, lengths);
            if (rowLen == 0) {
                result.set(outOffset + ii, ZERO_LENGTH_VECTOR);
            } else if (rowLen < 0) {
                // note that this may occur when data sent from a native arrow client is null
                result.set(outOffset + ii, null);
            } else {
                final char[] row = new char[rowLen];
                typedSource.copyToArray(offset, row, 0, rowLen);
                result.set(outOffset + ii, new CharVectorDirect(row));
            }
        }

        return result;
    }
}
