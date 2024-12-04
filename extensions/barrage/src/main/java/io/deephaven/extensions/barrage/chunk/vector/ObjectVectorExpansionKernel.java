//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk.vector;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Array;
import java.util.stream.Stream;

public class ObjectVectorExpansionKernel<T> implements VectorExpansionKernel<ObjectVector<T>> {
    private static final String DEBUG_NAME = "ObjectVectorExpansionKernel";

    private final Class<T> componentType;

    public ObjectVectorExpansionKernel(final Class<T> componentType) {
        this.componentType = componentType;
    }

    @Override
    public <A extends Any> WritableChunk<A> expand(
            @NotNull final ObjectChunk<ObjectVector<T>, A> source,
            final int fixedSizeLength,
            @Nullable final WritableIntChunk<ChunkPositions> offsetsDest) {
        if (source.size() == 0) {
            if (offsetsDest != null) {
                offsetsDest.setSize(0);
            }
            return WritableObjectChunk.makeWritableChunk(0);
        }

        final ObjectChunk<ObjectVector<?>, A> typedSource = source.asObjectChunk();

        long totalSize = 0;
        for (int ii = 0; ii < typedSource.size(); ++ii) {
            final ObjectVector<?> row = typedSource.get(ii);
            long rowLen;
            if (fixedSizeLength != 0) {
                rowLen = Math.abs(fixedSizeLength);
            } else {
                rowLen = row == null ? 0 : row.size();
            }
            totalSize += rowLen;
        }
        final WritableObjectChunk<T, A> result = WritableObjectChunk.makeWritableChunk(
                LongSizedDataStructure.intSize(DEBUG_NAME, totalSize));
        result.setSize(0);

        if (offsetsDest != null) {
            offsetsDest.setSize(source.size() + 1);
        }
        for (int ii = 0; ii < typedSource.size(); ++ii) {
            final ObjectVector<?> row = typedSource.get(ii);
            if (offsetsDest != null) {
                offsetsDest.set(ii, result.size());
            }
            if (row != null) {
                try (final CloseableIterator<?> iter = row.iterator()) {
                    Stream<?> stream = iter.stream();
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
                    // noinspection unchecked
                    stream.forEach(v -> result.add((T) v));
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
    public <A extends Any> WritableObjectChunk<ObjectVector<T>, A> contract(
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
        final ObjectChunk<T, A> typedSource = source.asObjectChunk();
        final WritableObjectChunk<ObjectVector<T>, A> result;
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
                // noinspection unchecked
                result.set(outOffset + ii, (ObjectVector<T>) ObjectVectorDirect.ZERO_LENGTH_VECTOR);
            } else {
                // noinspection unchecked
                final T[] row = (T[]) Array.newInstance(componentType, rowLen);
                typedSource.copyToArray(lenRead, row, 0, rowLen);
                lenRead += rowLen;
                result.set(outOffset + ii, new ObjectVectorDirect<>(row));
            }
        }

        return result;
    }
}
