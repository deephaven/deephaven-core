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

public class ObjectVectorExpansionKernel<T> implements VectorExpansionKernel<ObjectVector<T>> {
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
            long rowLen = row == null ? 0 : row.size();
            if (fixedSizeLength > 0) {
                rowLen = Math.min(rowLen, fixedSizeLength);
            }
            totalSize += rowLen;
        }
        final WritableObjectChunk<T, A> result = WritableObjectChunk.makeWritableChunk(
                LongSizedDataStructure.intSize("ExpansionKernel", totalSize));
        result.setSize(0);

        if (offsetsDest != null) {
            offsetsDest.setSize(source.size() + 1);
        }
        for (int ii = 0; ii < typedSource.size(); ++ii) {
            final ObjectVector<?> row = typedSource.get(ii);
            if (offsetsDest != null) {
                offsetsDest.set(ii, result.size());
            }
            if (row == null) {
                continue;
            }
            try (final CloseableIterator<?> iter = row.iterator()) {
                if (fixedSizeLength > 0) {
                    // noinspection unchecked
                    iter.stream().limit(fixedSizeLength).forEach(v -> result.add((T) v));
                } else {
                    // noinspection unchecked
                    iter.forEachRemaining(v -> result.add((T) v));
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
