//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
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
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.extensions.barrage.chunk.BaseChunkReader;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.vector.ObjectVectorDirect.ZERO_LENGTH_VECTOR;

public class BooleanVectorExpansionKernel implements VectorExpansionKernel<ObjectVector<Boolean>> {
    public final static BooleanVectorExpansionKernel INSTANCE = new BooleanVectorExpansionKernel();

    private static final String DEBUG_NAME = "BooleanVectorExpansionKernel";

    @Override
    public <A extends Any> WritableChunk<A> expand(
            @NotNull final ObjectChunk<ObjectVector<Boolean>, A> source,
            final int fixedSizeLength,
            @Nullable final WritableIntChunk<ChunkPositions> offsetsDest) {
        if (source.size() == 0) {
            if (offsetsDest != null) {
                offsetsDest.setSize(0);
            }
            return WritableByteChunk.makeWritableChunk(0);
        }

        final ObjectChunk<ObjectVector<Boolean>, A> typedSource = source.asObjectChunk();

        long totalSize = 0;
        if (fixedSizeLength != 0) {
            totalSize = source.size() * (long) fixedSizeLength;
        } else {
            for (int ii = 0; ii < source.size(); ++ii) {
                final ObjectVector<Boolean> row = typedSource.get(ii);
                final long rowLen = row == null ? 0 : row.size();
                totalSize += rowLen;
            }
        }
        final WritableByteChunk<A> result = WritableByteChunk.makeWritableChunk(
                LongSizedDataStructure.intSize(DEBUG_NAME, totalSize));
        result.setSize(0);

        if (offsetsDest != null) {
            offsetsDest.setSize(source.size() + 1);
        }
        for (int ii = 0; ii < typedSource.size(); ++ii) {
            final ObjectVector<Boolean> row = typedSource.get(ii);
            if (offsetsDest != null) {
                offsetsDest.set(ii, result.size());
            }
            if (row != null) {
                try (final CloseableIterator<Boolean> iter = row.iterator()) {
                    final int numToRead = LongSizedDataStructure.intSize(
                            DEBUG_NAME, fixedSizeLength == 0 ? row.size() : fixedSizeLength);
                    for (int jj = 0; jj < numToRead; ++jj) {
                        result.add(BooleanUtils.booleanAsByte(iter.next()));
                    }
                }
            }
            if (fixedSizeLength != 0) {
                final int toNull = LongSizedDataStructure.intSize(
                        DEBUG_NAME, Math.max(0, fixedSizeLength - (row == null ? 0 : row.size())));
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
    public <A extends Any> WritableObjectChunk<ObjectVector<Boolean>, A> contract(
            @NotNull final Chunk<A> source,
            final int sizePerElement,
            @Nullable final IntChunk<ChunkPositions> offsets,
            @Nullable final IntChunk<ChunkLengths> lengths,
            @Nullable final WritableChunk<A> outChunk,
            final int outOffset,
            final int totalRows) {
        final int itemsInBatch = offsets == null
                ? source.size() / sizePerElement
                : (offsets.size() - (lengths == null ? 1 : 0));
        final ByteChunk<A> typedSource = source.asByteChunk();
        final WritableObjectChunk<ObjectVector<Boolean>, A> result = BaseChunkReader.castOrCreateChunk(
                outChunk,
                outOffset,
                Math.max(totalRows, itemsInBatch),
                WritableObjectChunk::makeWritableChunk,
                WritableChunk::asWritableObjectChunk);

        for (int ii = 0; ii < itemsInBatch; ++ii) {
            final int offset = offsets == null ? ii * sizePerElement : offsets.get(ii);
            final int rowLen = computeSize(ii, sizePerElement, offsets, lengths);
            if (rowLen == 0) {
                // noinspection unchecked
                result.set(outOffset + ii, (ObjectVector<Boolean>) ZERO_LENGTH_VECTOR);
            } else {
                final Boolean[] row = new Boolean[rowLen];
                for (int jj = 0; jj < rowLen; ++jj) {
                    byte val = typedSource.get(offset + jj);
                    row[jj] = BooleanUtils.byteAsBoolean(val);
                }
                result.set(outOffset + ii, new ObjectVectorDirect<>(row));
            }
        }

        return result;
    }
}
