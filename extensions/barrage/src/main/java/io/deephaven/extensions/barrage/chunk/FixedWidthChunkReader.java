//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

public class FixedWidthChunkReader<T> extends BaseChunkReader<WritableObjectChunk<T, Values>> {
    private static final String DEBUG_NAME = "FixedWidthWriter";

    @FunctionalInterface
    public interface TypeConversion<T> {
        T apply(DataInput in) throws IOException;
    }

    private final boolean useDeephavenNulls;
    private final int elementSize;
    private final BarrageOptions options;
    private final TypeConversion<T> conversion;

    public FixedWidthChunkReader(
            final int elementSize,
            final boolean dhNullable,
            final BarrageOptions options,
            final TypeConversion<T> conversion) {
        this.elementSize = elementSize;
        this.options = options;
        this.conversion = conversion;
        this.useDeephavenNulls = dhNullable && options.useDeephavenNulls();
    }

    @Override
    public WritableObjectChunk<T, Values> readChunk(
            @NotNull final Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
            @NotNull final PrimitiveIterator.OfLong bufferInfoIter,
            @NotNull final DataInput is,
            @Nullable final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {
        final ChunkWriter.FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final long validityBuffer = bufferInfoIter.nextLong();
        final long payloadBuffer = bufferInfoIter.nextLong();

        final WritableObjectChunk<T, Values> chunk = castOrCreateChunk(
                outChunk,
                outOffset,
                Math.max(totalRows, nodeInfo.numElements),
                WritableObjectChunk::makeWritableChunk,
                WritableChunk::asWritableObjectChunk);

        if (nodeInfo.numElements == 0) {
            return chunk;
        }

        final int numValidityLongs = options.useDeephavenNulls() ? 0 : (nodeInfo.numElements + 63) / 64;
        try (final WritableLongChunk<Values> isValid = WritableLongChunk.makeWritableChunk(numValidityLongs)) {
            int jj = 0;
            final long numValidityLongsPresent = Math.min(numValidityLongs, validityBuffer / 8);
            for (; jj < numValidityLongsPresent; ++jj) {
                isValid.set(jj, is.readLong());
            }
            final long valBufRead = jj * 8L;
            if (valBufRead < validityBuffer) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, validityBuffer - valBufRead));
            }
            // we support short validity buffers
            for (; jj < numValidityLongs; ++jj) {
                isValid.set(jj, -1); // -1 is bit-wise representation of all ones
            }
            // consumed entire validity buffer by here

            final long payloadRead = (long) nodeInfo.numElements * elementSize;
            if (payloadBuffer < payloadRead) {
                throw new IllegalStateException("payload buffer is too short for expected number of elements");
            }

            if (useDeephavenNulls) {
                for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                    chunk.set(outOffset + ii, conversion.apply(is));
                }
            } else {
                useValidityBuffer(elementSize, conversion, is, nodeInfo, chunk, outOffset, isValid);
            }

            final long overhangPayload = payloadBuffer - payloadRead;
            if (overhangPayload > 0) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, overhangPayload));
            }
        }

        return chunk;
    }

    private static <T> void useValidityBuffer(
            final int elementSize,
            final TypeConversion<T> conversion,
            final DataInput is,
            final ChunkWriter.FieldNodeInfo nodeInfo,
            final WritableObjectChunk<T, Values> chunk,
            final int outOffset,
            final WritableLongChunk<Values> isValid) throws IOException {
        final int numElements = nodeInfo.numElements;
        final int numValidityWords = (numElements + 63) / 64;

        int ei = 0;
        int pendingSkips = 0;

        for (int vi = 0; vi < numValidityWords; ++vi) {
            int bitsLeftInThisWord = Math.min(64, numElements - vi * 64);
            long validityWord = isValid.get(vi);
            do {
                if ((validityWord & 1) == 1) {
                    if (pendingSkips > 0) {
                        is.skipBytes(pendingSkips * elementSize);
                        chunk.fillWithNullValue(outOffset + ei, pendingSkips);
                        ei += pendingSkips;
                        pendingSkips = 0;
                    }
                    chunk.set(outOffset + ei++, conversion.apply(is));
                    validityWord >>= 1;
                    bitsLeftInThisWord--;
                } else {
                    final int skips = Math.min(Long.numberOfTrailingZeros(validityWord), bitsLeftInThisWord);
                    pendingSkips += skips;
                    validityWord >>= skips;
                    bitsLeftInThisWord -= skips;
                }
            } while (bitsLeftInThisWord > 0);
        }

        if (pendingSkips > 0) {
            is.skipBytes(pendingSkips * elementSize);
            chunk.fillWithNullValue(outOffset + ei, pendingSkips);
        }
    }
}
