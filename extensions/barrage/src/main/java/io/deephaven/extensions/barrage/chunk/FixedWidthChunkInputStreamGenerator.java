//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

public class FixedWidthChunkInputStreamGenerator {
    private static final String DEBUG_NAME = "FixedWidthChunkInputStreamGenerator";

    @FunctionalInterface
    public interface TypeConversion<T> {
        T apply(DataInput in) throws IOException;
    }

    /**
     * Generic input stream reading from arrow's buffer and convert directly to java type.
     *
     * If useDeephavenNulls is enabled, then the conversion method must properly return a null value.
     *
     * @param elementSize the number of bytes per element (element size is fixed)
     * @param options the stream reader options
     * @param conversion the conversion method from input stream to the result type
     * @param fieldNodeIter arrow field node iterator
     * @param bufferInfoIter arrow buffer info iterator
     * @param outChunk the returned chunk from an earlier record batch
     * @param outOffset the offset to start writing into {@code outChunk}
     * @param totalRows the total known rows for this column; if known (else 0)
     * @param is data input stream
     * @param <T> the result type
     * @return the resulting chunk of the buffer that is read
     */
    public static <T> WritableObjectChunk<T, Values> extractChunkFromInputStreamWithTypeConversion(
            final int elementSize,
            final StreamReaderOptions options,
            final TypeConversion<T> conversion,
            final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter,
            final PrimitiveIterator.OfLong bufferInfoIter,
            final DataInput is,
            final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {

        final ChunkInputStreamGenerator.FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final long validityBuffer = bufferInfoIter.nextLong();
        final long payloadBuffer = bufferInfoIter.nextLong();

        final WritableObjectChunk<T, Values> chunk;
        if (outChunk != null) {
            chunk = outChunk.asWritableObjectChunk();
        } else {
            final int numRows = Math.max(nodeInfo.numElements, totalRows);
            chunk = WritableObjectChunk.makeWritableChunk(numRows);
            chunk.setSize(numRows);
        }

        if (nodeInfo.numElements == 0) {
            return chunk;
        }

        final int numValidityLongs = options.useDeephavenNulls() ? 0 : (nodeInfo.numElements + 63) / 64;
        try (final WritableLongChunk<Values> isValid = WritableLongChunk.makeWritableChunk(numValidityLongs)) {
            if (options.useDeephavenNulls() && validityBuffer != 0) {
                throw new IllegalStateException("validity buffer is non-empty, but is unnecessary");
            }
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

            if (options.useDeephavenNulls()) {
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
            final ChunkInputStreamGenerator.FieldNodeInfo nodeInfo,
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
