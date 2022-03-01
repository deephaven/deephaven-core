package io.deephaven.extensions.barrage.chunk;

import gnu.trove.iterator.TLongIterator;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

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
     * @param elementSize    the number of bytes per element (element size is fixed)
     * @param options        the stream reader options
     * @param conversion     the conversion method from input stream to the result type
     * @param fieldNodeIter  arrow field node iterator
     * @param bufferInfoIter arrow buffer info iterator
     * @param is             data input stream
     * @param <T>            the result type
     * @return               the resulting chunk of the buffer that is read
     */
    static <T> ObjectChunk<T, Values> extractChunkFromInputStreamWithTypeConversion(
            final int elementSize,
            final StreamReaderOptions options,
            final TypeConversion<T> conversion,
            final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter,
            final TLongIterator bufferInfoIter,
            final DataInput is) throws IOException {

        final ChunkInputStreamGenerator.FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final long validityBuffer = bufferInfoIter.next();
        final long payloadBuffer = bufferInfoIter.next();

        final WritableObjectChunk<T, Values> chunk = WritableObjectChunk.makeWritableChunk(nodeInfo.numElements);

        if (nodeInfo.numElements == 0) {
            return chunk;
        }

        final int numValidityLongs = options.useDeephavenNulls() ? 0 : (nodeInfo.numElements + 63) / 64;
        try (final WritableLongChunk<Values> isValid = WritableLongChunk.makeWritableChunk(numValidityLongs)) {
            if (options.useDeephavenNulls() && validityBuffer != 0) {
                throw new IllegalStateException("validity buffer is non-empty, but is unnecessary");
            }
            int jj = 0;
            for (; jj < Math.min(numValidityLongs, validityBuffer / 8); ++jj) {
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
                    chunk.set(ii, conversion.apply(is));
                }
            } else {
                useValidityBuffer(elementSize, conversion, is, nodeInfo, chunk, isValid);
            }

            final long overhangPayload = payloadBuffer - payloadRead;
            if (overhangPayload > 0) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, overhangPayload));
            }
        }

        chunk.setSize(nodeInfo.numElements);
        return chunk;
    }

    private static <T> void useValidityBuffer(
            final int elementSize,
            final TypeConversion<T> conversion,
            final DataInput is,
            final ChunkInputStreamGenerator.FieldNodeInfo nodeInfo,
            final WritableObjectChunk<T, Values> chunk,
            final WritableLongChunk<Values> isValid) throws IOException {
        long nextValid = 0;
        for (int ii = 0; ii < nodeInfo.numElements; ) {
            if ((ii % 64) == 0) {
                nextValid = isValid.get(ii / 64);
            }
            int maxToSkip = Math.min(nodeInfo.numElements - ii, 64 - (ii % 64));
            int numToSkip = Math.min(maxToSkip, Long.numberOfTrailingZeros(nextValid));

            if (numToSkip > 0) {
                is.skipBytes(numToSkip * elementSize);
                nextValid >>= numToSkip;
                for (int jj = 0; jj < numToSkip; ++jj) {
                    chunk.set(ii + jj, null);
                }
                ii += numToSkip;
            }
            if (maxToSkip > numToSkip) {
                final T value = conversion.apply(is);
                nextValid >>= 1;
                chunk.set(ii, value);
                ++ii;
            }
        }
    }
}
