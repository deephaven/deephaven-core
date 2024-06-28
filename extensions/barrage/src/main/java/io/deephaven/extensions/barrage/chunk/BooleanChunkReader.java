//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.datastructures.LongSizedDataStructure;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

import static io.deephaven.extensions.barrage.chunk.BaseChunkInputStreamGenerator.getNumLongsForBitPackOfSize;

public class BooleanChunkReader implements ChunkReader {
    private static final String DEBUG_NAME = "BooleanChunkReader";

    @FunctionalInterface
    public interface ByteConversion {
        byte apply(byte in);

        ByteConversion IDENTITY = (byte a) -> a;
    }

    private final ByteConversion conversion;

    public BooleanChunkReader() {
        this(ByteConversion.IDENTITY);
    }

    public BooleanChunkReader(ByteConversion conversion) {
        this.conversion = conversion;
    }

    @Override
    public WritableChunk<Values> readChunk(Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter,
            PrimitiveIterator.OfLong bufferInfoIter, DataInput is, WritableChunk<Values> outChunk, int outOffset,
            int totalRows) throws IOException {
        final ChunkInputStreamGenerator.FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final long validityBuffer = bufferInfoIter.nextLong();
        final long payloadBuffer = bufferInfoIter.nextLong();

        final WritableByteChunk<Values> chunk;
        if (outChunk != null) {
            chunk = outChunk.asWritableByteChunk();
        } else {
            final int numRows = Math.max(totalRows, nodeInfo.numElements);
            chunk = WritableByteChunk.makeWritableChunk(numRows);
            chunk.setSize(numRows);
        }

        if (nodeInfo.numElements == 0) {
            return chunk;
        }

        final int numValidityLongs = (nodeInfo.numElements + 63) / 64;
        try (final WritableLongChunk<Values> isValid = WritableLongChunk.makeWritableChunk(numValidityLongs)) {
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

            final int numPayloadBytesNeeded = (int) ((nodeInfo.numElements + 7L) / 8L);
            if (payloadBuffer < numPayloadBytesNeeded) {
                throw new IllegalStateException("payload buffer is too short for expected number of elements");
            }

            // cannot use deephaven nulls as booleans are not nullable
            useValidityBuffer(conversion, is, nodeInfo, chunk, outOffset, isValid);

            // flight requires that the payload buffer be padded to multiples of 8 bytes
            final long payloadRead = getNumLongsForBitPackOfSize(nodeInfo.numElements) * 8L;
            final long overhangPayload = payloadBuffer - payloadRead;
            if (overhangPayload > 0) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, overhangPayload));
            }
        }

        return chunk;
    }


    private static void useValidityBuffer(
            final ByteConversion conversion,
            final DataInput is,
            final ChunkInputStreamGenerator.FieldNodeInfo nodeInfo,
            final WritableByteChunk<Values> chunk,
            final int offset,
            final WritableLongChunk<Values> isValid) throws IOException {
        final int numElements = nodeInfo.numElements;
        final int numValidityWords = (numElements + 63) / 64;

        int ei = 0;
        int pendingSkips = 0;

        for (int vi = 0; vi < numValidityWords; ++vi) {
            int bitsLeftInThisWord = Math.min(64, numElements - vi * 64);
            long validityWord = isValid.get(vi);
            long payloadWord = is.readLong();
            do {
                if ((validityWord & 1) == 1) {
                    if (pendingSkips > 0) {
                        chunk.fillWithNullValue(offset + ei, pendingSkips);
                        ei += pendingSkips;
                        pendingSkips = 0;
                    }
                    final byte value = (payloadWord & 1) == 1 ? BooleanUtils.TRUE_BOOLEAN_AS_BYTE
                            : BooleanUtils.FALSE_BOOLEAN_AS_BYTE;
                    chunk.set(offset + ei++, conversion.apply(value));
                    validityWord >>= 1;
                    payloadWord >>= 1;
                    bitsLeftInThisWord--;
                } else {
                    final int skips = Math.min(Long.numberOfTrailingZeros(validityWord), bitsLeftInThisWord);
                    pendingSkips += skips;
                    validityWord >>= skips;
                    payloadWord >>= skips;
                    bitsLeftInThisWord -= skips;
                }
            } while (bitsLeftInThisWord > 0);
        }

        if (pendingSkips > 0) {
            chunk.fillWithNullValue(offset + ei, pendingSkips);
        }
    }
}
