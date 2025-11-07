//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.function.Function;

import static io.deephaven.extensions.barrage.chunk.BaseChunkWriter.getNumLongsForBitPackOfSize;

public class BooleanChunkReader extends BaseChunkReader<WritableByteChunk<Values>> {
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

    public <T> ChunkReader<WritableObjectChunk<T, Values>> transform(Function<Byte, T> transform) {
        return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows) -> {
            try (final WritableByteChunk<Values> inner = BooleanChunkReader.this.readChunk(
                    fieldNodeIter, bufferInfoIter, is, null, 0, 0)) {

                final WritableObjectChunk<T, Values> chunk = castOrCreateChunk(
                        outChunk,
                        outOffset,
                        Math.max(totalRows, inner.size()),
                        WritableObjectChunk::makeWritableChunk,
                        WritableChunk::asWritableObjectChunk);

                if (outChunk == null) {
                    // if we're not given an output chunk then we better be writing at the front of the new one
                    Assert.eqZero(outOffset, "outOffset");
                }

                for (int ii = 0; ii < inner.size(); ++ii) {
                    byte value = inner.get(ii);
                    chunk.set(outOffset + ii, transform.apply(value));
                }

                return chunk;
            }
        };
    }

    @Override
    public WritableByteChunk<Values> readChunk(
            @NotNull final Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
            @NotNull final PrimitiveIterator.OfLong bufferInfoIter,
            @NotNull final DataInput is,
            @Nullable final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {
        final ChunkWriter.FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final long validityBuffer = bufferInfoIter.nextLong();
        final long payloadBuffer = bufferInfoIter.nextLong();

        final WritableByteChunk<Values> chunk = castOrCreateChunk(
                outChunk,
                outOffset,
                Math.max(totalRows, nodeInfo.numElements),
                WritableByteChunk::makeWritableChunk,
                WritableChunk::asWritableByteChunk);

        if (nodeInfo.numElements == 0) {
            return chunk;
        }

        final int numValidityLongs = (nodeInfo.numElements + 63) / 64;
        try (final WritableLongChunk<Values> isValid = WritableLongChunk.makeWritableChunk(numValidityLongs)) {
            readValidityBuffer(is, numValidityLongs, validityBuffer, isValid, DEBUG_NAME);

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
            final ChunkWriter.FieldNodeInfo nodeInfo,
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
