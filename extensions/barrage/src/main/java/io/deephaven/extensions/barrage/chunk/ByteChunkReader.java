//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkReader and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

/**
 * Replication source for the other fixed-width primitive readers; see {@code ReplicateBarrageUtils}. Edits here must be
 * followed by {@code ./gradlew replicateBarrageUtils}. Keep this file ASCII-only: the replicator does not round-trip
 * non-ASCII text. The {@code Payload*} regions are overridden for {@code byte}, which needs no byte-order decoding.
 */
public class ByteChunkReader extends BaseChunkReader<WritableByteChunk<Values>> {
    private static final String DEBUG_NAME = "ByteChunkReader";

    // Number of elements decoded per bounded bulk-read window (see BaseChunkReader#BULK_READ_BUFFER_BYTES).
    private static final int BULK_READ_ELEMENTS = Math.max(1, BULK_READ_BUFFER_BYTES / Byte.BYTES);

    public static <WIRE_CHUNK_TYPE extends WritableChunk<Values>, T extends ChunkReader<WIRE_CHUNK_TYPE>> ChunkReader<WritableByteChunk<Values>> transformFrom(
            final T wireReader,
            final ChunkTransformer<WIRE_CHUNK_TYPE, WritableByteChunk<Values>> wireTransform) {
        return new TransformingChunkReader<>(
                wireReader,
                WritableByteChunk::makeWritableChunk,
                WritableChunk::asWritableByteChunk,
                wireTransform);
    }

    private final BarrageOptions options;

    public ByteChunkReader(BarrageOptions options) {
        this.options = options;
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

        final int numValidityLongs = options.useDeephavenNulls() ? 0 : (nodeInfo.numElements + 63) / 64;
        try (final WritableLongChunk<Values> isValid = WritableLongChunk.makeWritableChunk(numValidityLongs)) {
            readValidityBuffer(is, numValidityLongs, validityBuffer, isValid, DEBUG_NAME);

            final long payloadRead = (long) nodeInfo.numElements * Byte.BYTES;
            Assert.geq(payloadBuffer, "payloadBuffer", payloadRead, "payloadRead");

            if (options.useDeephavenNulls()) {
                useDeephavenNulls(is, nodeInfo, chunk, outOffset);
            } else {
                useValidityBuffer(is, nodeInfo, chunk, outOffset, isValid);
            }

            final long overhangPayload = payloadBuffer - payloadRead;
            if (overhangPayload > 0) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, overhangPayload));
            }
        }

        return chunk;
    }

    private static void useDeephavenNulls(
            final DataInput is,
            final ChunkWriter.FieldNodeInfo nodeInfo,
            final WritableByteChunk<Values> chunk,
            final int offset) throws IOException {
        final int numElements = nodeInfo.numElements;
        // region PayloadDhNulls
        // Bytes have no endianness, so transfer the payload straight into the chunk's backing array in
        // bounded windows rather than decoding element by element through a staging buffer.
        for (int ei = 0; ei < numElements;) {
            final int length = Math.min(BULK_READ_ELEMENTS, numElements - ei);
            is.readFully(chunk.array(), chunk.arrayOffset() + offset + ei, length);
            ei += length;
        }
        // endregion PayloadDhNulls
    }

    private static void useValidityBuffer(
            final DataInput is,
            final ChunkWriter.FieldNodeInfo nodeInfo,
            final WritableByteChunk<Values> chunk,
            final int offset,
            final WritableLongChunk<Values> isValid) throws IOException {
        final int numElements = nodeInfo.numElements;
        final int numValidityWords = (numElements + 63) / 64;

        // region PayloadValidityBuffer
        // The payload carries a value slot for every element, including nulls; transfer it straight into
        // the chunk's backing array in bounded windows, then overwrite the invalid positions with null.
        for (int ei = 0; ei < numElements;) {
            final int length = Math.min(BULK_READ_ELEMENTS, numElements - ei);
            is.readFully(chunk.array(), chunk.arrayOffset() + offset + ei, length);
            ei += length;
        }
        // endregion PayloadValidityBuffer

        int ei = 0;
        for (int vi = 0; vi < numValidityWords; ++vi) {
            int bitsLeftInThisWord = Math.min(64, numElements - vi * 64);
            long validityWord = isValid.get(vi);
            do {
                if ((validityWord & 1) == 1) {
                    // Skip the run of valid slots (already decoded) to the next null.
                    final int valids = Math.min(Long.numberOfTrailingZeros(~validityWord), bitsLeftInThisWord);
                    ei += valids;
                    validityWord >>= valids;
                    bitsLeftInThisWord -= valids;
                } else {
                    final int nulls = Math.min(Long.numberOfTrailingZeros(validityWord), bitsLeftInThisWord);
                    chunk.fillWithNullValue(offset + ei, nulls);
                    ei += nulls;
                    validityWord >>= nulls;
                    bitsLeftInThisWord -= nulls;
                }
            } while (bitsLeftInThisWord > 0);
        }
    }
}
