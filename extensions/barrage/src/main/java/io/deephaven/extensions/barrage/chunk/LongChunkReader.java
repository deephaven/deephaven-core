//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkReader and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

public class LongChunkReader extends BaseChunkReader<WritableLongChunk<Values>> {
    private static final String DEBUG_NAME = "LongChunkReader";

    // Number of elements decoded per bounded bulk-read window (see BaseChunkReader#BULK_READ_BUFFER_BYTES).
    private static final int BULK_READ_ELEMENTS = Math.max(1, BULK_READ_BUFFER_BYTES / Long.BYTES);

    public static <WIRE_CHUNK_TYPE extends WritableChunk<Values>, T extends ChunkReader<WIRE_CHUNK_TYPE>> ChunkReader<WritableLongChunk<Values>> transformFrom(
            final T wireReader,
            final ChunkTransformer<WIRE_CHUNK_TYPE, WritableLongChunk<Values>> wireTransform) {
        return new TransformingChunkReader<>(
                wireReader,
                WritableLongChunk::makeWritableChunk,
                WritableChunk::asWritableLongChunk,
                wireTransform);
    }

    private final BarrageOptions options;

    public LongChunkReader(BarrageOptions options) {
        this.options = options;
    }

    @Override
    public WritableLongChunk<Values> readChunk(
            @NotNull final Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
            @NotNull final PrimitiveIterator.OfLong bufferInfoIter,
            @NotNull final DataInput is,
            @Nullable final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {

        final ChunkWriter.FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final long validityBuffer = bufferInfoIter.nextLong();
        final long payloadBuffer = bufferInfoIter.nextLong();

        final WritableLongChunk<Values> chunk = castOrCreateChunk(
                outChunk,
                outOffset,
                Math.max(totalRows, nodeInfo.numElements),
                WritableLongChunk::makeWritableChunk,
                WritableChunk::asWritableLongChunk);

        if (nodeInfo.numElements == 0) {
            return chunk;
        }

        final int numValidityLongs = options.useDeephavenNulls() ? 0 : (nodeInfo.numElements + 63) / 64;
        try (final WritableLongChunk<Values> isValid = WritableLongChunk.makeWritableChunk(numValidityLongs)) {
            readValidityBuffer(is, numValidityLongs, validityBuffer, isValid, DEBUG_NAME);

            final long payloadRead = (long) nodeInfo.numElements * Long.BYTES;
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
            final WritableLongChunk<Values> chunk,
            final int offset) throws IOException {
        final int numElements = nodeInfo.numElements;
        // Read the payload in bounded windows into a reused buffer and decode each value from its little-endian bytes.
        final byte[] buffer = new byte[Math.min(numElements, BULK_READ_ELEMENTS) * Long.BYTES];
        for (int ei = 0; ei < numElements;) {
            final int n = Math.min(BULK_READ_ELEMENTS, numElements - ei);
            is.readFully(buffer, 0, n * Long.BYTES);
            for (int jj = 0; jj < n; ++jj) {
                chunk.set(offset + ei + jj, LittleEndianCodec.getLong(buffer, jj * Long.BYTES));
            }
            ei += n;
        }
    }

    private static void useValidityBuffer(
            final DataInput is,
            final ChunkWriter.FieldNodeInfo nodeInfo,
            final WritableLongChunk<Values> chunk,
            final int offset,
            final WritableLongChunk<Values> isValid) throws IOException {
        final int numElements = nodeInfo.numElements;
        final int numValidityWords = (numElements + 63) / 64;

        // The payload carries a value slot for every element, including nulls; read it in bounded windows into a
        // reused buffer and decode each value, then overwrite the invalid positions with the null value.
        final byte[] buffer = new byte[Math.min(numElements, BULK_READ_ELEMENTS) * Long.BYTES];
        for (int ei = 0; ei < numElements;) {
            final int n = Math.min(BULK_READ_ELEMENTS, numElements - ei);
            is.readFully(buffer, 0, n * Long.BYTES);
            for (int jj = 0; jj < n; ++jj) {
                chunk.set(offset + ei + jj, LittleEndianCodec.getLong(buffer, jj * Long.BYTES));
            }
            ei += n;
        }

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
