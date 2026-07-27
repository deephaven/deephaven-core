//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkReader and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.extensions.barrage.util.BarrageProtoUtil;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.PrimitiveIterator;

public class DoubleChunkReader extends BaseChunkReader<WritableDoubleChunk<Values>> {
    private static final String DEBUG_NAME = "DoubleChunkReader";

    // Reads a little-endian long from a byte[] at a byte offset in a single (possibly unaligned) load.
    private static final VarHandle LITTLE_ENDIAN_LONG =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    // Number of elements decoded per bounded bulk-read window (see BaseChunkReader#BULK_READ_BUFFER_BYTES).
    private static final int BULK_READ_ELEMENTS = Math.max(1, BULK_READ_BUFFER_BYTES / Double.BYTES);

    public static <WIRE_CHUNK_TYPE extends WritableChunk<Values>, T extends ChunkReader<WIRE_CHUNK_TYPE>> ChunkReader<WritableDoubleChunk<Values>> transformFrom(
            final T wireReader,
            final ChunkTransformer<WIRE_CHUNK_TYPE, WritableDoubleChunk<Values>> wireTransform) {
        return new TransformingChunkReader<>(
                wireReader,
                WritableDoubleChunk::makeWritableChunk,
                WritableChunk::asWritableDoubleChunk,
                wireTransform);
    }

    private final BarrageOptions options;

    public DoubleChunkReader(BarrageOptions options) {
        this.options = options;
    }

    @Override
    public WritableDoubleChunk<Values> readChunk(
            @NotNull final Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
            @NotNull final PrimitiveIterator.OfLong bufferInfoIter,
            @NotNull final DataInput is,
            @Nullable final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {

        final ChunkWriter.FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final long validityBuffer = bufferInfoIter.nextLong();
        final long payloadBuffer = bufferInfoIter.nextLong();

        final WritableDoubleChunk<Values> chunk = castOrCreateChunk(
                outChunk,
                outOffset,
                Math.max(totalRows, nodeInfo.numElements),
                WritableDoubleChunk::makeWritableChunk,
                WritableChunk::asWritableDoubleChunk);

        if (nodeInfo.numElements == 0) {
            return chunk;
        }

        final int numValidityLongs = options.useDeephavenNulls() ? 0 : (nodeInfo.numElements + 63) / 64;
        try (final WritableLongChunk<Values> isValid = WritableLongChunk.makeWritableChunk(numValidityLongs)) {
            readValidityBuffer(is, numValidityLongs, validityBuffer, isValid, DEBUG_NAME);

            final long payloadRead = (long) nodeInfo.numElements * Double.BYTES;
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
            final WritableDoubleChunk<Values> chunk,
            final int offset) throws IOException {
        final int numElements = nodeInfo.numElements;
        if (is instanceof BarrageProtoUtil.BarrageDataInputStream) {
            // Read the payload in bounded windows, decoding each double from the little-endian bytes as we go, so a
            // wide column never materializes an entire byte buffer at once.
            final BarrageProtoUtil.BarrageDataInputStream lis = (BarrageProtoUtil.BarrageDataInputStream) is;
            for (int ei = 0; ei < numElements;) {
                final int n = Math.min(BULK_READ_ELEMENTS, numElements - ei);
                final byte[] payload = lis.readRawBytes(n * Double.BYTES);
                for (int jj = 0; jj < n; ++jj) {
                    chunk.set(offset + ei + jj,
                            Double.longBitsToDouble((long) LITTLE_ENDIAN_LONG.get(payload, jj * Double.BYTES)));
                }
                ei += n;
            }
        } else {
            for (int ii = 0; ii < numElements; ++ii) {
                chunk.set(offset + ii, is.readDouble());
            }
        }
    }

    private static void useValidityBuffer(
            final DataInput is,
            final ChunkWriter.FieldNodeInfo nodeInfo,
            final WritableDoubleChunk<Values> chunk,
            final int offset,
            final WritableLongChunk<Values> isValid) throws IOException {
        final int numElements = nodeInfo.numElements;
        final int numValidityWords = (numElements + 63) / 64;

        if (is instanceof BarrageProtoUtil.BarrageDataInputStream) {
            // Walk the validity buffer and the payload together in a single pass, decoding only the valid slots from
            // each bounded window of little-endian bytes and filling runs of nulls in place. Null slots still occupy a
            // slot in the payload, so the window position advances past them, but they are never decoded.
            final BarrageProtoUtil.BarrageDataInputStream lis = (BarrageProtoUtil.BarrageDataInputStream) is;
            for (int ei = 0; ei < numElements;) {
                final int n = Math.min(BULK_READ_ELEMENTS, numElements - ei);
                final byte[] payload = lis.readRawBytes(n * Double.BYTES);
                for (int jj = 0; jj < n; ++jj) {
                    chunk.set(offset + ei + jj,
                            Double.longBitsToDouble((long) LITTLE_ENDIAN_LONG.get(payload, jj * Double.BYTES)));
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
            return;
        }

        int ei = 0;
        int pendingSkips = 0;

        for (int vi = 0; vi < numValidityWords; ++vi) {
            int bitsLeftInThisWord = Math.min(64, numElements - vi * 64);
            long validityWord = isValid.get(vi);
            do {
                if ((validityWord & 1) == 1) {
                    if (pendingSkips > 0) {
                        is.skipBytes(pendingSkips * Double.BYTES);
                        chunk.fillWithNullValue(offset + ei, pendingSkips);
                        ei += pendingSkips;
                        pendingSkips = 0;
                    }
                    chunk.set(offset + ei++, is.readDouble());
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
            is.skipBytes(pendingSkips * Double.BYTES);
            chunk.fillWithNullValue(offset + ei, pendingSkips);
        }
    }
}
