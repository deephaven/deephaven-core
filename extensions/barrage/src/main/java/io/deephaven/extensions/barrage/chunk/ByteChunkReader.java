//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
import java.util.function.Function;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

public class ByteChunkReader extends BaseChunkReader<WritableByteChunk<Values>> {
    private static final String DEBUG_NAME = "ByteChunkReader";

    @FunctionalInterface
    public interface ToByteTransformFunction<WIRE_CHUNK_TYPE extends WritableChunk<Values>> {
        byte get(WIRE_CHUNK_TYPE wireValues, int wireOffset);
    }

    public static <WIRE_CHUNK_TYPE extends WritableChunk<Values>, T extends ChunkReader<WIRE_CHUNK_TYPE>> ChunkReader<WritableByteChunk<Values>> transformTo(
            final T wireReader,
            final ToByteTransformFunction<WIRE_CHUNK_TYPE> wireTransform) {
        return new TransformingChunkReader<>(
                wireReader,
                WritableByteChunk::makeWritableChunk,
                WritableChunk::asWritableByteChunk,
                (wireValues, outChunk, wireOffset, outOffset) -> outChunk.set(
                        outOffset, wireTransform.get(wireValues, wireOffset)));
    }

    private final BarrageOptions options;
    private final ByteConversion conversion;

    @FunctionalInterface
    public interface ByteConversion {
        byte apply(byte in);

        ByteConversion IDENTITY = (byte a) -> a;
    }

    public ByteChunkReader(BarrageOptions options) {
        this(options, ByteConversion.IDENTITY);
    }

    public ByteChunkReader(BarrageOptions options, ByteConversion conversion) {
        this.options = options;
        this.conversion = conversion;
    }

    public <T> ChunkReader<WritableObjectChunk<T, Values>> transform(Function<Byte, T> transform) {
        return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows) -> {
            try (final WritableByteChunk<Values> inner = ByteChunkReader.this.readChunk(
                    fieldNodeIter, bufferInfoIter, is, null, 0, 0)) {

                final WritableObjectChunk<T, Values> chunk = castOrCreateChunk(
                        outChunk,
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
                Math.max(totalRows, nodeInfo.numElements),
                WritableByteChunk::makeWritableChunk,
                WritableChunk::asWritableByteChunk);

        if (nodeInfo.numElements == 0) {
            return chunk;
        }

        final int numValidityLongs = options.useDeephavenNulls() ? 0 : (nodeInfo.numElements + 63) / 64;
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

            final long payloadRead = (long) nodeInfo.numElements * Byte.BYTES;
            Assert.geq(payloadBuffer, "payloadBuffer", payloadRead, "payloadRead");

            if (options.useDeephavenNulls()) {
                useDeephavenNulls(conversion, is, nodeInfo, chunk, outOffset);
            } else {
                useValidityBuffer(conversion, is, nodeInfo, chunk, outOffset, isValid);
            }

            final long overhangPayload = payloadBuffer - payloadRead;
            if (overhangPayload > 0) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, overhangPayload));
            }
        }

        return chunk;
    }

    private static void useDeephavenNulls(
            final ByteConversion conversion,
            final DataInput is,
            final ChunkWriter.FieldNodeInfo nodeInfo,
            final WritableByteChunk<Values> chunk,
            final int offset) throws IOException {
        if (conversion == ByteConversion.IDENTITY) {
            for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                chunk.set(offset + ii, is.readByte());
            }
        } else {
            for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                final byte in = is.readByte();
                final byte out = in == NULL_BYTE ? in : conversion.apply(in);
                chunk.set(offset + ii, out);
            }
        }
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
            do {
                if ((validityWord & 1) == 1) {
                    if (pendingSkips > 0) {
                        is.skipBytes(pendingSkips * Byte.BYTES);
                        chunk.fillWithNullValue(offset + ei, pendingSkips);
                        ei += pendingSkips;
                        pendingSkips = 0;
                    }
                    chunk.set(offset + ei++, conversion.apply(is.readByte()));
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
            is.skipBytes(pendingSkips * Byte.BYTES);
            chunk.fillWithNullValue(offset + ei, pendingSkips);
        }
    }
}
