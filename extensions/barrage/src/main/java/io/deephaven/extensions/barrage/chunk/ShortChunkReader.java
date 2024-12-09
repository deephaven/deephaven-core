//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkReader and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableShortChunk;
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

import static io.deephaven.util.QueryConstants.NULL_SHORT;

public class ShortChunkReader extends BaseChunkReader<WritableShortChunk<Values>> {
    private static final String DEBUG_NAME = "ShortChunkReader";

    @FunctionalInterface
    public interface ToShortTransformFunction<WIRE_CHUNK_TYPE extends WritableChunk<Values>> {
        short get(WIRE_CHUNK_TYPE wireValues, int wireOffset);
    }

    public static <WIRE_CHUNK_TYPE extends WritableChunk<Values>, T extends ChunkReader<WIRE_CHUNK_TYPE>> ChunkReader<WritableShortChunk<Values>> transformTo(
            final T wireReader,
            final ToShortTransformFunction<WIRE_CHUNK_TYPE> wireTransform) {
        return new TransformingChunkReader<>(
                wireReader,
                WritableShortChunk::makeWritableChunk,
                WritableChunk::asWritableShortChunk,
                (wireValues, outChunk, wireOffset, outOffset) -> outChunk.set(
                        outOffset, wireTransform.get(wireValues, wireOffset)));
    }

    private final BarrageOptions options;
    private final ShortConversion conversion;

    @FunctionalInterface
    public interface ShortConversion {
        short apply(short in);

        ShortConversion IDENTITY = (short a) -> a;
    }

    public ShortChunkReader(BarrageOptions options) {
        this(options, ShortConversion.IDENTITY);
    }

    public ShortChunkReader(BarrageOptions options, ShortConversion conversion) {
        this.options = options;
        this.conversion = conversion;
    }

    public <T> ChunkReader<WritableObjectChunk<T, Values>> transform(Function<Short, T> transform) {
        return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows) -> {
            try (final WritableShortChunk<Values> inner = ShortChunkReader.this.readChunk(
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
                    short value = inner.get(ii);
                    chunk.set(outOffset + ii, transform.apply(value));
                }

                return chunk;
            }
        };
    }

    @Override
    public WritableShortChunk<Values> readChunk(
            @NotNull final Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
            @NotNull final PrimitiveIterator.OfLong bufferInfoIter,
            @NotNull final DataInput is,
            @Nullable final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {

        final ChunkWriter.FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final long validityBuffer = bufferInfoIter.nextLong();
        final long payloadBuffer = bufferInfoIter.nextLong();

        final WritableShortChunk<Values> chunk = castOrCreateChunk(
                outChunk,
                Math.max(totalRows, nodeInfo.numElements),
                WritableShortChunk::makeWritableChunk,
                WritableChunk::asWritableShortChunk);

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

            final long payloadRead = (long) nodeInfo.numElements * Short.BYTES;
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
            final ShortConversion conversion,
            final DataInput is,
            final ChunkWriter.FieldNodeInfo nodeInfo,
            final WritableShortChunk<Values> chunk,
            final int offset) throws IOException {
        if (conversion == ShortConversion.IDENTITY) {
            for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                chunk.set(offset + ii, is.readShort());
            }
        } else {
            for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                final short in = is.readShort();
                final short out = in == NULL_SHORT ? in : conversion.apply(in);
                chunk.set(offset + ii, out);
            }
        }
    }

    private static void useValidityBuffer(
            final ShortConversion conversion,
            final DataInput is,
            final ChunkWriter.FieldNodeInfo nodeInfo,
            final WritableShortChunk<Values> chunk,
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
                        is.skipBytes(pendingSkips * Short.BYTES);
                        chunk.fillWithNullValue(offset + ei, pendingSkips);
                        ei += pendingSkips;
                        pendingSkips = 0;
                    }
                    chunk.set(offset + ei++, conversion.apply(is.readShort()));
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
            is.skipBytes(pendingSkips * Short.BYTES);
            chunk.fillWithNullValue(offset + ei, pendingSkips);
        }
    }
}
