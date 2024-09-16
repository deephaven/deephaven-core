//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.util.Float16;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.apache.arrow.flatbuf.Precision;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

public class DoubleChunkReader extends BaseChunkReader<WritableDoubleChunk<Values>> {
    private static final String DEBUG_NAME = "DoubleChunkReader";

    public interface ToDoubleTransformFunction<WireChunkType extends WritableChunk<Values>> {
        double get(WireChunkType wireValues, int wireOffset);
    }

    public static <WireChunkType extends WritableChunk<Values>, T extends ChunkReader<WireChunkType>> ChunkReader<WritableDoubleChunk<Values>> transformTo(
            final T wireReader,
            final ToDoubleTransformFunction<WireChunkType> wireTransform) {
        return new TransformingChunkReader<>(
                wireReader,
                WritableDoubleChunk::makeWritableChunk,
                WritableChunk::asWritableDoubleChunk,
                (wireValues, outChunk, wireOffset, outOffset) -> outChunk.set(
                        outOffset, wireTransform.get(wireValues, wireOffset)));
    }

    private final short precisionFlatbufId;
    private final ChunkReader.Options options;

    public DoubleChunkReader(
            final short precisionFlatbufId,
            final ChunkReader.Options options) {
        this.precisionFlatbufId = precisionFlatbufId;
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
                Math.max(totalRows, nodeInfo.numElements),
                WritableDoubleChunk::makeWritableChunk,
                WritableChunk::asWritableDoubleChunk);

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

            final long payloadRead = (long) nodeInfo.numElements * Double.BYTES;
            Assert.geq(payloadBuffer, "payloadBuffer", payloadRead, "payloadRead");

            if (options.useDeephavenNulls()) {
                useDeephavenNulls(precisionFlatbufId, is, nodeInfo, chunk, outOffset);
            } else {
                useValidityBuffer(precisionFlatbufId, is, nodeInfo, chunk, outOffset, isValid);
            }

            final long overhangPayload = payloadBuffer - payloadRead;
            if (overhangPayload > 0) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, overhangPayload));
            }
        }

        return chunk;
    }

    private static void useDeephavenNulls(
            final short precisionFlatbufId,
            final DataInput is,
            final ChunkWriter.FieldNodeInfo nodeInfo,
            final WritableDoubleChunk<Values> chunk,
            final int offset) throws IOException {
        switch (precisionFlatbufId) {
            case Precision.HALF:
                throw new IllegalStateException("Cannot use Deephaven nulls with half-precision floats");
            case Precision.SINGLE:
                for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                    final float v = is.readFloat();
                    chunk.set(offset + ii, v == QueryConstants.NULL_FLOAT ? QueryConstants.NULL_DOUBLE : v);
                }
                break;
            case Precision.DOUBLE:
                for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                    chunk.set(offset + ii, is.readDouble());
                }
                break;
            default:
                throw new IllegalStateException("Unsupported floating point precision: " + precisionFlatbufId);
        }
    }

    @FunctionalInterface
    private interface DoubleSupplier {
        double next() throws IOException;
    }

    private static double doubleCast(float a) {
        return a == QueryConstants.NULL_FLOAT ? QueryConstants.NULL_DOUBLE : (double) a;
    }

    private static void useValidityBuffer(
            final short precisionFlatbufId,
            final DataInput is,
            final ChunkWriter.FieldNodeInfo nodeInfo,
            final WritableDoubleChunk<Values> chunk,
            final int offset,
            final WritableLongChunk<Values> isValid) throws IOException {
        final int numElements = nodeInfo.numElements;
        final int numValidityWords = (numElements + 63) / 64;

        int ei = 0;
        int pendingSkips = 0;

        final int elementSize;
        final DoubleSupplier supplier;
        switch (precisionFlatbufId) {
            case Precision.HALF:
                elementSize = Short.BYTES;
                supplier = () -> Float16.toFloat(is.readShort());
                break;
            case Precision.SINGLE:
                elementSize = Float.BYTES;
                supplier = () -> doubleCast(is.readFloat());
                break;
            case Precision.DOUBLE:
                elementSize = Double.BYTES;
                supplier = is::readDouble;
                break;
            default:
                throw new IllegalStateException("Unsupported floating point precision: " + precisionFlatbufId);
        }

        for (int vi = 0; vi < numValidityWords; ++vi) {
            int bitsLeftInThisWord = Math.min(64, numElements - vi * 64);
            long validityWord = isValid.get(vi);
            do {
                if ((validityWord & 1) == 1) {
                    if (pendingSkips > 0) {
                        is.skipBytes(pendingSkips * elementSize);
                        chunk.fillWithNullValue(offset + ei, pendingSkips);
                        ei += pendingSkips;
                        pendingSkips = 0;
                    }
                    chunk.set(offset + ei++, supplier.next());
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
            chunk.fillWithNullValue(offset + ei, pendingSkips);
        }
    }
}
