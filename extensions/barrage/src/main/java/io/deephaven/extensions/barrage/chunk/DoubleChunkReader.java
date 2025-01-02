//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatChunkReader and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.BarrageOptions;
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

    public interface ToDoubleTransformFunction<WIRE_CHUNK_TYPE extends WritableChunk<Values>> {
        double get(WIRE_CHUNK_TYPE wireValues, int wireOffset);
    }

    public static <WIRE_CHUNK_TYPE extends WritableChunk<Values>, T extends ChunkReader<WIRE_CHUNK_TYPE>> ChunkReader<WritableDoubleChunk<Values>> transformTo(
            final T wireReader,
            final ToDoubleTransformFunction<WIRE_CHUNK_TYPE> wireTransform) {
        return new TransformingChunkReader<>(
                wireReader,
                WritableDoubleChunk::makeWritableChunk,
                WritableChunk::asWritableDoubleChunk,
                (wireValues, outChunk, wireOffset, outOffset) -> outChunk.set(
                        outOffset, wireTransform.get(wireValues, wireOffset)));
    }

    private final short precisionFlatBufId;
    private final BarrageOptions options;

    public DoubleChunkReader(
            final short precisionFlatbufId,
            final BarrageOptions options) {
        this.precisionFlatBufId = precisionFlatbufId;
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
            readValidityBuffer(is, numValidityLongs, validityBuffer, isValid, DEBUG_NAME);

            final long payloadRead = (long) nodeInfo.numElements * Double.BYTES;
            Assert.geq(payloadBuffer, "payloadBuffer", payloadRead, "payloadRead");

            if (options.useDeephavenNulls()) {
                useDeephavenNulls(precisionFlatBufId, is, nodeInfo, chunk, outOffset);
            } else {
                useValidityBuffer(precisionFlatBufId, is, nodeInfo, chunk, outOffset, isValid);
            }

            final long overhangPayload = payloadBuffer - payloadRead;
            if (overhangPayload > 0) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, overhangPayload));
            }
        }

        return chunk;
    }

    private static void useDeephavenNulls(
            final short precisionFlatBufId,
            final DataInput is,
            final ChunkWriter.FieldNodeInfo nodeInfo,
            final WritableDoubleChunk<Values> chunk,
            final int offset) throws IOException {
        switch (precisionFlatBufId) {
            case Precision.HALF:
                throw new IllegalStateException("Cannot use Deephaven nulls with half-precision floats");
            case Precision.SINGLE:
                for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                    // region PrecisionSingleDhNulls
                    final float v = is.readFloat();
                    chunk.set(offset + ii, doubleCast(v));
                    // endregion PrecisionSingleDhNulls
                }
                break;
            case Precision.DOUBLE:
                for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                    // region PrecisionDoubleDhNulls
                    chunk.set(offset + ii, is.readDouble());
                    // endregion PrecisionDoubleDhNulls
                }
                break;
            default:
                throw new IllegalStateException("Unsupported floating point precision: " + precisionFlatBufId);
        }
    }

    @FunctionalInterface
    private interface DoubleSupplier {
        double next() throws IOException;
    }

    // region FPCastHelper
    private static double doubleCast(float a) {
        return a == QueryConstants.NULL_FLOAT ? QueryConstants.NULL_DOUBLE : (double) a;
    }
    // endregion FPCastHelper

    private static void useValidityBuffer(
            final short precisionFlatBufId,
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
        switch (precisionFlatBufId) {
            case Precision.HALF:
                elementSize = Short.BYTES;
                supplier = () -> Float16.toFloat(is.readShort());
                break;
            case Precision.SINGLE:
                // region PrecisionSingleValidityBuffer
                elementSize = Float.BYTES;
                supplier = () -> doubleCast(is.readFloat());
                // endregion PrecisionSingleValidityBuffer
                break;
            case Precision.DOUBLE:
                elementSize = Double.BYTES;
                // region PrecisionDoubleValidityBuffer
                supplier = is::readDouble;
                // endregion PrecisionDoubleValidityBuffer
                break;
            default:
                throw new IllegalStateException("Unsupported floating point precision: " + precisionFlatBufId);
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
