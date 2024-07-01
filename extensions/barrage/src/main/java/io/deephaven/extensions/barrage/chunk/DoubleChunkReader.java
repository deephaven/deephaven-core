//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.function.Function;
import java.util.function.IntFunction;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoubleChunkReader implements ChunkReader {
    private static final String DEBUG_NAME = "DoubleChunkReader";
    private final StreamReaderOptions options;
    private final DoubleConversion conversion;

    @FunctionalInterface
    public interface DoubleConversion {
        double apply(double in);

        DoubleConversion IDENTITY = (double a) -> a;
    }

    public DoubleChunkReader(StreamReaderOptions options) {
        this(options, DoubleConversion.IDENTITY);
    }

    public DoubleChunkReader(StreamReaderOptions options, DoubleConversion conversion) {
        this.options = options;
        this.conversion = conversion;
    }

    public <T> ChunkReader transform(Function<Double, T> transform) {
        return (fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows) -> {
            try (final WritableDoubleChunk<Values> inner = DoubleChunkReader.this.readChunk(
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
                    double value = inner.get(ii);
                    chunk.set(outOffset + ii, transform.apply(value));
                }

                return chunk;
            }
        };
    }

    @Override
    public WritableDoubleChunk<Values> readChunk(Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter,
            PrimitiveIterator.OfLong bufferInfoIter, DataInput is, WritableChunk<Values> outChunk, int outOffset,
            int totalRows) throws IOException {

        final ChunkInputStreamGenerator.FieldNodeInfo nodeInfo = fieldNodeIter.next();
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

            final long payloadRead = (long) nodeInfo.numElements * Double.BYTES;
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

    private static <T extends WritableChunk<Values>> T castOrCreateChunk(
            final WritableChunk<Values> outChunk,
            final int numRows,
            final IntFunction<T> chunkFactory,
            final Function<WritableChunk<Values>, T> castFunction) {
        if (outChunk != null) {
            return castFunction.apply(outChunk);
        }
        final T newChunk = chunkFactory.apply(numRows);
        newChunk.setSize(numRows);
        return newChunk;
    }

    private static void useDeephavenNulls(
            final DoubleConversion conversion,
            final DataInput is,
            final ChunkInputStreamGenerator.FieldNodeInfo nodeInfo,
            final WritableDoubleChunk<Values> chunk,
            final int offset) throws IOException {
        if (conversion == DoubleConversion.IDENTITY) {
            for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                chunk.set(offset + ii, is.readDouble());
            }
        } else {
            for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                final double in = is.readDouble();
                final double out = in == NULL_DOUBLE ? in : conversion.apply(in);
                chunk.set(offset + ii, out);
            }
        }
    }

    private static void useValidityBuffer(
            final DoubleConversion conversion,
            final DataInput is,
            final ChunkInputStreamGenerator.FieldNodeInfo nodeInfo,
            final WritableDoubleChunk<Values> chunk,
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
                        is.skipBytes(pendingSkips * Double.BYTES);
                        chunk.fillWithNullValue(offset + ei, pendingSkips);
                        ei += pendingSkips;
                        pendingSkips = 0;
                    }
                    chunk.set(offset + ei++, conversion.apply(is.readDouble()));
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
