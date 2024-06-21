//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkInputStreamGenerator and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.PoolableChunk;
import io.deephaven.engine.primitive.function.ToByteFunction;
import io.deephaven.engine.rowset.RowSet;
import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.function.Function;
import java.util.function.IntFunction;

import static io.deephaven.util.QueryConstants.*;

public class ByteChunkInputStreamGenerator extends BaseChunkInputStreamGenerator<ByteChunk<Values>> {
    private static final String DEBUG_NAME = "ByteChunkInputStreamGenerator";

    public static ByteChunkInputStreamGenerator convertBoxed(
            final ObjectChunk<Byte, Values> inChunk, final long rowOffset) {
        return convertWithTransform(inChunk, rowOffset, TypeUtils::unbox);
    }

    public static <T> ByteChunkInputStreamGenerator convertWithTransform(
            final ObjectChunk<T, Values> inChunk, final long rowOffset, final ToByteFunction<T> transform) {
        // This code path is utilized for arrays and vectors of DateTimes, LocalDate, and LocalTime, which cannot be
        // reinterpreted.
        WritableByteChunk<Values> outChunk = WritableByteChunk.makeWritableChunk(inChunk.size());
        for (int i = 0; i < inChunk.size(); ++i) {
            T value = inChunk.get(i);
            outChunk.set(i, transform.applyAsByte(value));
        }
        // inChunk is a transfer of ownership to us, but we've converted what we need, so we must close it now
        if (inChunk instanceof PoolableChunk) {
            ((PoolableChunk) inChunk).close();
        }
        return new ByteChunkInputStreamGenerator(outChunk, Byte.BYTES, rowOffset);
    }

    ByteChunkInputStreamGenerator(final ByteChunk<Values> chunk, final int elementSize, final long rowOffset) {
        super(chunk, elementSize, rowOffset);
    }

    @Override
    public DrainableColumn getInputStream(final StreamReaderOptions options, @Nullable final RowSet subset) {
        return new ByteChunkInputStream(options, subset);
    }

    private class ByteChunkInputStream extends BaseChunkInputStream {
        private ByteChunkInputStream(final StreamReaderOptions options, final RowSet subset) {
            super(chunk, options, subset);
        }

        private int cachedNullCount = -1;

        @Override
        public int nullCount() {
            if (options.useDeephavenNulls()) {
                return 0;
            }
            if (cachedNullCount == -1) {
                cachedNullCount = 0;
                subset.forAllRowKeys(row -> {
                    if (chunk.get((int) row) == NULL_BYTE) {
                        ++cachedNullCount;
                    }
                });
            }
            return cachedNullCount;
        }

        @Override
        public void visitFieldNodes(final FieldNodeListener listener) {
            listener.noteLogicalFieldNode(subset.intSize(DEBUG_NAME), nullCount());
        }

        @Override
        public void visitBuffers(final BufferListener listener) {
            // validity
            listener.noteLogicalBuffer(sendValidityBuffer() ? getValidityMapSerializationSizeFor(subset.intSize()) : 0);
            // payload
            long length = elementSize * subset.size();
            final long bytesExtended = length & REMAINDER_MOD_8_MASK;
            if (bytesExtended > 0) {
                length += 8 - bytesExtended;
            }
            listener.noteLogicalBuffer(length);
        }

        @Override
        public int drainTo(final OutputStream outputStream) throws IOException {
            if (read || subset.isEmpty()) {
                return 0;
            }

            long bytesWritten = 0;
            read = true;
            final LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(outputStream);
            // write the validity array with LSB indexing
            if (sendValidityBuffer()) {
                final SerContext context = new SerContext();
                final Runnable flush = () -> {
                    try {
                        dos.writeLong(context.accumulator);
                    } catch (final IOException e) {
                        throw new UncheckedDeephavenException(
                                "Unexpected exception while draining data to OutputStream: ", e);
                    }
                    context.accumulator = 0;
                    context.count = 0;
                };
                subset.forAllRowKeys(row -> {
                    if (chunk.get((int) row) != NULL_BYTE) {
                        context.accumulator |= 1L << context.count;
                    }
                    if (++context.count == 64) {
                        flush.run();
                    }
                });
                if (context.count > 0) {
                    flush.run();
                }

                bytesWritten += getValidityMapSerializationSizeFor(subset.intSize());
            }

            // write the included values
            subset.forAllRowKeys(row -> {
                try {
                    final byte val = chunk.get((int) row);
                    dos.writeByte(val);
                } catch (final IOException e) {
                    throw new UncheckedDeephavenException("Unexpected exception while draining data to OutputStream: ",
                            e);
                }
            });

            bytesWritten += elementSize * subset.size();
            final long bytesExtended = bytesWritten & REMAINDER_MOD_8_MASK;
            if (bytesExtended > 0) {
                bytesWritten += 8 - bytesExtended;
                dos.write(PADDING_BUFFER, 0, (int) (8 - bytesExtended));
            }

            return LongSizedDataStructure.intSize("ByteChunkInputStreamGenerator", bytesWritten);
        }
    }

    @FunctionalInterface
    public interface ByteConversion {
        byte apply(byte in);

        ByteConversion IDENTITY = (byte a) -> a;
    }

    static WritableByteChunk<Values> extractChunkFromInputStream(
            final int elementSize,
            final StreamReaderOptions options,
            final Iterator<FieldNodeInfo> fieldNodeIter,
            final PrimitiveIterator.OfLong bufferInfoIter,
            final DataInput is,
            final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {
        return extractChunkFromInputStreamWithConversion(
                elementSize, options, ByteConversion.IDENTITY, fieldNodeIter, bufferInfoIter, is, outChunk, outOffset,
                totalRows);
    }

    static <T> WritableObjectChunk<T, Values> extractChunkFromInputStreamWithTransform(
            final int elementSize,
            final StreamReaderOptions options,
            final Function<Byte, T> transform,
            final Iterator<FieldNodeInfo> fieldNodeIter,
            final PrimitiveIterator.OfLong bufferInfoIter,
            final DataInput is,
            final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {

        try (final WritableByteChunk<Values> inner = extractChunkFromInputStream(
                elementSize, options, fieldNodeIter, bufferInfoIter, is, null, 0, 0)) {

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
    }

    static WritableByteChunk<Values> extractChunkFromInputStreamWithConversion(
            final int elementSize,
            final StreamReaderOptions options,
            final ByteConversion conversion,
            final Iterator<FieldNodeInfo> fieldNodeIter,
            final PrimitiveIterator.OfLong bufferInfoIter,
            final DataInput is,
            final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {

        final FieldNodeInfo nodeInfo = fieldNodeIter.next();
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

            final long payloadRead = (long) nodeInfo.numElements * elementSize;
            if (payloadBuffer < payloadRead) {
                throw new IllegalStateException("payload buffer is too short for expected number of elements");
            }

            if (options.useDeephavenNulls()) {
                useDeephavenNulls(conversion, is, nodeInfo, chunk, outOffset);
            } else {
                useValidityBuffer(elementSize, conversion, is, nodeInfo, chunk, outOffset, isValid);
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
            final ByteConversion conversion,
            final DataInput is,
            final FieldNodeInfo nodeInfo,
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
            final int elementSize,
            final ByteConversion conversion,
            final DataInput is,
            final FieldNodeInfo nodeInfo,
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
                        is.skipBytes(pendingSkips * elementSize);
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
            is.skipBytes(pendingSkips * elementSize);
            chunk.fillWithNullValue(offset + ei, pendingSkips);
        }
    }
}
