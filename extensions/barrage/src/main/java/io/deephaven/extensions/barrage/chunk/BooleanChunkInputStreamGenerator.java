//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.PoolableChunk;
import io.deephaven.engine.rowset.RowSet;
import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableLongChunk;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.PrimitiveIterator;

import static io.deephaven.util.QueryConstants.*;

public class BooleanChunkInputStreamGenerator extends BaseChunkInputStreamGenerator<ByteChunk<Values>> {
    private static final String DEBUG_NAME = "BooleanChunkInputStreamGenerator";

    public static BooleanChunkInputStreamGenerator convertBoxed(
            final ObjectChunk<Boolean, Values> inChunk, final long rowOffset) {
        // This code path is utilized for arrays / vectors, which cannot be reinterpreted.
        WritableByteChunk<Values> outChunk = WritableByteChunk.makeWritableChunk(inChunk.size());
        for (int i = 0; i < inChunk.size(); ++i) {
            final Boolean value = inChunk.get(i);
            outChunk.set(i, BooleanUtils.booleanAsByte(value));
        }
        if (inChunk instanceof PoolableChunk) {
            ((PoolableChunk) inChunk).close();
        }
        return new BooleanChunkInputStreamGenerator(outChunk, rowOffset);
    }

    BooleanChunkInputStreamGenerator(final ByteChunk<Values> chunk, final long rowOffset) {
        // note: element size is zero here to indicate that we cannot use the element size as it is in bytes per row
        super(chunk, 0, rowOffset);
    }

    @Override
    public DrainableColumn getInputStream(final StreamReaderOptions options, @Nullable final RowSet subset) {
        return new BooleanChunkInputStream(options, subset);
    }

    private class BooleanChunkInputStream extends BaseChunkInputStream {
        private BooleanChunkInputStream(final StreamReaderOptions options, final RowSet subset) {
            super(chunk, options, subset);
        }

        private int cachedNullCount = -1;

        @Override
        public int nullCount() {
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
        protected int getRawSize() {
            long size = 0;
            if (sendValidityBuffer()) {
                size += getValidityMapSerializationSizeFor(subset.intSize(DEBUG_NAME));
            }
            size += getNumLongsForBitPackOfSize(subset.intSize(DEBUG_NAME)) * (long) Long.BYTES;
            return LongSizedDataStructure.intSize(DEBUG_NAME, size);
        }

        @Override
        public void visitFieldNodes(final FieldNodeListener listener) {
            listener.noteLogicalFieldNode(subset.intSize(DEBUG_NAME), nullCount());
        }

        @Override
        public void visitBuffers(final BufferListener listener) {
            // validity
            int validityLen = sendValidityBuffer() ? getValidityMapSerializationSizeFor(subset.intSize(DEBUG_NAME)) : 0;
            listener.noteLogicalBuffer(validityLen);
            // payload
            listener.noteLogicalBuffer(getNumLongsForBitPackOfSize(subset.intSize(DEBUG_NAME)) * (long) Long.BYTES);
        }

        @Override
        @SuppressWarnings("UnstableApiUsage")
        public int drainTo(final OutputStream outputStream) throws IOException {
            if (read || subset.isEmpty()) {
                return 0;
            }

            long bytesWritten = 0;
            read = true;
            final LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(outputStream);
            // write the validity array with LSB indexing
            final SerContext context = new SerContext();
            final Runnable flush = () -> {
                try {
                    dos.writeLong(context.accumulator);
                } catch (final IOException e) {
                    throw new UncheckedDeephavenException("Unexpected exception while draining data to OutputStream: ",
                            e);
                }
                context.accumulator = 0;
                context.count = 0;
            };

            if (sendValidityBuffer()) {
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
                bytesWritten += getValidityMapSerializationSizeFor(subset.intSize(DEBUG_NAME));
            }

            // write the included values
            subset.forAllRowKeys(row -> {
                final byte byteValue = chunk.get((int) row);
                if (byteValue != NULL_BYTE) {
                    context.accumulator |= (byteValue > 0 ? 1L : 0L) << context.count;
                }
                if (++context.count == 64) {
                    flush.run();
                }
            });
            if (context.count > 0) {
                flush.run();
            }
            bytesWritten += getNumLongsForBitPackOfSize(subset.intSize(DEBUG_NAME)) * (long) Long.BYTES;

            return LongSizedDataStructure.intSize(DEBUG_NAME, bytesWritten);
        }
    }

    @FunctionalInterface
    public interface ByteConversion {
        byte apply(byte in);

        ByteConversion IDENTITY = (byte a) -> a;
    }

    static WritableChunk<Values> extractChunkFromInputStream(
            final StreamReaderOptions options,
            final Iterator<FieldNodeInfo> fieldNodeIter,
            final PrimitiveIterator.OfLong bufferInfoIter,
            final DataInput is,
            final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {
        return extractChunkFromInputStreamWithConversion(
                options, ByteConversion.IDENTITY, fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
    }

    static WritableChunk<Values> extractChunkFromInputStreamWithConversion(
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

        final WritableByteChunk<Values> chunk;
        if (outChunk != null) {
            chunk = outChunk.asWritableByteChunk();
        } else {
            final int numRows = Math.max(totalRows, nodeInfo.numElements);
            chunk = WritableByteChunk.makeWritableChunk(numRows);
            chunk.setSize(numRows);
        }

        if (nodeInfo.numElements == 0) {
            return chunk;
        }

        final int numValidityLongs = (nodeInfo.numElements + 63) / 64;
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
