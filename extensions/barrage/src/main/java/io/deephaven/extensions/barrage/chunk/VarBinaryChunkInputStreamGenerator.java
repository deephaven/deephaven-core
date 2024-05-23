//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.ChunkPoolConstants;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.util.pools.PoolableChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.PrimitiveIterator;

public class VarBinaryChunkInputStreamGenerator<T> extends BaseChunkInputStreamGenerator<ObjectChunk<T, Values>> {
    private static final String DEBUG_NAME = "ObjectChunkInputStream Serialization";
    private static final int BYTE_CHUNK_SIZE = ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY;

    private final Appender<T> appendItem;

    public static class ByteStorage extends OutputStream implements SafeCloseable {

        private final WritableLongChunk<ChunkPositions> offsets;
        private final ArrayList<WritableByteChunk<Values>> byteChunks;

        /**
         * The total number of bytes written to this output stream
         */
        private long writtenTotalByteCount = 0L;
        /**
         * The total number of bytes written to the current ByteChunk
         */
        private int activeChunkByteCount = 0;
        /**
         * The ByteChunk to which we are currently writing
         */
        private WritableByteChunk<Values> activeChunk = null;

        public ByteStorage(int size) {
            offsets = WritableLongChunk.makeWritableChunk(size);
            byteChunks = new ArrayList<>();

            // create an initial chunk for data storage. it might not be needed, but eliminates testing on every
            // write operation and the costs for creating and disposing from the pool are minimal
            byteChunks.add(activeChunk = WritableByteChunk.makeWritableChunk(BYTE_CHUNK_SIZE));
        }

        public boolean isEmpty() {
            return writtenTotalByteCount == 0;
        }

        /**
         * Writes the specified byte to the underlying {@code ByteChunk}.
         *
         * @param b the byte to be written.
         */
        public synchronized void write(int b) throws IOException {
            // do the write
            activeChunk.set(activeChunkByteCount++, (byte) b);

            // increment the offset
            writtenTotalByteCount += 1;

            // allocate a new chunk when needed
            if (activeChunkByteCount == BYTE_CHUNK_SIZE) {
                byteChunks.add(activeChunk = WritableByteChunk.makeWritableChunk(BYTE_CHUNK_SIZE));
                activeChunkByteCount = 0;
            }
        }

        /**
         * Writes {@code len} bytes from the specified byte array starting at offset {@code off} to the underlying
         * {@code ByteChunk}.
         *
         * @param b the data.
         * @param off the start offset in the data.
         * @param len the number of bytes to write.
         * @throws IndexOutOfBoundsException if {@code off} is negative, {@code len} is negative, or {@code len} is
         *         greater than {@code b.length - off}
         */
        public synchronized void write(@NotNull byte[] b, int off, int len) throws IOException {
            int remaining = len;
            while (remaining > 0) {
                final int writeLen = Math.min(remaining, BYTE_CHUNK_SIZE - activeChunkByteCount);

                // do the write
                activeChunk.copyFromTypedArray(b, off, activeChunkByteCount, writeLen);

                // increment the counts
                writtenTotalByteCount += writeLen;
                activeChunkByteCount += writeLen;
                off += writeLen;

                remaining -= writeLen;

                // allocate a new chunk when needed
                if (activeChunkByteCount == BYTE_CHUNK_SIZE) {
                    byteChunks.add(activeChunk = WritableByteChunk.makeWritableChunk(BYTE_CHUNK_SIZE));
                    activeChunkByteCount = 0;
                }
            }
        }

        public long size() {
            return writtenTotalByteCount;
        }

        /***
         * computes the size of the payload from sPos to ePos (inclusive)
         *
         * @param sPos the first data item to include in this payload
         * @param ePos the last data item to include in this payload
         * @return number of bytes in the payload
         */
        public long getPayloadSize(int sPos, int ePos) {
            return offsets.get(ePos + 1) - offsets.get(sPos);
        }

        /***
         * write payload from sPos to ePos (inclusive) to the output stream
         *
         * @param dos the data output stream to populate with data
         * @param sPos the first data item to include in this payload
         * @param ePos the last data item to include in this payload
         * @return number of bytes written to the outputstream
         * @throws IOException if there is a problem writing to the output stream
         */
        public long writePayload(LittleEndianDataOutputStream dos, int sPos, int ePos) throws IOException {
            final long writeLen = getPayloadSize(sPos, ePos);
            long remainingBytes = writeLen;

            long startBytePos = offsets.get(sPos);
            while (remainingBytes > 0) {
                final int chunkIdx = (int) (startBytePos / BYTE_CHUNK_SIZE);
                final int byteIdx = (int) (startBytePos % BYTE_CHUNK_SIZE);

                final ByteChunk<?> chunk = byteChunks.get(chunkIdx);

                final int len = (int) Math.min(remainingBytes, BYTE_CHUNK_SIZE - byteIdx);

                // do the write (using the stream adapter utility)
                ByteChunkToOutputStreamAdapter.write(dos, chunk, byteIdx, len);

                // increment the offsets
                startBytePos += len;
                remainingBytes -= len;
            }
            return writeLen;
        }

        @Override
        public void close() {
            try {
                super.close();
            } catch (IOException e) {
                // ignore this error
            }

            // close the offset and byte chunks
            offsets.close();
            for (WritableByteChunk<?> chunk : byteChunks) {
                chunk.close();
            }
        }
    }

    private ByteStorage byteStorage = null;

    public interface Appender<T> {
        void append(OutputStream out, T item) throws IOException;
    }

    public interface Mapper<T> {
        T constructFrom(byte[] buf, int offset, int length) throws IOException;
    }

    VarBinaryChunkInputStreamGenerator(final ObjectChunk<T, Values> chunk,
            final long rowOffset,
            final Appender<T> appendItem) {
        super(chunk, 0, rowOffset);
        this.appendItem = appendItem;
    }

    private synchronized void computePayload() throws IOException {
        if (byteStorage != null) {
            return;
        }
        byteStorage = new ByteStorage(chunk.size() == 0 ? 0 : (chunk.size() + 1));

        if (chunk.size() > 0) {
            byteStorage.offsets.set(0, 0);
        }
        for (int i = 0; i < chunk.size(); ++i) {
            if (chunk.get(i) != null) {
                appendItem.append(byteStorage, chunk.get(i));
            }
            byteStorage.offsets.set(i + 1, byteStorage.size());
        }
    }

    @Override
    public void close() {
        if (REFERENCE_COUNT_UPDATER.decrementAndGet(this) == 0) {
            if (chunk instanceof PoolableChunk) {
                ((PoolableChunk) chunk).close();
            }
            if (byteStorage != null) {
                byteStorage.close();
            }
        }
    }

    @Override
    public DrainableColumn getInputStream(final StreamReaderOptions options, @Nullable final RowSet subset)
            throws IOException {
        computePayload();
        return new ObjectChunkInputStream(options, subset);
    }

    private class ObjectChunkInputStream extends BaseChunkInputStream {

        private int cachedSize = -1;

        private ObjectChunkInputStream(
                final StreamReaderOptions options, final RowSet subset) {
            super(chunk, options, subset);
        }

        private int cachedNullCount = -1;

        @Override
        public int nullCount() {
            if (cachedNullCount == -1) {
                cachedNullCount = 0;
                subset.forAllRowKeys(i -> {
                    if (chunk.get((int) i) == null) {
                        ++cachedNullCount;
                    }
                });
            }
            return cachedNullCount;
        }

        @Override
        public void visitFieldNodes(FieldNodeListener listener) {
            listener.noteLogicalFieldNode(subset.intSize(DEBUG_NAME), nullCount());
        }

        @Override
        public void visitBuffers(final BufferListener listener) {
            // validity
            final int numElements = subset.intSize(DEBUG_NAME);
            listener.noteLogicalBuffer(sendValidityBuffer() ? getValidityMapSerializationSizeFor(numElements) : 0);

            // offsets
            long numOffsetBytes = Integer.BYTES * (((long) numElements) + (numElements > 0 ? 1 : 0));
            final long bytesExtended = numOffsetBytes & REMAINDER_MOD_8_MASK;
            if (bytesExtended > 0) {
                numOffsetBytes += 8 - bytesExtended;
            }
            listener.noteLogicalBuffer(numOffsetBytes);

            // payload
            final MutableLong numPayloadBytes = new MutableLong();
            subset.forAllRowKeyRanges((s, e) -> {
                numPayloadBytes.add(byteStorage.getPayloadSize((int) s, (int) e));
            });
            final long payloadExtended = numPayloadBytes.longValue() & REMAINDER_MOD_8_MASK;
            if (payloadExtended > 0) {
                numPayloadBytes.add(8 - payloadExtended);
            }
            listener.noteLogicalBuffer(numPayloadBytes.longValue());
        }

        @Override
        protected int getRawSize() {
            if (cachedSize == -1) {
                MutableLong totalCachedSize = new MutableLong(0L);
                if (sendValidityBuffer()) {
                    totalCachedSize.add(getValidityMapSerializationSizeFor(subset.intSize(DEBUG_NAME)));
                }

                // there are n+1 offsets; it is not assumed first offset is zero
                if (!subset.isEmpty() && subset.size() == byteStorage.offsets.size() - 1) {
                    totalCachedSize.add(byteStorage.offsets.size() * (long) Integer.BYTES);
                    totalCachedSize.add(byteStorage.size());
                } else {
                    totalCachedSize.add(subset.isEmpty() ? 0 : Integer.BYTES); // account for the n+1 offset
                    subset.forAllRowKeyRanges((s, e) -> {
                        // account for offsets
                        totalCachedSize.add((e - s + 1) * Integer.BYTES);

                        // account for payload
                        totalCachedSize.add(byteStorage.getPayloadSize((int) s, (int) e));
                    });
                }

                if (!subset.isEmpty() && (subset.size() & 0x1) == 0) {
                    // then we must also align offset array
                    totalCachedSize.add(Integer.BYTES);
                }
                cachedSize = LongSizedDataStructure.intSize(DEBUG_NAME, totalCachedSize.longValue());
            }
            return cachedSize;
        }

        @Override
        public int drainTo(final OutputStream outputStream) throws IOException {
            if (read || subset.isEmpty()) {
                return 0;
            }

            read = true;
            long bytesWritten = 0;
            final LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(outputStream);
            // write the validity array with LSB indexing
            if (sendValidityBuffer()) {
                final SerContext context = new SerContext();
                final Runnable flush = () -> {
                    try {
                        dos.writeLong(context.accumulator);
                    } catch (final IOException e) {
                        throw new UncheckedDeephavenException("couldn't drain data to OutputStream", e);
                    }
                    context.accumulator = 0;
                    context.count = 0;
                };
                subset.forAllRowKeys(rawRow -> {
                    final int row = LongSizedDataStructure.intSize(DEBUG_NAME, rawRow);
                    if (chunk.get(row) != null) {
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

            // write offsets array
            dos.writeInt(0);

            final MutableInt logicalSize = new MutableInt();
            subset.forAllRowKeys((idx) -> {
                try {
                    logicalSize.add(LongSizedDataStructure.intSize("int cast",
                            byteStorage.getPayloadSize((int) idx, (int) idx)));
                    dos.writeInt(logicalSize.intValue());
                } catch (final IOException e) {
                    throw new UncheckedDeephavenException("couldn't drain data to OutputStream", e);
                }
            });
            bytesWritten += Integer.BYTES * (subset.size() + 1);

            if ((subset.size() & 0x1) == 0) {
                // then we must pad to align next buffer
                dos.writeInt(0);
                bytesWritten += Integer.BYTES;
            }

            final MutableLong payloadLen = new MutableLong();
            subset.forAllRowKeyRanges((s, e) -> {
                try {
                    payloadLen.add(byteStorage.writePayload(dos, (int) s, (int) e));
                } catch (final IOException err) {
                    throw new UncheckedDeephavenException("couldn't drain data to OutputStream", err);
                }
            });
            bytesWritten += payloadLen.longValue();

            final long bytesExtended = bytesWritten & REMAINDER_MOD_8_MASK;
            if (bytesExtended > 0) {
                bytesWritten += 8 - bytesExtended;
                dos.write(PADDING_BUFFER, 0, (int) (8 - bytesExtended));
            }

            return LongSizedDataStructure.intSize(DEBUG_NAME, bytesWritten);
        }
    }

    static <T> WritableObjectChunk<T, Values> extractChunkFromInputStream(
            final DataInput is,
            final Iterator<FieldNodeInfo> fieldNodeIter,
            final PrimitiveIterator.OfLong bufferInfoIter,
            final Mapper<T> mapper,
            final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {
        final FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final long validityBuffer = bufferInfoIter.nextLong();
        final long offsetsBuffer = bufferInfoIter.nextLong();
        final long payloadBuffer = bufferInfoIter.nextLong();

        final int numElements = nodeInfo.numElements;
        final WritableObjectChunk<T, Values> chunk;
        if (outChunk != null) {
            chunk = outChunk.asWritableObjectChunk();
        } else {
            final int numRows = Math.max(totalRows, numElements);
            chunk = WritableObjectChunk.makeWritableChunk(numRows);
            chunk.setSize(numRows);
        }

        if (numElements == 0) {
            return chunk;
        }

        final int numValidityWords = (numElements + 63) / 64;
        try (final WritableLongChunk<Values> isValid = WritableLongChunk.makeWritableChunk(numValidityWords);
                final WritableIntChunk<Values> offsets = WritableIntChunk.makeWritableChunk(numElements + 1)) {
            // Read validity buffer:
            int jj = 0;
            for (; jj < Math.min(numValidityWords, validityBuffer / 8); ++jj) {
                isValid.set(jj, is.readLong());
            }
            final long valBufRead = jj * 8L;
            if (valBufRead < validityBuffer) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, validityBuffer - valBufRead));
            }
            // we support short validity buffers
            for (; jj < numValidityWords; ++jj) {
                isValid.set(jj, -1); // -1 is bit-wise representation of all ones
            }

            // Read offsets:
            final long offBufRead = (numElements + 1L) * Integer.BYTES;
            if (offsetsBuffer < offBufRead) {
                throw new IllegalStateException("offset buffer is too short for the expected number of elements");
            }
            for (int i = 0; i < numElements + 1; ++i) {
                offsets.set(i, is.readInt());
            }
            if (offBufRead < offsetsBuffer) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, offsetsBuffer - offBufRead));
            }

            // Read data:
            final int bytesRead = LongSizedDataStructure.intSize(DEBUG_NAME, payloadBuffer);
            final byte[] serializedData = new byte[bytesRead];
            is.readFully(serializedData);

            // Deserialize:
            int ei = 0;
            int pendingSkips = 0;

            for (int vi = 0; vi < numValidityWords; ++vi) {
                int bitsLeftInThisWord = Math.min(64, numElements - vi * 64);
                long validityWord = isValid.get(vi);
                do {
                    if ((validityWord & 1) == 1) {
                        if (pendingSkips > 0) {
                            chunk.fillWithNullValue(outOffset + ei, pendingSkips);
                            ei += pendingSkips;
                            pendingSkips = 0;
                        }
                        final int offset = offsets.get(ei);
                        final int length = offsets.get(ei + 1) - offset;
                        Assert.geq(length, "length", 0);
                        if (offset + length > serializedData.length) {
                            throw new IllegalStateException("not enough data was serialized to parse this element: " +
                                    "elementIndex=" + ei + " offset=" + offset + " length=" + length +
                                    " serializedLen=" + serializedData.length);
                        }
                        chunk.set(outOffset + ei++, mapper.constructFrom(serializedData, offset, length));
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
                chunk.fillWithNullValue(outOffset + ei, pendingSkips);
            }
        }

        return chunk;
    }
}
