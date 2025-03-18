//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.ChunkPoolConstants;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

public class VarBinaryChunkWriter<T> extends BaseChunkWriter<ObjectChunk<T, Values>> {
    private static final String DEBUG_NAME = "ObjectChunkInputStream Serialization";
    private static final int BYTE_CHUNK_SIZE = ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY;

    public interface Appender<T> {
        void append(OutputStream out, T item) throws IOException;
    }

    private final Appender<T> appendItem;

    public VarBinaryChunkWriter(
            final boolean fieldNullable,
            final Appender<T> appendItem) {
        super(null, ObjectChunk::getEmptyChunk, 0, false, fieldNullable);
        this.appendItem = appendItem;
    }

    @Override
    public DrainableColumn getInputStream(
            @NotNull final ChunkWriter.Context context,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options) throws IOException {
        // noinspection unchecked
        return new ObjectChunkInputStream((Context) context, subset, options);
    }

    @Override
    public Context makeContext(
            @NotNull final ObjectChunk<T, Values> chunk,
            final long rowOffset) {
        return new Context(chunk, rowOffset);
    }

    @Override
    protected int computeNullCount(
            @NotNull final ChunkWriter.Context context,
            @NotNull final RowSequence subset) {
        final MutableInt nullCount = new MutableInt(0);
        final ObjectChunk<Object, Values> objectChunk = context.getChunk().asObjectChunk();
        subset.forAllRowKeys(row -> {
            if (objectChunk.isNull((int) row)) {
                nullCount.increment();
            }
        });
        return nullCount.get();
    }

    @Override
    protected void writeValidityBufferInternal(ChunkWriter.@NotNull Context context, @NotNull RowSequence subset,
            @NotNull SerContext serContext) {
        final ObjectChunk<Object, Values> objectChunk = context.getChunk().asObjectChunk();
        subset.forAllRowKeys(row -> serContext.setNextIsNull(objectChunk.isNull((int) row)));
    }

    public final class Context extends ChunkWriter.Context {
        private final ByteStorage byteStorage;

        public Context(
                @NotNull final ObjectChunk<T, Values> chunk,
                final long rowOffset) {
            super(chunk, rowOffset);

            byteStorage = new ByteStorage(chunk.size() == 0 ? 0 : (chunk.size() + 1));

            if (chunk.size() > 0) {
                byteStorage.offsets.set(0, 0);
            }

            for (int ii = 0; ii < chunk.size(); ++ii) {
                if (!chunk.isNull(ii)) {
                    try {
                        appendItem.append(byteStorage, chunk.get(ii));
                    } catch (final IOException ioe) {
                        throw new UncheckedDeephavenException(
                                "Unexpected exception while draining data to OutputStream: ", ioe);
                    }
                }
                byteStorage.offsets.set(ii + 1, byteStorage.size());
            }
        }

        @Override
        protected void onReferenceCountAtZero() {
            super.onReferenceCountAtZero();
            byteStorage.close();
        }
    }

    private class ObjectChunkInputStream extends BaseChunkInputStream<Context> {

        private int cachedSize = -1;

        private ObjectChunkInputStream(
                @NotNull final Context context,
                @Nullable final RowSet subset,
                @NotNull final BarrageOptions options) throws IOException {
            super(context, subset, options);
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
            listener.noteLogicalBuffer(padBufferSize(numOffsetBytes));

            // payload
            final MutableLong numPayloadBytes = new MutableLong();
            subset.forAllRowKeyRanges((s, e) -> {
                numPayloadBytes.add(context.byteStorage.getPayloadSize((int) s, (int) e));
            });
            listener.noteLogicalBuffer(padBufferSize(numPayloadBytes.get()));
        }

        @Override
        protected int getRawSize() {
            if (cachedSize == -1) {
                MutableLong totalCachedSize = new MutableLong(0L);
                if (sendValidityBuffer()) {
                    totalCachedSize.add(getValidityMapSerializationSizeFor(subset.intSize(DEBUG_NAME)));
                }

                // there are n+1 offsets; it is not assumed first offset is zero
                if (!subset.isEmpty() && subset.size() == context.byteStorage.offsets.size() - 1) {
                    totalCachedSize.add(context.byteStorage.offsets.size() * (long) Integer.BYTES);
                    totalCachedSize.add(context.byteStorage.size());
                } else {
                    totalCachedSize.add(subset.isEmpty() ? 0 : Integer.BYTES); // account for the n+1 offset
                    subset.forAllRowKeyRanges((s, e) -> {
                        // account for offsets
                        totalCachedSize.add((e - s + 1) * Integer.BYTES);

                        // account for payload
                        totalCachedSize.add(context.byteStorage.getPayloadSize((int) s, (int) e));
                    });
                }

                if (!subset.isEmpty() && (subset.size() & 0x1) == 0) {
                    // then we must also align offset array
                    totalCachedSize.add(Integer.BYTES);
                }
                cachedSize = LongSizedDataStructure.intSize(DEBUG_NAME, totalCachedSize.get());
            }
            return cachedSize;
        }

        @Override
        public int drainTo(final OutputStream outputStream) throws IOException {
            if (hasBeenRead) {
                return 0;
            }

            hasBeenRead = true;
            final MutableLong bytesWritten = new MutableLong();
            final LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(outputStream);

            // write the validity buffer
            bytesWritten.add(writeValidityBuffer(dos));

            // write offsets array
            if (!subset.isEmpty()) {
                dos.writeInt(0);
            }

            final MutableInt logicalSize = new MutableInt();
            subset.forAllRowKeys((idx) -> {
                try {
                    logicalSize.add(LongSizedDataStructure.intSize("int cast",
                            context.byteStorage.getPayloadSize((int) idx, (int) idx)));
                    dos.writeInt(logicalSize.get());
                } catch (final IOException e) {
                    throw new UncheckedDeephavenException("couldn't drain data to OutputStream", e);
                }
            });
            long numRows = subset.size();
            if (numRows > 0) {
                numRows += 1;
            }
            bytesWritten.add(Integer.BYTES * numRows);

            if (!subset.isEmpty() && (subset.size() & 0x1) == 0) {
                // then we must pad to align next buffer
                dos.writeInt(0);
                bytesWritten.add(Integer.BYTES);
            }

            subset.forAllRowKeyRanges((s, e) -> {
                try {
                    bytesWritten.add(context.byteStorage.writePayload(dos, (int) s, (int) e));
                } catch (IOException ex) {
                    throw new UncheckedDeephavenException("couldn't drain data to OutputStream", ex);
                }
            });
            bytesWritten.add(writePadBuffer(dos, bytesWritten.get()));
            return LongSizedDataStructure.intSize(DEBUG_NAME, bytesWritten.get());
        }
    }

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
}
