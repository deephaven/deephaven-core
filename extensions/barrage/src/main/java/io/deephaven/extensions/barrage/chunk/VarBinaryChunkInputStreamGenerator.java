/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.extensions.barrage.chunk;

import com.google.common.io.LittleEndianDataOutputStream;
import gnu.trove.iterator.TLongIterator;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.util.pools.PoolableChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.util.BarrageProtoUtil;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

public class VarBinaryChunkInputStreamGenerator<T> extends BaseChunkInputStreamGenerator<ObjectChunk<T, Values>> {
    private static final String DEBUG_NAME = "ObjectChunkInputStream Serialization";

    private final Class<T> type;
    private final Appender<T> appendItem;

    private byte[] bytes;
    private WritableIntChunk<ChunkPositions> offsets;

    public interface Appender<T> {
        void append(OutputStream out, T item) throws IOException;
    }

    public interface Mapper<T> {
        T constructFrom(byte[] buf, int offset, int length) throws IOException;
    }

    VarBinaryChunkInputStreamGenerator(final Class<T> type, final ObjectChunk<T, Values> chunk,
                                       final Appender<T> appendItem) {
        super(chunk, 0);
        this.type = type;
        this.appendItem = appendItem;
    }

    private synchronized void computePayload() throws IOException {
        if (bytes != null) {
            return;
        }

        offsets = WritableIntChunk.makeWritableChunk(chunk.size() == 0 ? 0 : (chunk.size() + 1));

        try (final BarrageProtoUtil.ExposedByteArrayOutputStream baos = new BarrageProtoUtil.ExposedByteArrayOutputStream()) {
            if (chunk.size() > 0) {
                offsets.set(0, 0);
            }
            for (int i = 0; i < chunk.size(); ++i) {
                if (chunk.get(i) != null) {
                    appendItem.append(baos, chunk.get(i));
                }
                offsets.set(i + 1, baos.size());
            }
            bytes = baos.peekBuffer();
        }
    }

    @Override
    public void close() {
        if (REFERENCE_COUNT_UPDATER.decrementAndGet(this) == 0) {
            if (chunk instanceof PoolableChunk) {
                ((PoolableChunk) chunk).close();
            }
            if (offsets != null) {
                offsets.close();
            }
        }
    }

    @Override
    public DrainableColumn getInputStream(final StreamReaderOptions options, final @Nullable RowSet subset) throws IOException {
        computePayload();
        return new ObjectChunkInputStream(options, offsets, bytes, subset);
    }

    private class ObjectChunkInputStream extends BaseChunkInputStream {
        private int cachedSize = -1;
        private final byte[] myBytes;
        private final IntChunk<ChunkPositions> myOffsets;

        private ObjectChunkInputStream(
                final StreamReaderOptions options,
                final IntChunk<ChunkPositions> myOffsets,
                final byte[] myBytes, final RowSet subset) {
            super(chunk, options, subset);
            this.myBytes = myBytes;
            this.myOffsets = myOffsets;
        }

        private int cachedNullCount = -1;

        @Override
        public int nullCount() {
            if (cachedNullCount == -1) {
                cachedNullCount = 0;
                subset.forAllRowKeys(i -> {
                    if (chunk.get((int)i) == null) {
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
            long numOffsetBytes = Integer.BYTES * (((long)numElements) + (numElements > 0 ? 1 : 0));
            final long bytesExtended = numOffsetBytes & REMAINDER_MOD_8_MASK;
            if (bytesExtended > 0) {
                numOffsetBytes += 8 - bytesExtended;
            }
            listener.noteLogicalBuffer(numOffsetBytes);

            // payload
            final MutableLong numPayloadBytes = new MutableLong();
            subset.forAllRowKeyRanges((s, e) -> {
                // account for payload
                numPayloadBytes.add(myOffsets.get(LongSizedDataStructure.intSize(DEBUG_NAME, e + 1)));
                numPayloadBytes.subtract(myOffsets.get(LongSizedDataStructure.intSize(DEBUG_NAME, s)));
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
                cachedSize = 0;
                if (sendValidityBuffer()) {
                    cachedSize += getValidityMapSerializationSizeFor(subset.intSize(DEBUG_NAME));
                }

                // there are n+1 offsets; it is not assumed first offset is zero
                if (!subset.isEmpty() && subset.size() == myOffsets.size() - 1) {
                    cachedSize += myOffsets.size() * Integer.BYTES;
                    cachedSize += myOffsets.get(subset.intSize(DEBUG_NAME)) - myOffsets.get(0);
                } else  {
                    cachedSize += subset.isEmpty() ? 0 : Integer.BYTES; // account for the n+1 offset
                    subset.forAllRowKeyRanges((s, e) -> {
                        // account for offsets
                        cachedSize += (e - s + 1) * Integer.BYTES;
                        // account for payload
                        cachedSize += myOffsets.get(LongSizedDataStructure.intSize(DEBUG_NAME, e + 1));
                        cachedSize -= myOffsets.get(LongSizedDataStructure.intSize(DEBUG_NAME, s));
                    });
                }

                if (!subset.isEmpty() && (subset.size() & 0x1) == 0) {
                    // then we must also align offset array
                    cachedSize += Integer.BYTES;
                }
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
            subset.forAllRowKeys((rawRow) -> {
                try {
                    final int rowEnd = LongSizedDataStructure.intSize(DEBUG_NAME, rawRow + 1);
                    final int size = myOffsets.get(rowEnd) - myOffsets.get(rowEnd - 1);
                    logicalSize.add(size);
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
                    // we have already int-size verified all rows in the RowSet
                    final int startOffset = myOffsets.get((int) s);
                    final int endOffset = myOffsets.get((int) e + 1);
                    dos.write(myBytes, startOffset, endOffset - startOffset);
                    payloadLen.add(endOffset - startOffset);
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

    static <T> ObjectChunk<T, Values> extractChunkFromInputStream(
            final DataInput is,
            final Iterator<FieldNodeInfo> fieldNodeIter,
            final TLongIterator bufferInfoIter,
            final Mapper<T> mapper) throws IOException {
        final FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final long validityBuffer = bufferInfoIter.next();
        final long offsetsBuffer = bufferInfoIter.next();
        final long payloadBuffer = bufferInfoIter.next();

        final WritableObjectChunk<T, Values> chunk = WritableObjectChunk.makeWritableChunk(nodeInfo.numElements);

        if (nodeInfo.numElements == 0) {
            return chunk;
        }

        final int numValidityLongs = (nodeInfo.numElements + 63) / 64;
        try (final WritableLongChunk<Values> isValid = WritableLongChunk.makeWritableChunk(numValidityLongs);
             final WritableIntChunk<Values> offsets = WritableIntChunk.makeWritableChunk(nodeInfo.numElements + 1)) {
            // Read validity buffer:
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

            // Read offsets:
            final long offBufRead = (nodeInfo.numElements + 1L) * Integer.BYTES;
            if (offsetsBuffer < offBufRead) {
                throw new IllegalStateException("offset buffer is too short for the expected number of elements");
            }
            for (int i = 0; i < nodeInfo.numElements + 1; ++i) {
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
            long nextValid = 0;
            for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                if ((ii % 64) == 0) {
                    nextValid = isValid.get(ii / 64);
                }
                if ((nextValid & 0x1) == 0x1) {
                    final int offset = offsets.get(ii);
                    final int length = offsets.get(ii + 1) - offset;
                    if (offset + length > serializedData.length) {
                        throw new IllegalStateException("not enough data was serialized to parse this element");
                    }
                    chunk.set(ii, mapper.constructFrom(serializedData, offset, length));
                } else {
                    chunk.set(ii, null);
                }
                nextValid >>= 1;
            }
        }

        chunk.setSize(nodeInfo.numElements);
        return chunk;
    }
}
