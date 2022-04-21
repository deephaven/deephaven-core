/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.extensions.barrage.chunk;

import com.google.common.io.LittleEndianDataOutputStream;
import gnu.trove.iterator.TLongIterator;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
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
import java.util.ArrayList;
import java.util.Iterator;

public class VarBinaryChunkInputStreamGenerator<T> extends BaseChunkInputStreamGenerator<ObjectChunk<T, Values>> {
    private static final String DEBUG_NAME = "ObjectChunkInputStream Serialization";

    private final Appender<T> appendItem;

    public static class ByteStorage {

        private final WritableIntChunk<ChunkPositions> offsets;
        private final ArrayList<byte[]> byteArrays;
        private final ArrayList<Integer> byteArraySizes;
        private final ArrayList<Integer> byteArrayStartIndex;

        // low-budget memoization
        private long lastIdx = -1;
        private int lastIdxArrayIdx = -1;
        private int lastIdxOffset = -1;

        public ByteStorage(int size) {
            offsets = WritableIntChunk.makeWritableChunk(size);

            byteArrays = new ArrayList<>();
            byteArraySizes = new ArrayList<>();
            byteArrayStartIndex = new ArrayList<>();
        }

        public int size() {
            return byteArrays.size();
        }

        void addByteArray(byte[] arr, int startIndex, int size) {
            byteArrays.add(arr);
            byteArrayStartIndex.add(startIndex);
            byteArraySizes.add(size);
        }

        public boolean isEmpty() {
            return byteArrays.isEmpty();
        }

        /***
         * computes the size of the payload from sPos to ePos (inclusive)
         *
         * @param sPos the first data item to include in this payload
         * @param ePos the last data item to include in this payload
         * @return number of bytes in the payload
         */
        public long getPayloadSize(int sPos, int ePos) {
            final int startArrayIndex;
            final int startOffset;

            // might already have these start offsets saved
            if (sPos == lastIdx) {
                startArrayIndex = lastIdxArrayIdx;
                startOffset = lastIdxOffset;
            } else {
                startArrayIndex = getByteArrayIndex(sPos);
                startOffset = offsets.get(sPos);
            }

            // store these for current and later re-use
            lastIdx = ePos + 1;
            lastIdxArrayIdx = getByteArrayIndex((int)lastIdx);
            lastIdxOffset = offsets.get(ePos + 1);

            if (startArrayIndex == lastIdxArrayIdx) { // same byte array, can optimize
                return lastIdxOffset - startOffset;
            } else {
                // need to span multiple byte arrays
                long byteCount = getByteArraySize(startArrayIndex) - startOffset;
                for (int midArrayIndex = startArrayIndex + 1; midArrayIndex < lastIdxArrayIdx; midArrayIndex++) {
                    byteCount += getByteArraySize(midArrayIndex);
                }
                byteCount += lastIdxOffset;
                return byteCount;
            }
        }

        public Integer getByteArrayIndex(int pos) {
            // optimize for most common case
            if (byteArrays.size() == 1) {
                return 0;
            }
            for (int i = byteArrayStartIndex.size() - 1; i > 0; --i) {
                if (byteArrayStartIndex.get(i) <= pos) {
                    return i;
                }
            }
            return 0;
        }

        public int getByteArraySize(int arrayIdx) {
            return byteArraySizes.get(arrayIdx);
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
            final int startArrayIndex = getByteArrayIndex(sPos);
            final int startOffset = offsets.get(sPos);

            final int endArrayIndex = getByteArrayIndex(ePos + 1);
            final int endOffset = offsets.get(ePos + 1);

            long writeLen = 0;

            if (startArrayIndex == endArrayIndex) { // same byte array, can optimize
                dos.write(byteArrays.get(startArrayIndex), startOffset, endOffset - startOffset);
                writeLen += endOffset - startOffset;
            } else {
                // need to span multiple byte arrays
                int firstSize = byteArraySizes.get(startArrayIndex) - startOffset;
                dos.write(byteArrays.get(startArrayIndex), startOffset, firstSize);
                writeLen += firstSize;

                for (int midArrayIndex = startArrayIndex + 1; midArrayIndex < endArrayIndex; midArrayIndex++) {
                    int midSize = getByteArraySize(midArrayIndex);
                    dos.write(byteArrays.get(midArrayIndex), 0, midSize);
                    writeLen += midSize;
                }

                dos.write(byteArrays.get(endArrayIndex), 0, endOffset);
                writeLen += endOffset;
            }
            return writeLen;
        }
    };

    private ByteStorage byteStorage = null;

    public interface Appender<T> {
        void append(OutputStream out, T item) throws IOException;
    }

    public interface Mapper<T> {
        T constructFrom(byte[] buf, int offset, int length) throws IOException;
    }

    VarBinaryChunkInputStreamGenerator(final ObjectChunk<T, Values> chunk,
                                       final Appender<T> appendItem) {
        super(chunk, 0);
        this.appendItem = appendItem;
    }

    private synchronized void computePayload() throws IOException {
        if (byteStorage != null) {
            return;
        }
        byteStorage = new ByteStorage(chunk.size() == 0 ? 0 : (chunk.size() + 1));

        BarrageProtoUtil.ExposedByteArrayOutputStream baos = new BarrageProtoUtil.ExposedByteArrayOutputStream();
        if (chunk.size() > 0) {
            byteStorage.offsets.set(0, 0);
        }
        int baosSize = 0;
        int startIndex = 0;

        for (int i = 0; i < chunk.size(); ++i) {
            if (chunk.get(i) != null) {
                try {
                    appendItem.append(baos, chunk.get(i));
                    baosSize = baos.size();
                } catch (OutOfMemoryError ex) {
                    // we overran the buffer on this item and the output stream probably has junk from the failed write
                    // but it is no more than the size of a single data item.  We use the stored output stream size
                    // though instead of querying the size of the output stream (since that includes junk bytes)

                    // add the buffer to storage
                    byteStorage.addByteArray(baos.peekBuffer(), startIndex, baosSize);

                    // close the old output stream and create a new one
                    baos.close();
                    baos = new BarrageProtoUtil.ExposedByteArrayOutputStream();

                    // add the item to the new buffer
                    appendItem.append(baos, chunk.get(i));
                    baosSize = baos.size();
                    startIndex = i;
                    byteStorage.offsets.set(i, 0);
                }
            }
            byteStorage.offsets.set(i + 1, baosSize);
        }
        byteStorage.addByteArray(baos.peekBuffer(), startIndex, baosSize);
        baos.close();
    }

    @Override
    public void close() {
        if (REFERENCE_COUNT_UPDATER.decrementAndGet(this) == 0) {
            if (chunk instanceof PoolableChunk) {
                ((PoolableChunk) chunk).close();
            }
            if (byteStorage != null) {
                byteStorage.offsets.close();
            }
        }
    }

    @Override
    public DrainableColumn getInputStream(final StreamReaderOptions options, final @Nullable RowSet subset) throws IOException {
        computePayload();
        return new ObjectChunkInputStream(options, byteStorage, subset);
    }

    private class ObjectChunkInputStream extends BaseChunkInputStream {
        private int cachedSize = -1;
        private final ByteStorage myByteStorage;

        private ObjectChunkInputStream(
                final StreamReaderOptions options,
                final ByteStorage myByteStorage, final RowSet subset) {
            super(chunk, options, subset);
            this.myByteStorage = myByteStorage;
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
                numPayloadBytes.add(myByteStorage.getPayloadSize((int)s, (int)e));
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
                if (!subset.isEmpty() && subset.size() == myByteStorage.offsets.size() - 1) {
                    totalCachedSize.add(myByteStorage.offsets.size() * Integer.BYTES);
                    for (int i = 0; i < myByteStorage.size(); i++) {
                        totalCachedSize.add(myByteStorage.getByteArraySize(i));
                    }
                } else  {
                    totalCachedSize.add(subset.isEmpty() ? 0 : Integer.BYTES); // account for the n+1 offset
                    subset.forAllRowKeyRanges((s, e) -> {
                        // account for offsets
                        totalCachedSize.add((e - s + 1) * Integer.BYTES);

                        // account for payload
                        totalCachedSize.add(myByteStorage.getPayloadSize((int)s, (int)e));
                    });
                }

                if (!subset.isEmpty() && (subset.size() & 0x1) == 0) {
                    // then we must also align offset array
                    totalCachedSize.add(Integer.BYTES);
                }
                cachedSize = LongSizedDataStructure.intSize("VarBinaryChunkInputStreamGenerator.getRawSize", totalCachedSize.longValue());
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
                    logicalSize.add(myByteStorage.getPayloadSize((int)idx,(int)idx));
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
                    payloadLen.add(myByteStorage.writePayload(dos, (int) s, (int) e));
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
            final TLongIterator bufferInfoIter,
            final Mapper<T> mapper,
            final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {
        final FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final long validityBuffer = bufferInfoIter.next();
        final long offsetsBuffer = bufferInfoIter.next();
        final long payloadBuffer = bufferInfoIter.next();

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
                        if (offset + length > serializedData.length) {
                            throw new IllegalStateException("not enough data was serialized to parse this element: " +
                                    "elementIndex=" + ei + " offset=" + offset + " length=" + length +
                                    " serializedLen=" + serializedData.length);
                        }
                        chunk.set(outOffset + ei++, mapper.constructFrom(serializedData, offset, length));                        validityWord >>= 1;
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
