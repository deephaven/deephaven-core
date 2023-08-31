/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base;

import io.deephaven.base.ArrayUtil;
import io.deephaven.parquet.base.util.Helpers;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

/**
 * Plain encoding except for binary values
 */
public class PlainBinaryChunkedWriter extends AbstractBulkValuesWriter<Binary[]> {

    private static final int MAXIMUM_TOTAL_CAPACITY = ArrayUtil.MAX_ARRAY_SIZE;

    private final ByteBufferAllocator allocator;

    ByteBuffer innerBuffer;

    public PlainBinaryChunkedWriter(final int pageSize, @NotNull final ByteBufferAllocator allocator) {
        innerBuffer = allocator.allocate(pageSize);
        innerBuffer.order(ByteOrder.LITTLE_ENDIAN);
        this.allocator = allocator;
        innerBuffer.mark();
    }

    @Override
    public final void writeBytes(Binary v) {
        ensureCapacityFor(v);
        innerBuffer.putInt(v.length());
        innerBuffer.put(v.toByteBuffer());
    }

    @Override
    public long getBufferedSize() {
        return innerBuffer.remaining();
    }

    @Override
    public BytesInput getBytes() {
        return BytesInput.from(innerBuffer);
    }

    @Override
    public void reset() {
        innerBuffer.reset();
        innerBuffer.limit(innerBuffer.capacity());
    }

    @Override
    public ByteBuffer getByteBufferView() {
        innerBuffer.limit(innerBuffer.position());
        innerBuffer.reset();
        return innerBuffer;
    }

    @Override
    public void close() {
        allocator.release(innerBuffer);
    }

    @Override
    public long getAllocatedSize() {
        return innerBuffer.capacity();
    }

    @Override
    public Encoding getEncoding() {
        return Encoding.PLAIN;
    }

    @Override
    public String memUsageString(String prefix) {
        return String.format("%s %s, %,d bytes", prefix, getClass().getSimpleName(), innerBuffer.capacity());
    }

    @Override
    public void writeBulk(@NotNull Binary[] bulkValues,
                          final int rowCount,
                          @NotNull final Statistics<?> statistics) {
        for (int i = 0; i < rowCount; i++) {
            final Binary v = bulkValues[i];
            writeBytes(v);
            statistics.updateStats(v);
        }
    }

    @NotNull
    @Override
    public WriteResult writeBulkFilterNulls(@NotNull final Binary[] bulkValues,
                                            @NotNull final RunLengthBitPackingHybridEncoder dlEncoder,
                                            final int rowCount,
                                            @NotNull final Statistics<?> statistics) throws IOException {
        for (int i = 0; i < rowCount; i++) {
            if (bulkValues[i] != null) {
                final Binary v = bulkValues[i];
                writeBytes(v);
                statistics.updateStats(v);
                dlEncoder.writeInt(DL_ITEM_PRESENT);
            } else {
                statistics.incrementNumNulls();
                dlEncoder.writeInt(DL_ITEM_NULL);
            }
        }
        return new WriteResult(rowCount);
    }

    @Override
    public @NotNull WriteResult writeBulkVectorFilterNulls(@NotNull Binary[] bulkValues,
                                                           final int nonNullLeafCount,
                                                           @NotNull final Statistics<?> statistics) {
        IntBuffer nullOffsets = IntBuffer.allocate(4);
        for (int i = 0; i < nonNullLeafCount; i++) {
            if (bulkValues[i] != null) {
                final Binary v = bulkValues[i];
                writeBytes(v);
                statistics.updateStats(v);
            } else {
                nullOffsets = Helpers.ensureCapacity(nullOffsets);
                nullOffsets.put(i);
                statistics.incrementNumNulls();
            }
        }
        return new WriteResult(nonNullLeafCount, nullOffsets);
    }

    private void ensureCapacityFor(@NotNull final Binary v) {
        if (v.length() == 0 || innerBuffer.remaining() >= v.length() + Integer.BYTES) {
            return;
        }

        final int currentCapacity = innerBuffer.capacity();
        final int currentPosition = innerBuffer.position();
        final long requiredCapacity = (long)currentPosition + v.length() + Integer.BYTES;
        if (requiredCapacity > MAXIMUM_TOTAL_CAPACITY) {
            throw new IllegalStateException("Unable to write " + requiredCapacity + " values. (Maximum capacity: " + MAXIMUM_TOTAL_CAPACITY + ".)");
        }

        int newCapacity = currentCapacity;
        while (newCapacity < requiredCapacity) {
            newCapacity = (int) Math.min(MAXIMUM_TOTAL_CAPACITY, newCapacity * 2L);
        }

        final ByteBuffer newBuf = allocator.allocate(newCapacity);
        newBuf.order(ByteOrder.LITTLE_ENDIAN);
        newBuf.mark();

        innerBuffer.limit(innerBuffer.position());
        innerBuffer.reset();
        newBuf.put(innerBuffer);
        allocator.release(innerBuffer);
        innerBuffer = newBuf;
    }
}
