/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base;

import io.deephaven.parquet.base.util.Helpers;
import io.deephaven.util.QueryConstants;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

/**
 * A writer for encoding ints in the PLAIN format
 */
public class PlainIntChunkedWriter extends AbstractBulkValuesWriter<IntBuffer> {
    private static final int MAXIMUM_TOTAL_CAPACITY = Integer.MAX_VALUE / Integer.BYTES;
    private final ByteBufferAllocator allocator;

    private IntBuffer targetBuffer;
    private ByteBuffer innerBuffer;


    PlainIntChunkedWriter(final int targetPageSize, @NotNull final ByteBufferAllocator allocator) {
        this.allocator = allocator;
        realloc(targetPageSize);
    }

    @Override
    public final void writeInteger(int v) {
        targetBuffer.put(v);
    }

    @Override
    public long getBufferedSize() {
        return (long) targetBuffer.remaining() * Integer.BYTES;
    }

    @Override
    public BytesInput getBytes() {
        innerBuffer.limit(innerBuffer.position() + targetBuffer.position() * Integer.BYTES);
        return BytesInput.from(innerBuffer);
    }

    @Override
    public void reset() {
        innerBuffer.reset();
        innerBuffer.limit(innerBuffer.capacity());
        targetBuffer.reset();
    }

    @Override
    public ByteBuffer getByteBufferView() {
        innerBuffer.limit(innerBuffer.position() + targetBuffer.position() * Integer.BYTES);
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
    public void writeBulk(@NotNull IntBuffer bulkValues, int rowCount) {
        ensureCapacityFor(bulkValues);
        targetBuffer.put(bulkValues);
    }

    @NotNull
    @Override
    public WriteResult writeBulkFilterNulls(@NotNull final IntBuffer bulkValues,
                                            @NotNull final RunLengthBitPackingHybridEncoder dlEncoder,
                                            final int rowCount) throws IOException {
        ensureCapacityFor(bulkValues);
        while (bulkValues.hasRemaining()) {
            final int next = bulkValues.get();
            if (next != QueryConstants.NULL_INT) {
                writeInteger(next);
                dlEncoder.writeInt(DL_ITEM_PRESENT);
            } else {
                dlEncoder.writeInt(DL_ITEM_NULL);
            }
        }
        return new WriteResult(rowCount);
    }

    @NotNull
    @Override
    public WriteResult writeBulkFilterNulls(@NotNull final IntBuffer bulkValues,
                                            final int rowCount) {
        ensureCapacityFor(bulkValues);
        int i = 0;
        IntBuffer nullOffsets = IntBuffer.allocate(4);
        while (bulkValues.hasRemaining()) {
            final int next = bulkValues.get();
            if (next != QueryConstants.NULL_INT) {
                writeInteger(next);
            } else {
                nullOffsets = Helpers.ensureCapacity(nullOffsets);
                nullOffsets.put(i);
            }
            i++;
        }
        return new WriteResult(rowCount, nullOffsets);
    }

    private void ensureCapacityFor(@NotNull final IntBuffer valuesToAdd) {
        if(!valuesToAdd.hasRemaining()) {
            return;
        }

        final int currentCapacity = targetBuffer.capacity();
        final int currentPosition = targetBuffer.position();
        final int requiredCapacity = currentPosition + valuesToAdd.remaining();
        if(requiredCapacity < currentCapacity) {
            return;
        }

        if(requiredCapacity > MAXIMUM_TOTAL_CAPACITY) {
            throw new IllegalStateException("Unable to write " + requiredCapacity + " values");
        }

        int newCapacity = currentCapacity;
        while(newCapacity < requiredCapacity) {
            newCapacity = Math.min(MAXIMUM_TOTAL_CAPACITY, newCapacity * 2);
        }

        realloc(newCapacity * Integer.BYTES);
    }

    private void realloc(final int newCapacity) {
        final ByteBuffer newBuf = allocator.allocate(newCapacity);
        newBuf.order(ByteOrder.LITTLE_ENDIAN);
        final IntBuffer newIntBuf = newBuf.asIntBuffer();
        newBuf.mark();
        newIntBuf.mark();

        if(this.innerBuffer != null) {
            targetBuffer.limit(targetBuffer.position());
            targetBuffer.reset();
            newIntBuf.put(targetBuffer);
            allocator.release(innerBuffer);
        }
        innerBuffer = newBuf;
        targetBuffer = newIntBuf;
    }
}
