/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit PlainIntChunkedWriter and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
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
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;

// Duplicate for Replication
import java.nio.IntBuffer;

/**
 * A writer for encoding floats in the PLAIN format
 */
public class PlainFloatChunkedWriter extends AbstractBulkValuesWriter<FloatBuffer, Float> {
    private static final int MAXIMUM_TOTAL_CAPACITY = Integer.MAX_VALUE / Float.BYTES;

    private final int targetPageSize;
    private final ByteBufferAllocator allocator;

    private FloatBuffer targetBuffer;
    private ByteBuffer innerBuffer;


    PlainFloatChunkedWriter(final int targetPageSize, @NotNull final ByteBufferAllocator allocator) {
        this.targetPageSize = targetPageSize;
        this.allocator = allocator;
    }

    @Override
    public final void writeFloat(float v) {
        targetBuffer.put(v);
    }

    @Override
    public long getBufferedSize() {
        return (long) targetBuffer.remaining() * Float.BYTES;
    }

    @Override
    public BytesInput getBytes() {
        innerBuffer.limit(innerBuffer.position() + targetBuffer.position() * Float.BYTES);
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
        innerBuffer.limit(innerBuffer.position() + targetBuffer.position() * Float.BYTES);
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
    public void writeBulk(@NotNull FloatBuffer bulkValues, int rowCount) {
        ensureCapacityFor(bulkValues);
        targetBuffer.put(bulkValues);
    }

    @NotNull
    @Override
    public WriteResult writeBulkFilterNulls(@NotNull final FloatBuffer bulkValues,
                                            @Nullable final Float nullValue,
                                            @NotNull final RunLengthBitPackingHybridEncoder dlEncoder,
                                            final int rowCount) throws IOException {
        ensureCapacityFor(bulkValues);
        while (bulkValues.hasRemaining()) {
            final float next = bulkValues.get();
            if (next != QueryConstants.NULL_FLOAT) {
                writeFloat(next);
                dlEncoder.writeInt(DL_ITEM_PRESENT);
            } else {
                dlEncoder.writeInt(DL_ITEM_NULL);
            }
        }
        return new WriteResult(rowCount);
    }

    @NotNull
    @Override
    public WriteResult writeBulkFilterNulls(@NotNull final FloatBuffer bulkValues,
                                            @Nullable final Float nullValue,
                                            final int rowCount) {
        ensureCapacityFor(bulkValues);
        int i = 0;
        IntBuffer nullOffsets = IntBuffer.allocate(4);
        while (bulkValues.hasRemaining()) {
            final float next = bulkValues.get();
            if (next != QueryConstants.NULL_FLOAT) {
                writeFloat(next);
            } else {
                nullOffsets = Helpers.ensureCapacity(nullOffsets);
                nullOffsets.put(i);
            }
            i++;
        }
        return new WriteResult(rowCount, nullOffsets);
    }

    private void ensureCapacityFor(@NotNull final FloatBuffer valuesToAdd) {
        if(!valuesToAdd.hasRemaining()) {
            return;
        }

        final int currentCapacity = targetBuffer == null ? 0 : targetBuffer.capacity();
        final int currentPosition = targetBuffer == null ? 0 : targetBuffer.position();
        final int requiredCapacity = currentPosition + valuesToAdd.remaining();
        if(requiredCapacity < currentCapacity) {
            return;
        }

        if(requiredCapacity > MAXIMUM_TOTAL_CAPACITY) {
            throw new IllegalStateException("Unable to write " + requiredCapacity + " values");
        }

        int newCapacity = Math.max(targetPageSize / Float.BYTES, currentCapacity * 2);
        while(newCapacity < requiredCapacity) {
            newCapacity = Math.min(MAXIMUM_TOTAL_CAPACITY, newCapacity * 2);
        }

        newCapacity *= Float.BYTES;

        final ByteBuffer newBuf = allocator.allocate(newCapacity);
        newBuf.order(ByteOrder.LITTLE_ENDIAN);
        final FloatBuffer newFloatBuf = newBuf.asFloatBuffer();
        newBuf.mark();
        newFloatBuf.mark();

        if(this.innerBuffer != null) {
            targetBuffer.flip();
            newFloatBuf.put(targetBuffer);
            allocator.release(innerBuffer);
        }
        this.innerBuffer = newBuf;
        this.targetBuffer = newFloatBuf;
        targetBuffer.mark();
        innerBuffer.mark();
    }
}
