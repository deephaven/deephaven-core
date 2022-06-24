/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit PlainIntChunkedWriter and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/**
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
 * Plain encoding except for booleans
 */
public class PlainFloatChunkedWriter extends AbstractBulkValuesWriter<FloatBuffer, Float> {
    private final ByteBufferAllocator allocator;
    private final int originalLimit;

    private final FloatBuffer targetBuffer;
    private final ByteBuffer innerBuffer;

    PlainFloatChunkedWriter(final int pageSize, @NotNull final ByteBufferAllocator allocator) {
        this.innerBuffer = allocator.allocate(pageSize);
        this.innerBuffer.order(ByteOrder.LITTLE_ENDIAN);
        this.originalLimit = innerBuffer.limit();
        this.allocator = allocator;
        this.targetBuffer = innerBuffer.asFloatBuffer();
        targetBuffer.mark();
        innerBuffer.mark();
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
        innerBuffer.limit(originalLimit);
        innerBuffer.reset();
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
        targetBuffer.put(bulkValues);
    }

    @NotNull
    @Override
    public WriteResult writeBulkFilterNulls(@NotNull final FloatBuffer bulkValues,
                                            @Nullable final Float nullValue,
                                            @NotNull final RunLengthBitPackingHybridEncoder dlEncoder,
                                            final int rowCount) throws IOException {
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
}
