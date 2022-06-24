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
import java.nio.LongBuffer;

// Duplicate for Replication
import java.nio.IntBuffer;

/**
 * Plain encoding except for booleans
 */
public class PlainLongChunkedWriter extends AbstractBulkValuesWriter<LongBuffer, Long> {
    private final ByteBufferAllocator allocator;
    private final int originalLimit;

    private final LongBuffer targetBuffer;
    private final ByteBuffer innerBuffer;

    PlainLongChunkedWriter(final int pageSize, @NotNull final ByteBufferAllocator allocator) {
        this.innerBuffer = allocator.allocate(pageSize);
        this.innerBuffer.order(ByteOrder.LITTLE_ENDIAN);
        this.originalLimit = innerBuffer.limit();
        this.allocator = allocator;
        this.targetBuffer = innerBuffer.asLongBuffer();
        targetBuffer.mark();
        innerBuffer.mark();
    }

    @Override
    public final void writeLong(long v) {
        targetBuffer.put(v);
    }

    @Override
    public long getBufferedSize() {
        return (long) targetBuffer.remaining() * Long.BYTES;
    }

    @Override
    public BytesInput getBytes() {
        innerBuffer.limit(innerBuffer.position() + targetBuffer.position() * Long.BYTES);
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
        innerBuffer.limit(innerBuffer.position() + targetBuffer.position() * Long.BYTES);
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
    public void writeBulk(@NotNull LongBuffer bulkValues, int rowCount) {
        targetBuffer.put(bulkValues);
    }

    @NotNull
    @Override
    public WriteResult writeBulkFilterNulls(@NotNull final LongBuffer bulkValues,
                                            @Nullable final Long nullValue,
                                            @NotNull final RunLengthBitPackingHybridEncoder dlEncoder,
                                            final int rowCount) throws IOException {
        while (bulkValues.hasRemaining()) {
            final long next = bulkValues.get();
            if (next != QueryConstants.NULL_LONG) {
                writeLong(next);
                dlEncoder.writeInt(DL_ITEM_PRESENT);
            } else {
                dlEncoder.writeInt(DL_ITEM_NULL);
            }
        }
        return new WriteResult(rowCount);
    }

    @NotNull
    @Override
    public WriteResult writeBulkFilterNulls(@NotNull final LongBuffer bulkValues,
                                            @Nullable final Long nullValue,
                                            final int rowCount) {
        int i = 0;
        IntBuffer nullOffsets = IntBuffer.allocate(4);
        while (bulkValues.hasRemaining()) {
            final long next = bulkValues.get();
            if (next != QueryConstants.NULL_LONG) {
                writeLong(next);
            } else {
                nullOffsets = Helpers.ensureCapacity(nullOffsets);
                nullOffsets.put(i);
            }
            i++;
        }
        return new WriteResult(rowCount, nullOffsets);
    }
}
