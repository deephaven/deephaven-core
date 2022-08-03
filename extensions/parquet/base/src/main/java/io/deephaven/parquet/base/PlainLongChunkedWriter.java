/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit PlainIntChunkedWriter and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.parquet.base;

import io.deephaven.parquet.base.util.Helpers;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;

/**
 * Plain encoding except for booleans
 */
public class PlainLongChunkedWriter extends AbstractBulkValuesWriter<LongBuffer, Number> {
    private final ByteBufferAllocator allocator;
    private final int originalLimit;

    private final LongBuffer targetBuffer;
    private final ByteBuffer innerBuffer;

    PlainLongChunkedWriter(int pageSize, ByteBufferAllocator allocator) {
        innerBuffer = allocator.allocate(pageSize);
        innerBuffer.order(ByteOrder.LITTLE_ENDIAN);
        originalLimit = innerBuffer.limit();
        this.allocator = allocator;
        targetBuffer = innerBuffer.asLongBuffer();
        targetBuffer.mark();
        innerBuffer.mark();
    }

    @Override
    public final void writeLong(long v) {
        targetBuffer.put(v);
    }

    @Override
    public long getBufferedSize() {
        return targetBuffer.remaining() * Long.BYTES;
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
    public void writeBulk(LongBuffer bulkValues, int rowCount) {
        targetBuffer.put(bulkValues);
    }

    @Override
    public WriteResult writeBulkFilterNulls(LongBuffer bulkValues, Number nullValue, RunLengthBitPackingHybridEncoder dlEncoder, int rowCount) throws IOException {
        long nullLong = nullValue.longValue();
        while (bulkValues.hasRemaining()) {
            long next = bulkValues.get();
            if (next != nullLong) {
                writeLong(next);
                dlEncoder.writeInt(1);
            } else {
                dlEncoder.writeInt(0);
            }
        }
        return new WriteResult(rowCount);
    }

    @Override
    public WriteResult writeBulkFilterNulls(LongBuffer bulkValues, Number nullValue, int rowCount) {
        long nullLong = nullValue.longValue();
        int i = 0;
        IntBuffer nullOffsets = IntBuffer.allocate(4);
        while (bulkValues.hasRemaining()) {
            long next = bulkValues.get();
            if (next != nullLong) {
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
