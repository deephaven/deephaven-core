/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base;

import io.deephaven.parquet.base.util.Helpers;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.Objects;

/**
 * Plain encoding except for binary values
 */
public class PlainBinaryChunkedWriter extends AbstractBulkValuesWriter<Binary[], Binary> {
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
        if (innerBuffer.remaining() < v.length() + 4) {
            ByteBuffer newBuffer = allocator.allocate(innerBuffer.capacity() * 2);
            innerBuffer.flip();
            newBuffer.mark();
            newBuffer.put(innerBuffer);
            allocator.release(innerBuffer);
            innerBuffer = newBuffer;
            innerBuffer.order(ByteOrder.LITTLE_ENDIAN);
            innerBuffer.limit(innerBuffer.capacity());
        }
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
        innerBuffer.position(0);
        innerBuffer.limit(innerBuffer.capacity());
    }

    @Override
    public ByteBuffer getByteBufferView() {
        innerBuffer.flip();
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
    public void writeBulk(@NotNull Binary[] bulkValues, int rowCount) {
        for (int i = 0; i < rowCount; i++) {
            writeBytes(bulkValues[i]);
        }
    }

    @NotNull
    @Override
    public WriteResult writeBulkFilterNulls(@NotNull final Binary[] bulkValues,
                                            @Nullable final Binary nullValue,
                                            @NotNull final RunLengthBitPackingHybridEncoder dlEncoder,
                                            final int rowCount) throws IOException {
        for (int i = 0; i < rowCount; i++) {
            if (bulkValues[i] != null) {
                writeBytes(bulkValues[i]);
                dlEncoder.writeInt(1);
            } else {
                dlEncoder.writeInt(0);
            }
        }
        return new WriteResult(rowCount);
    }

    @Override
    public @NotNull WriteResult writeBulkFilterNulls(@NotNull Binary[] bulkValues, @Nullable Binary nullValue, int nonNullLeafCount) {
        IntBuffer nullOffsets = IntBuffer.allocate(4);
        for (int i = 0; i < nonNullLeafCount; i++) {
            if (bulkValues[i] != null) {
                writeBytes(bulkValues[i]);
            } else {
                nullOffsets = Helpers.ensureCapacity(nullOffsets);
                nullOffsets.put(i);
            }
        }
        return new WriteResult(nonNullLeafCount, nullOffsets);
    }
}
