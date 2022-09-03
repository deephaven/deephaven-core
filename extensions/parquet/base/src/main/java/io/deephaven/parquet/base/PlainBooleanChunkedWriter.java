/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base;

import io.deephaven.parquet.base.util.Helpers;
import io.deephaven.util.QueryConstants;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

/**
 * Plain encoding except for booleans
 */
public class PlainBooleanChunkedWriter extends AbstractBulkValuesWriter<ByteBuffer> {
    private final BooleanPlainValuesWriter writer;

    public PlainBooleanChunkedWriter() {
        writer = new BooleanPlainValuesWriter();
    }

    @Override
    public final void writeBoolean(boolean v) {
        writer.writeBoolean(v);
    }

    @Override
    public long getBufferedSize() {
        return writer.getBufferedSize();
    }

    @Override
    public BytesInput getBytes() {
        return writer.getBytes();
    }

    @Override
    public void reset() {
        writer.reset();
    }

    @Override
    public ByteBuffer getByteBufferView() throws IOException {
        return writer.getBytes().toByteBuffer();
    }

    @Override
    public void close() {
        writer.close();
    }

    @Override
    public long getAllocatedSize() {
        return writer.getAllocatedSize();
    }

    @Override
    public Encoding getEncoding() {
        return Encoding.PLAIN;
    }

    @Override
    public String memUsageString(String prefix) {
        return String.format("%s %s, %,d bytes", prefix, getClass().getSimpleName(), writer.getAllocatedSize());
    }

    @Override
    public void writeBulk(@NotNull ByteBuffer bulkValues, int rowCount) {
        while (bulkValues.hasRemaining()) {
            writeBoolean(bulkValues.get() == 1);
        }
    }

    @NotNull
    @Override
    public WriteResult writeBulkFilterNulls(@NotNull ByteBuffer bulkValues, @NotNull RunLengthBitPackingHybridEncoder dlEncoder, int rowCount) throws IOException {
        while (bulkValues.hasRemaining()) {
            final byte next = bulkValues.get();
            if (next != QueryConstants.NULL_BYTE) {
                writeBoolean(next == 1);
                dlEncoder.writeInt(DL_ITEM_PRESENT);
            } else {
                dlEncoder.writeInt(DL_ITEM_NULL);
            }
        }
        return new WriteResult(rowCount);
    }

    @Override
    public @NotNull WriteResult writeBulkFilterNulls(@NotNull ByteBuffer bulkValues, int rowCount) {
        IntBuffer nullOffsets = IntBuffer.allocate(4);
        int i = 0;
        while (bulkValues.hasRemaining()) {
            final byte next = bulkValues.get();
            if (next != QueryConstants.NULL_BYTE) {
                writeBoolean(next == 1);
            } else {
                nullOffsets = Helpers.ensureCapacity(nullOffsets);
                nullOffsets.put(i);
            }
            i++;
        }
        return new WriteResult(rowCount, nullOffsets);
    }
}
