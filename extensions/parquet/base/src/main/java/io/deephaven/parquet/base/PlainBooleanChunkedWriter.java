//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.BooleanUtils;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

/**
 * Plain encoding except for booleans
 */
final class PlainBooleanChunkedWriter extends AbstractBulkValuesWriter<ByteBuffer> {
    private final BooleanPlainValuesWriter writer;
    private IntBuffer nullOffsets;

    PlainBooleanChunkedWriter() {
        writer = new BooleanPlainValuesWriter();
        nullOffsets = IntBuffer.allocate(4);
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
    public void writeBulk(@NotNull ByteBuffer bulkValues,
            final int rowCount,
            @NotNull final Statistics<?> statistics) {
        while (bulkValues.hasRemaining()) {
            final boolean v = bulkValues.get() == 1;
            writeBoolean(v);
            statistics.updateStats(v);
        }
    }

    @NotNull
    @Override
    public WriteResult writeBulkFilterNulls(@NotNull ByteBuffer bulkValues,
            @NotNull RunLengthBitPackingHybridEncoder dlEncoder,
            final int rowCount,
            @NotNull final Statistics<?> statistics) throws IOException {
        while (bulkValues.hasRemaining()) {
            final Boolean next = BooleanUtils.byteAsBoolean(bulkValues.get());
            if (next != null) {
                writeBoolean(next);
                statistics.updateStats(next);
                dlEncoder.writeInt(DL_ITEM_PRESENT);
            } else {
                statistics.incrementNumNulls();
                dlEncoder.writeInt(DL_ITEM_NULL);
            }
        }
        return new WriteResult(rowCount);
    }

    @Override
    public @NotNull WriteResult writeBulkVectorFilterNulls(@NotNull ByteBuffer bulkValues,
            final int rowCount,
            @NotNull final Statistics<?> statistics) {
        nullOffsets.clear();
        int i = 0;
        while (bulkValues.hasRemaining()) {
            final Boolean next = BooleanUtils.byteAsBoolean(bulkValues.get());
            if (next != null) {
                writeBoolean(next);
                statistics.updateStats(next);
            } else {
                nullOffsets = Helpers.ensureCapacity(nullOffsets);
                nullOffsets.put(i);
                statistics.incrementNumNulls();
            }
            i++;
        }
        return new WriteResult(rowCount, nullOffsets);
    }
}
