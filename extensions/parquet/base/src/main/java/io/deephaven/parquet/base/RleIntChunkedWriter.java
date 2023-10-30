/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base;

import io.deephaven.parquet.base.util.Helpers;
import io.deephaven.util.QueryConstants;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import static org.apache.parquet.bytes.BytesInput.concat;

/**
 * Plain encoding except for booleans
 */
public class RleIntChunkedWriter extends AbstractBulkValuesWriter<IntBuffer> {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.parquet.column.values.plain.PlainValuesWriter.class);

    private final RunLengthBitPackingHybridEncoder encoder;
    private final byte bitWidth;
    private IntBuffer nullOffsets;

    RleIntChunkedWriter(int pageSize, ByteBufferAllocator allocator, byte bitWidth) {
        encoder = new RunLengthBitPackingHybridEncoder(bitWidth, pageSize, pageSize, allocator);
        this.bitWidth = bitWidth;
        nullOffsets = IntBuffer.allocate(4);
    }

    @Override
    public final void writeInteger(int v) {
        try {
            encoder.writeInt(v);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getBufferedSize() {
        return encoder.getBufferedSize();
    }

    @Override
    public BytesInput getBytes() {
        try {
            byte[] bytesHeader = new byte[]{bitWidth};
            BytesInput rleEncodedBytes = encoder.toBytes();
            LOG.debug("rle encoded bytes {}", rleEncodedBytes.size());
            return concat(BytesInput.from(bytesHeader), rleEncodedBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void reset() {
        encoder.reset();
    }

    @Override
    public ByteBuffer getByteBufferView() {
        try {
            return getBytes().toByteBuffer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        encoder.close();
    }

    @Override
    public long getAllocatedSize() {
        return encoder.getAllocatedSize();
    }

    @Override
    public Encoding getEncoding() {
        return Encoding.PLAIN;
    }

    @Override
    public String memUsageString(String prefix) {
        return String.format("%s %s, %,d bytes", prefix, getClass().getSimpleName(), encoder.getAllocatedSize());
    }

    @Override
    public void writeBulk(@NotNull IntBuffer bulkValues,
                          final int rowCount,
                          @Nullable final Statistics<?> statistics) {
        // Track statistics while we write the values.
        for (int i = 0; i < rowCount; i++) {
            final int v = bulkValues.get();
            writeInteger(v);
            statistics.updateStats(v);
        }
    }

    @NotNull
    @Override
    public WriteResult writeBulkFilterNulls(@NotNull IntBuffer bulkValues,
                                            @NotNull RunLengthBitPackingHybridEncoder dlEncoder,
                                            final int rowCount,
                                            @NotNull final Statistics<?> statistics) throws IOException {
        while (bulkValues.hasRemaining()) {
            int v = bulkValues.get();
            if (v != QueryConstants.NULL_INT) {
                writeInteger(v);
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
    public @NotNull WriteResult writeBulkVectorFilterNulls(@NotNull IntBuffer bulkValues,
                                                           final int rowCount,
                                                           @NotNull final Statistics<?> statistics) {
        nullOffsets.clear();
        int i = 0;
        while (bulkValues.hasRemaining()) {
            int v = bulkValues.get();
            if (v != QueryConstants.NULL_INT) {
                writeInteger(v);
                statistics.updateStats(v);
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
