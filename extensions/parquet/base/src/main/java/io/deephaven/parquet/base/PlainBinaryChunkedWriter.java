/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit PlainIntChunkedWriter and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.parquet.base;

import io.deephaven.parquet.base.util.Helpers;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.Objects;


/**
 * Plain encoding except for booleans
 */
public class PlainBinaryChunkedWriter extends AbstractBulkValuesWriter<Binary[], Binary> {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.parquet.column.values.plain.PlainValuesWriter.class);
    private final ByteBufferAllocator allocator;
    private int originalLimit;

    ByteBuffer innerBuffer;

    public PlainBinaryChunkedWriter(int pageSize, ByteBufferAllocator allocator) {
        innerBuffer = allocator.allocate(pageSize);
        innerBuffer.order(ByteOrder.LITTLE_ENDIAN);
        this.allocator = allocator;
        innerBuffer.mark();
        originalLimit = innerBuffer.limit();
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
            originalLimit = innerBuffer.limit();
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
        innerBuffer.limit(originalLimit);
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
    public void writeBulk(Binary[] bulkValues, int rowCount) {
        for (int i = 0; i < rowCount; i++) {
            writeBytes(bulkValues[i]);
        }
    }

    @Override
    public WriteResult writeBulkFilterNulls(Binary[] bulkValues, Binary nullValue, RunLengthBitPackingHybridEncoder dlEncoder, int rowCount) throws IOException {
        int nullCount = 0;
        for (int i = 0; i < rowCount; i++) {
            if (!Objects.equals(bulkValues[i], nullValue)) {
                writeBytes(bulkValues[i]);
                dlEncoder.writeInt(1);
            } else {
                nullCount++;
                dlEncoder.writeInt(0);
            }
        }
        return new WriteResult(rowCount);
    }

    @Override
    public WriteResult writeBulkFilterNulls(Binary[] bulkValues, Binary nullValue, int nonNullLeafCount) {
        int nullCount = 0;
        IntBuffer nullOffsets = IntBuffer.allocate(4);
        for (int i = 0; i < nonNullLeafCount; i++) {
            if (!Objects.equals(bulkValues[i], nullValue)) {
                writeBytes(bulkValues[i]);
            } else {
                nullOffsets = Helpers.ensureCapacity(nullOffsets);
                nullOffsets.put(i);
                nullCount++;
            }
        }
        return new WriteResult(nonNullLeafCount, nullOffsets);
    }




}
