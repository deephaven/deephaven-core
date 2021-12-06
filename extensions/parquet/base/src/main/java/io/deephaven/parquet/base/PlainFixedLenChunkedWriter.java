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
import java.nio.IntBuffer;
import java.util.Arrays;


/**
 * Plain encoding except for booleans
 */
public class PlainFixedLenChunkedWriter extends AbstractBulkValuesWriter<ByteBuffer, Binary> {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.parquet.column.values.plain.PlainValuesWriter.class);
    private final ByteBufferAllocator allocator;
    private final int originalLimit;
    private final int fixedLength;
    private final int initialPosition;

    ByteBuffer innerBuffer;

    public PlainFixedLenChunkedWriter(int pageSize, int fixedLength, ByteBufferAllocator allocator) {
        innerBuffer = allocator.allocate(pageSize);
        this.allocator = allocator;
        this.initialPosition = innerBuffer.position();
        originalLimit = innerBuffer.limit();
        this.fixedLength = fixedLength;
    }


    @Override
    public final void writeBytes(Binary v) {
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
        innerBuffer.position(initialPosition);
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
    public void writeBulk(ByteBuffer bulkValues, int rowCount) {
        innerBuffer.put(bulkValues);
    }

    @Override
    public WriteResult writeBulkFilterNulls(ByteBuffer bulkValues, Binary nullValue, RunLengthBitPackingHybridEncoder dlEncoder, int rowCount) throws IOException {
        byte[] nullBytes = nullValue.getBytes();
        byte[] stepBytes = new byte[fixedLength];
        int nullCount = 0;
        for (int i = bulkValues.position(); i < bulkValues.limit(); i += fixedLength) {
            bulkValues.get(stepBytes);
            if (!Arrays.equals(stepBytes, nullBytes)) {
                innerBuffer.put(bulkValues);
                dlEncoder.writeInt(1);
            } else {
                nullCount++;
                dlEncoder.writeInt(0);
            }
        }
        return new WriteResult(rowCount);
    }

    @Override
    public WriteResult writeBulkFilterNulls(ByteBuffer bulkValues, Binary nullValue, int rowCount) {
        byte[] nullBytes = nullValue.getBytes();
        byte[] stepBytes = new byte[fixedLength];
        int nullCount = 0;
        IntBuffer nullOffsets = IntBuffer.allocate(4);
        for (int i = bulkValues.position(); i < bulkValues.limit(); i += fixedLength) {
            bulkValues.get(stepBytes);
            if (!Arrays.equals(stepBytes, nullBytes)) {
                innerBuffer.put(bulkValues);
            } else {
                nullCount++;
                nullOffsets = Helpers.ensureCapacity(nullOffsets);
                nullOffsets.put(i);
            }
        }
        return new WriteResult(rowCount, nullOffsets);
    }
}
