/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit PlainIntChunkedWriter and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.parquet.base;

import io.deephaven.parquet.base.util.Helpers;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.*;


/**
 * Plain encoding except for booleans
 */
public class PlainFloatChunkedWriter extends AbstractBulkValuesWriter<FloatBuffer, Float> {
    private final ByteBufferAllocator allocator;
    private final int originalLimit;

    private FloatBuffer targetBuffer;
    private ByteBuffer innerBuffer;

    PlainFloatChunkedWriter(int pageSize, ByteBufferAllocator allocator) {
        innerBuffer = allocator.allocate(pageSize);
        innerBuffer.order(ByteOrder.LITTLE_ENDIAN);
        originalLimit = innerBuffer.limit();
        this.allocator = allocator;
        targetBuffer = innerBuffer.asFloatBuffer();
        targetBuffer.mark();
        innerBuffer.mark();
    }


    @Override
    public final void writeFloat(float v) {
        targetBuffer.put(v);
    }

    @Override
    public long getBufferedSize() {
        return targetBuffer.remaining() * Float.BYTES;
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
    public void writeBulk(FloatBuffer bulkValues, int rowCount) {
        targetBuffer.put(bulkValues);
    }

    @Override
    public WriteResult writeBulkFilterNulls(FloatBuffer bulkValues, Float nullValue, RunLengthBitPackingHybridEncoder dlEncoder, int rowCount) throws IOException {
        float nullFloat = nullValue;
        int nullCount = 0;
        while (bulkValues.hasRemaining()) {
            float next = bulkValues.get();
            if (next != nullFloat) {
                writeFloat(next);
                dlEncoder.writeInt(1);
            } else {
                nullCount++;
                dlEncoder.writeInt(0);
            }
        }
        return new WriteResult(rowCount);
    }

    @Override
    public WriteResult writeBulkFilterNulls(FloatBuffer bulkValues, Float nullValue, int rowCount) {
        float nullFloat = nullValue;
        int nullCount = 0;
        int i = 0;
        IntBuffer nullOffsets = IntBuffer.allocate(4);
        while (bulkValues.hasRemaining()) {
            float next = bulkValues.get();
            if (next != nullFloat) {
                writeFloat(next);
            } else {
                nullOffsets = Helpers.ensureCapacity(nullOffsets);
                nullOffsets.put(i);
                nullCount++;
            }
            i++;
        }
        return new WriteResult(rowCount, nullOffsets);
    }



}
