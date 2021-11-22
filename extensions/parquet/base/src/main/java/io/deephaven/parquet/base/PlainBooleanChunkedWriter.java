/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit PlainIntChunkedWriter and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.parquet.base;

import io.deephaven.parquet.base.util.Helpers;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;


/**
 * Plain encoding except for booleans
 */
public class PlainBooleanChunkedWriter extends AbstractBulkValuesWriter<ByteBuffer, Byte> {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.parquet.column.values.plain.PlainValuesWriter.class);

    BooleanPlainValuesWriter writer;

    public PlainBooleanChunkedWriter(int pageSize, ByteBufferAllocator allocator) {
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
    public void writeBulk(ByteBuffer bulkValues, int rowCount) {
        while (bulkValues.hasRemaining()) {
            writeBoolean(bulkValues.get() == 1);
        }
    }

    @Override
    public WriteResult writeBulkFilterNulls(ByteBuffer bulkValues, Byte nullValue, RunLengthBitPackingHybridEncoder dlEncoder, int rowCount) throws IOException {
        byte nullBool = nullValue;
        int nullCount = 0;
        while (bulkValues.hasRemaining()) {
            byte next = bulkValues.get();
            if (next != nullBool) {
                writeBoolean(next == 1);
                dlEncoder.writeInt(1);
            } else {
                nullCount++;
                dlEncoder.writeInt(0);
            }
        }
        return new WriteResult(rowCount);
    }

    @Override
    public WriteResult writeBulkFilterNulls(ByteBuffer bulkValues, Byte nullValue, int rowCount) {
        byte nullBool = nullValue;
        int nullCount = 0;
        IntBuffer nullOffsets = IntBuffer.allocate(4);
        int i = 0;
        while (bulkValues.hasRemaining()) {
            byte next = bulkValues.get();
            if (next != nullBool) {
                writeBoolean(next == 1);
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
