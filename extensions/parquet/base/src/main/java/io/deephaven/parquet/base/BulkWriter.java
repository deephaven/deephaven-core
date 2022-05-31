package io.deephaven.parquet.base;

import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

/**
 * Provides the option to write values of specific type in bulk The concrete type of the bulkValue object depends in the
 * specific implementation
 */
public interface BulkWriter<T, L> {
    void writeBulk(T bulkValues, int rowCount);

    class WriteResult {
        final int valueCount;
        final IntBuffer nullOffsets;

        WriteResult(int valueCount) {
            this(valueCount, null);
        }

        public WriteResult(int valueCount, IntBuffer nullOffsets) {
            this.valueCount = valueCount;

            this.nullOffsets = nullOffsets;
        }
    }

    WriteResult writeBulkFilterNulls(T bulkValues, L nullValue, RunLengthBitPackingHybridEncoder dlEncoder,
            int rowCount) throws IOException;

    int writeBulkVector(T bulkValues, IntBuffer repeatCount, RunLengthBitPackingHybridEncoder rlEncoder,
            RunLengthBitPackingHybridEncoder dlEncoder, int nonNullValueCount, L nullValue) throws IOException;

    WriteResult writeBulkFilterNulls(T bulkValues, L nullValue, int rowCount);

    void reset();

    ByteBuffer getByteBufferView() throws IOException;

}
