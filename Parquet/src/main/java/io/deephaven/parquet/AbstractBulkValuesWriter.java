package io.deephaven.parquet;

import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.IntBuffer;

public abstract class AbstractBulkValuesWriter<T, L> extends ValuesWriter
    implements BulkWriter<T, L> {

    @Override
    public int writeBulkVector(T bulkValues, IntBuffer repeatCount,
        RunLengthBitPackingHybridEncoder rlEncoder,
        RunLengthBitPackingHybridEncoder dlEncoder, int nonNullValueCount, L nullValue)
        throws IOException {
        IntBuffer nullsOffsets =
            writeBulkFilterNulls(bulkValues, nullValue, nonNullValueCount).nullOffsets;
        return applyDlAndRl(repeatCount, rlEncoder, dlEncoder, nullsOffsets);
    }

    @NotNull
    int applyDlAndRl(IntBuffer repeatCount, RunLengthBitPackingHybridEncoder rlEncoder,
        RunLengthBitPackingHybridEncoder dlEncoder,
        IntBuffer nullsOffsets) throws IOException {
        int valueCount = 0;
        int leafCount = 0;

        nullsOffsets.flip();
        int nextNullOffset = nullsOffsets.hasRemaining() ? nullsOffsets.get() : Integer.MAX_VALUE;
        /*
         * DL: 0 - null row DL: 1 - empty array DL: 2 - null element DL: 3 - non-null array element
         */

        while (repeatCount.hasRemaining()) {
            int length = repeatCount.get();
            if (length != Integer.MIN_VALUE) {
                if (length == 0) {
                    dlEncoder.writeInt(1);
                } else {
                    if (leafCount == nextNullOffset) {
                        nextNullOffset =
                            nullsOffsets.hasRemaining() ? nullsOffsets.get() : Integer.MAX_VALUE;
                        dlEncoder.writeInt(2);
                    } else {
                        dlEncoder.writeInt(3);
                    }
                    leafCount++;
                }
                valueCount++;
                rlEncoder.writeInt(0);
                for (short i = 1; i < length; i++) {
                    if (leafCount++ == nextNullOffset) {
                        nextNullOffset =
                            nullsOffsets.hasRemaining() ? nullsOffsets.get() : Integer.MAX_VALUE;
                        dlEncoder.writeInt(2);
                    } else {
                        dlEncoder.writeInt(3);
                    }
                    rlEncoder.writeInt(1);
                    valueCount++;
                }
            } else {
                valueCount++;
                dlEncoder.writeInt(0);
                rlEncoder.writeInt(0);
            }
        }
        return valueCount;
    }
}
