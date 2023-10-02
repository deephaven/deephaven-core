/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.parquet.base.BulkWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

final class IntArrayTransfer extends PrimitiveArrayAndVectorTransfer<int[], int[], IntBuffer> {
    BulkWriter writer;
    Statistics<?> stats;
    IntArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                     final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize / Integer.BYTES, targetPageSize,
                IntBuffer.allocate(1));
    }

    @Override
    public void setWriter(final BulkWriter writer) {
        this.writer = writer;
    }
    @Override
    public void setStats(final Statistics<?> stats) {
        this.stats = stats;
    }

    @Override
    void encodeDataForBuffering(final int @NotNull [] data, @NotNull final EncodedData<int[]> encodedData) {
        encodedData.fillRepeated(data, data.length * Integer.BYTES, data.length);
    }

    @Override
    int getNumBytesBuffered() {
        return (buffer.position() + repeatCounts.position()) * Integer.BYTES;
    }

    @Override
    void resizeBuffer(final int length) {
//        buffer = IntBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(final int @NotNull [] data) {
//        buffer.put(data);
        writer.clearNullOffsets();
        writer.ensureCapacityFor(data.length);
        for (int i = 0; i < data.length; i++) {
//            buffer.put(data[i]);
            writer.writeValue(i, data[i], stats);
        }
    }
}
