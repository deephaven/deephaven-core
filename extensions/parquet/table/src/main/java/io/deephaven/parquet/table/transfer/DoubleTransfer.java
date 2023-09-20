/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit IntTransfer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.QueryConstants;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

import java.nio.Buffer;
import java.nio.DoubleBuffer;

public class DoubleTransfer implements TransferObject<DoubleBuffer> {

    private final ColumnSource<?> columnSource;
    private final ChunkSource.GetContext context;
    private final DoubleBuffer buffer;
    private DoubleChunk<? extends Values> chunk;
    private double minValue = QueryConstants.NULL_DOUBLE;
    private double maxValue = QueryConstants.NULL_DOUBLE;

    public DoubleTransfer(
            @NotNull final ColumnSource<?> columnSource,
            final int targetSize) {
        this.columnSource = columnSource;
        this.buffer = DoubleBuffer.allocate(targetSize);
        context = columnSource.makeGetContext(targetSize);
    }

    @Override
    final public void fetchData(@NotNull final RowSequence rs) {
        chunk = columnSource.getChunk(context, rs).asDoubleChunk();
    }

    @Override
    final public int transferAllToBuffer() {
        return transferOnePageToBuffer();
    }

    @Override
    final public int transferOnePageToBuffer() {
        if (!hasMoreDataToBuffer()) {
            return 0;
        }
        buffer.clear();
        // Assuming that all the fetched data will fit in one page. This is because page count is accurately
        // calculated for non variable-width types. Check ParquetTableWriter.getTargetRowsPerPage for more details.
        copyAllFromChunkToBuffer();
        buffer.flip();
        int ret = chunk.size();
        chunk = null;
        return ret;
    }

    /**
     * Helper method to copy all data from {@code this.chunk} to {@code this.buffer}. The buffer should be cleared
     * before calling this method and is positioned for a {@link Buffer#flip()} after the call.
     */
    private void copyAllFromChunkToBuffer() {
        for (int chunkIdx = 0; chunkIdx < chunk.size(); ++chunkIdx) {
            double value = chunk.get(chunkIdx);
            if (value != QueryConstants.NULL_DOUBLE) {
                if (minValue == QueryConstants.NULL_DOUBLE) {
                    minValue = maxValue = value;
                } else if (value < minValue) {
                    minValue = value;
                } else if (value > maxValue) {
                    maxValue = value;
                }
            }
            buffer.put(value);
        }
    }

    @Override
    final public boolean hasMoreDataToBuffer() {
        return (chunk != null);
    }

    @Override
    final public DoubleBuffer getBuffer() {
        return buffer;
    }

    @Override
    final public void close() {
        context.close();
    }

    @Override
    public <T extends Comparable<T>> void updateStatistics(@NotNull final Statistics<T> stats) {
        if (minValue != QueryConstants.NULL_DOUBLE) {
            ((DoubleStatistics) stats).setMinMax(minValue, maxValue);
        }
    }
}
