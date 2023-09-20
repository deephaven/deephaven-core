/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.QueryConstants;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

import java.nio.Buffer;
import java.nio.IntBuffer;

public class IntTransfer implements TransferObject<IntBuffer> {

    private final ColumnSource<?> columnSource;
    private final ChunkSource.GetContext context;
    private final IntBuffer buffer;
    private IntChunk<? extends Values> chunk;
    private int minValue = QueryConstants.NULL_INT;
    private int maxValue = QueryConstants.NULL_INT;

    public IntTransfer(
            @NotNull final ColumnSource<?> columnSource,
            final int targetSize) {
        this.columnSource = columnSource;
        this.buffer = IntBuffer.allocate(targetSize);
        context = columnSource.makeGetContext(targetSize);
    }

    @Override
    final public void fetchData(@NotNull final RowSequence rs) {
        chunk = columnSource.getChunk(context, rs).asIntChunk();
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
            int value = chunk.get(chunkIdx);
            if (value != QueryConstants.NULL_INT) {
                if (minValue == QueryConstants.NULL_INT) {
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
    final public IntBuffer getBuffer() {
        return buffer;
    }

    @Override
    final public void close() {
        context.close();
    }

    @Override
    public <T extends Comparable<T>> void updateStatistics(@NotNull final Statistics<T> stats) {
        if (minValue != QueryConstants.NULL_INT) {
            ((IntStatistics) stats).setMinMax(minValue, maxValue);
        }
    }
}
