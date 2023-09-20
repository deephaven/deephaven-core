/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class BooleanTransfer implements TransferObject<ByteBuffer> {

    private final ColumnSource<?> columnSource;
    private final ChunkSource.GetContext context;
    private ByteChunk<? extends Values> chunk;
    private final ByteBuffer buffer;

    public BooleanTransfer(
            @NotNull ColumnSource<?> columnSource,
            int targetSize) {
        this.columnSource = columnSource;
        this.buffer = ByteBuffer.allocate(targetSize);
        this.context = columnSource.makeGetContext(targetSize);
    }

    @Override
    public void fetchData(@NotNull final RowSequence rs) {
        chunk = columnSource.getChunk(context, rs).asByteChunk();
    }

    @Override
    public int transferAllToBuffer() {
        return transferOnePageToBuffer();
    }

    @Override
    public int transferOnePageToBuffer() {
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

    private void copyAllFromChunkToBuffer() {
        for (int chunkIdx = 0; chunkIdx < chunk.size(); ++chunkIdx) {
            buffer.put(chunk.get(chunkIdx));
        }
    }

    @Override
    public boolean hasMoreDataToBuffer() {
        return chunk != null;
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public void close() {
        context.close();
    }

    public <T extends Comparable<T>> void updateStatistics(@NotNull final Statistics<T> stats) {}
}
