/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

/**
 * A transfer object base class for primitive types that can be cast to int without loss of precision.
 */
class IntArrayTransfer implements TransferObject<IntBuffer> {
    protected ObjectChunk<int[], Values> chunk;
    private final ColumnSource<?> columnSource;
    private final RowSequence.Iterator tableRowSetIt;
    private final ChunkSource.GetContext context;
    private final int maxValuesPerPage;
    private final IntBuffer buffer;
    private IntBuffer arrayLengths;
    private int currentChunkIdx;

    IntArrayTransfer(@NotNull final  ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet, final int targetPageSize) {
        this.columnSource = columnSource;
        this.tableRowSetIt = tableRowSet.getRowSequenceIterator();
        this.maxValuesPerPage = targetPageSize / Integer.BYTES;
        Assert.gtZero(maxValuesPerPage, "maxValuesPerPage");
        this.context = columnSource.makeGetContext(maxValuesPerPage);
        this.buffer = IntBuffer.allocate(maxValuesPerPage);
        this.arrayLengths = IntBuffer.allocate(maxValuesPerPage);
        this.currentChunkIdx = 0;
    }

    @Override
    final public int transferOnePageToBuffer() {
        if (!hasMoreDataToBuffer()) {
            return 0;
        }
        if (buffer.limit() != buffer.capacity()) {
            // Clear any old buffered data
            buffer.clear();
            arrayLengths.clear();
        }

        boolean bufferFull = false;
        do {
            if (chunk == null) {
                // Fetch a chunk of data from the table
                final RowSequence rs = tableRowSetIt.getNextRowSequenceWithLength(maxValuesPerPage);
                // noinspection unchecked
                chunk = (ObjectChunk<int[], Values>) columnSource.getChunk(context, rs);
            }

            final int chunkSize = chunk.size();
            while (currentChunkIdx < chunkSize) {
                final int[] arrayData = chunk.get(currentChunkIdx);
                if (arrayData == null) {
                    currentChunkIdx++;
                    // TODO Do we need to add anything to buffer?
                    arrayLengths.put(QueryConstants.NULL_INT);
                    continue;
                }
                int numValuesBuffered = buffer.position();
                // Always copy the first vector
                if (numValuesBuffered != 0 && numValuesBuffered + arrayData.length > maxValuesPerPage) {
                    bufferFull = true;
                    break;
                }
                copyArrayDataIntoBuffer(arrayData);
                arrayLengths.put(arrayData.length);
                currentChunkIdx++;
            }
            if (currentChunkIdx == chunk.size()) {
                chunk = null;
            }
        } while(!bufferFull && tableRowSetIt.hasMore());
        buffer.flip();
        arrayLengths.flip();
        return buffer.limit();
    }

    /**
     * Copy entire array into buffer because our reading code expects all elements from a single array or a vector
     * to be on the same page (refer classes ToVectorPage and ToArrayPage for more details).
     */
     private void copyArrayDataIntoBuffer(int[] data) {
        buffer.put(data);
     }

    @Override
    final public boolean hasMoreDataToBuffer() {
        // More unread data present either the table or from the chunk
        return tableRowSetIt.hasMore() || chunk != null;
    }

    @Override
    final public IntBuffer getBuffer() {
        return buffer;
    }

    @Override
    final public IntBuffer getRepeatCount() {
        return arrayLengths;
    }

    @Override
    final public void close() {
        context.close();
        tableRowSetIt.close();
    }
}
