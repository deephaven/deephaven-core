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

    /**
     * Number of values buffered
     */
    private int bufferedValueCount;
    // TODO Use IntBuffer.position() instead of this variable

    /**
     * The lengths of arrays stored in the buffer
     */
    private IntBuffer arrayLengths;

    /**
     * Index of next object from the chunk to be buffered
     */
    private int currentChunkIdx;

    IntArrayTransfer(@NotNull final  ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet, final int targetPageSize) {
        this.columnSource = columnSource;
        this.tableRowSetIt = tableRowSet.getRowSequenceIterator();
        this.maxValuesPerPage = targetPageSize / Integer.BYTES;
        Assert.gtZero(maxValuesPerPage, "maxValuesPerPage");
        this.context = columnSource.makeGetContext(maxValuesPerPage);
//        this.buffer = new int[maxValuesPerPage];
        buffer = IntBuffer.allocate(maxValuesPerPage);
        this.bufferedValueCount = 0;
        this.arrayLengths = IntBuffer.allocate(maxValuesPerPage);
        currentChunkIdx = 0;
    }

    @Override
    final public int transferOnePageToBuffer() {
        if (!hasMoreDataToBuffer()) {
            return 0;
        }
        if (bufferedValueCount != 0) {
            // Clear any old buffered data
//            Arrays.fill(buffer, 0, bufferedValueCount, 0);
            buffer.clear();
            arrayLengths.clear();
            bufferedValueCount = 0;
        }

        do {
            if (chunk == null) {
                // Fetch a chunk of data from the table
                final RowSequence rs = tableRowSetIt.getNextRowSequenceWithLength(maxValuesPerPage);
                // noinspection unchecked
                chunk = (ObjectChunk<int[], Values>) columnSource.getChunk(context, rs);
            }

            final int chunkSize = chunk.size();
            while (currentChunkIdx < chunkSize) {
                final int[] intArray = chunk.get(currentChunkIdx);
                if (intArray == null) {
                    currentChunkIdx++;
                    // TODO Do we need to add anything to buffer?
                    arrayLengths.put(QueryConstants.NULL_INT);
                    continue;
                }
                int numValues = intArray.length;
                // Always copy the first vector
                if (bufferedValueCount != 0 && bufferedValueCount + numValues > maxValuesPerPage) {
                    break;
                }
                // TODO Copy the complete array from chunk to buffer with resizing the buffer if needed
                buffer.put(0);
                bufferedValueCount += numValues;
                arrayLengths.put(numValues);
                currentChunkIdx++;
            }
            if (currentChunkIdx == chunk.size()) {
                chunk = null;
            }
        } while(bufferedValueCount < maxValuesPerPage && tableRowSetIt.hasMore());
        return bufferedValueCount;
    }

    @Override
    final public boolean hasMoreDataToBuffer() {
        // More data can be read either from the table or from the chunk
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
