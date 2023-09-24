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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * TODO Add comments
 */
abstract class VariableWidthTransfer<T, E, B> implements TransferObject<B> {
    private ObjectChunk<T, Values> chunk;
    protected final B buffer;
    private final ColumnSource<?> columnSource;
    private final RowSequence.Iterator tableRowSetIt;
    private final ChunkSource.GetContext context;
    private final int maxValuesPerPage;
    private final int targetPageSize;
    private int currentChunkIdx;
    /**
     * Cached value which took us beyond the page size limit. We cache it to avoid re-encoding.
     */
    @Nullable
    private EncodedData cachedValue;

    VariableWidthTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int maxValuesPerPage, final int targetPageSize, @NotNull final B buffer) {
        this.columnSource = columnSource;
        this.tableRowSetIt = tableRowSet.getRowSequenceIterator();
        this.targetPageSize = targetPageSize;
        Assert.gtZero(maxValuesPerPage, "targetPageSize");
        this.maxValuesPerPage = maxValuesPerPage;
        Assert.gtZero(maxValuesPerPage, "maxValuesPerPage");
        this.context = columnSource.makeGetContext(maxValuesPerPage);
        this.currentChunkIdx = 0;
        this.buffer = buffer;
    }

    @Override
    public final B getBuffer() {
        return buffer;
    }

    final public boolean hasMoreDataToBuffer() {
        // Unread data present either the table or in the chunk
        return tableRowSetIt.hasMore() || chunk != null;
    }

    // TODO Add comments
    class EncodedData {
        E data;
        int numBytes;

        EncodedData(@NotNull final E data, final int numBytes) {
            this.data = data;
            this.numBytes = numBytes;
        }
    }

    // TODO Add comments about what to be done before and after calling this method
    final void transferOnePageToBufferHelper() {
        if (!hasMoreDataToBuffer()) {
            return;
        }
        boolean stop = false;
        do {
            if (chunk == null) {
                // Fetch a chunk of data from the table
                final RowSequence rs = tableRowSetIt.getNextRowSequenceWithLength(maxValuesPerPage);
                // noinspection unchecked
                chunk = (ObjectChunk<T, Values>) columnSource.getChunk(context, rs);
                currentChunkIdx = 0;
            }
            final int chunkSize = chunk.size();
            while (currentChunkIdx < chunkSize) {
                final T data = chunk.get(currentChunkIdx);
                if (data == null) {
                    if (!addNullToBuffer()) {
                        stop = true;
                        break;
                    }
                    currentChunkIdx++;
                    continue;
                }
                EncodedData nextEntry;
                if (cachedValue == null) {
                    nextEntry = encodeDataForBuffering(data);
                } else {
                    nextEntry = cachedValue;
                    cachedValue = null;
                }
                int numBytesBuffered = getNumBytesBuffered();
                // Always copy the first entry
                if ((numBytesBuffered != 0 && numBytesBuffered + nextEntry.numBytes > targetPageSize) ||
                        !addEncodedDataToBuffer(nextEntry)) {
                    stop = true;
                    cachedValue = nextEntry;
                    break;
                }
                currentChunkIdx++;
            }
            if (currentChunkIdx == chunk.size()) {
                chunk = null;
            }
        } while (!stop && tableRowSetIt.hasMore());
    }

    abstract boolean addNullToBuffer();

    abstract boolean addEncodedDataToBuffer(@NotNull final EncodedData encodedData); // TODO Use better names

    abstract int getNumBytesBuffered();

    abstract EncodedData encodeDataForBuffering(@NotNull final T data);

    final public void close() {
        context.close();
        tableRowSetIt.close();
    }
}
