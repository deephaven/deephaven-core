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
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * Used as a base class of transfer objects for types like strings or big integers that need specialized encoding, and
 * thus we need to enforce page size limits while writing.
 */
abstract class EncodedTransfer<T> implements TransferObject<Binary[]> {
    private final ColumnSource<?> columnSource;
    private final ChunkSource.GetContext context;
    private final Binary[] buffer;

    private ObjectChunk<T, Values> chunk;

    /**
     * Number of objects buffered
     */
    private int bufferedDataCount;

    /**
     * The target size of data to be stored in a single page. This is not a strictly enforced "maximum" page size.
     */
    private final int targetPageSize;

    /**
     * Index of next object from the chunk to be buffered
     */
    private int currentChunkIdx;

    /**
     * Encoded value which takes us beyond the page size limit. We cache it to avoid re-encoding.
     */
    @Nullable
    private Binary cachedEncodedValue;

    public EncodedTransfer(
            @NotNull final ColumnSource<?> columnSource,
            final int maxValuesPerPage,
            final int targetPageSize) {
        this.columnSource = columnSource;
        this.buffer = new Binary[maxValuesPerPage];
        context = this.columnSource.makeGetContext(maxValuesPerPage);
        this.targetPageSize = targetPageSize;
        bufferedDataCount = 0;
        cachedEncodedValue = null;
    }

    @Override
    final public void fetchData(@NotNull final RowSequence rs) {
        // noinspection unchecked
        chunk = (ObjectChunk<T, Values>) columnSource.getChunk(context, rs);
        currentChunkIdx = 0;
        bufferedDataCount = 0;
    }

    @Override
    final public int transferAllToBuffer() {
        // Assuming this method is called after fetchData() and that the buffer is empty.
        Assert.neqNull(chunk, "chunk");
        Assert.eqZero(currentChunkIdx, "currentChunkIdx");
        Assert.eqZero(bufferedDataCount, "bufferedDataCount");
        int chunkSize = chunk.size();
        while (currentChunkIdx < chunkSize) {
            final T value = chunk.get(currentChunkIdx++);
            buffer[bufferedDataCount++] = value == null ? null : encodeToBinary(value);
        }
        chunk = null;
        return bufferedDataCount;
    }

    @Override
    final public int transferOnePageToBuffer() {
        if (!hasMoreDataToBuffer()) {
            return 0;
        }
        if (bufferedDataCount != 0) {
            // Clear any old buffered data
            Arrays.fill(buffer, 0, bufferedDataCount, null);
            bufferedDataCount = 0;
        }
        int bufferedDataSize = 0;
        int chunkSize = chunk.size();
        while (currentChunkIdx < chunkSize) {
            final T value = chunk.get(currentChunkIdx);
            if (value == null) {
                currentChunkIdx++;
                buffer[bufferedDataCount++] = null;
                continue;
            }
            Binary binaryEncodedValue;
            if (cachedEncodedValue == null) {
                binaryEncodedValue = encodeToBinary(value);
            } else {
                binaryEncodedValue = cachedEncodedValue;
                cachedEncodedValue = null;
            }

            // Always buffer the first element, even if it exceeds the target page size.
            if (bufferedDataSize != 0 && bufferedDataSize + binaryEncodedValue.length() > targetPageSize) {
                cachedEncodedValue = binaryEncodedValue;
                break;
            }
            currentChunkIdx++;
            buffer[bufferedDataCount++] = binaryEncodedValue;
            bufferedDataSize += binaryEncodedValue.length();
        }
        if (currentChunkIdx == chunk.size()) {
            chunk = null;
        }
        return bufferedDataCount;
    }

    abstract Binary encodeToBinary(T value);

    @Override
    final public boolean hasMoreDataToBuffer() {
        return ((chunk != null) && (currentChunkIdx < chunk.size()));
    }

    @Override
    final public Binary[] getBuffer() {
        return buffer;
    }

    @Override
    final public void close() {
        context.close();
    }
}
