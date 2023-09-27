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
 * Base type for all transfer objects where we don't know the size of the data before actually reading the data. This
 * includes strings, codec encoded objects, arrays and vectors. This class provides methods to iterate over the column,
 * fetch the data, encode it and adds it to buffer while enforcing page size constraints and handling overflow.
 *
 * @param <T> The type of the data in the column
 * @param <E> The type of the encoded data to be added to the buffer
 * @param <B> The type of the buffer to be written out to the Parquet file
 */
abstract class VariableWidthTransfer<T, E, B> implements TransferObject<B> {
    private ObjectChunk<T, Values> chunk;
    protected B buffer; // Not final because we might need to resize it in case of overflow
    private final ColumnSource<?> columnSource;
    private final RowSequence.Iterator tableRowSetIt;
    private final ChunkSource.GetContext context;
    private final int maxValuesPerPage;
    private final int targetPageSize;
    private int currentChunkIdx;
    /**
     * The reusable field used to store the output from {@link #encodeDataForBuffering(Object)}.
     */
    final EncodedData encodedData;
    /**
     * The value which took us beyond the page size limit. We cache it to avoid re-encoding.
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
        this.encodedData = new EncodedData();
        cachedValue = null;
    }

    @Override
    public final B getBuffer() {
        return buffer;
    }

    @Override
    final public boolean hasMoreDataToBuffer() {
        // Unread data present either in the table or in the chunk
        return tableRowSetIt.hasMore() || chunk != null;
    }

    /**
     * We pull each row from the column and encode it before adding it to the buffer. This class is ued to store the
     * encoded data, the number of values encoded (which can be more than 1 in case of array/vector columns) and the
     * number of bytes encoded.
     */
    final class EncodedData {
        // TODO Where should I place this class in this file
        E encodedValues;
        int numValues;
        int numBytes;

        /**
         * Construct an empty object to be filled later using {@link #fill(Object, int, int)} method
         */
        EncodedData() {}

        /**
         * Used for non vector/array types where we have a single value in each row
         */
        void fill(@NotNull final E encodedValues, final int numBytes) {
            this.encodedValues = encodedValues;
            this.numBytes = numBytes;
            this.numValues = 1;
        }

        /**
         * Used for vector/array types where we can have a more than one value in each row
         */
        void fill(@NotNull final E data, final int numValues, final int numBytes) {
            fill(data, numBytes);
            this.numValues = numValues;
        }
    }


    /**
     * Helper method which transfers one page size worth of data from column source to buffer. The method assumes we
     * have more data to buffer, so should be called if {@link #hasMoreDataToBuffer()} returns true.
     */
    final void transferOnePageToBufferHelper() {
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
                    encodeDataForBuffering(data);
                    nextEntry = encodedData;
                } else {
                    // We avoid re-encoding by using the cached value
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

    /**
     * This method is called when we encounter a null in the column.
     *
     * @return Whether we succeeded in adding the null to the buffer. A false value indicates overflow of underlying
     *         buffer and that we should stop reading more data from the column and return the buffer as-is.
     */
    abstract boolean addNullToBuffer();

    /**
     * This method is called when we fetch a non-null row entry from the column and need to encode it before adding it
     * to the buffer. This method assumes the data is non-null. The encoded data is stored in the variable
     * {@link #encodedData}
     *
     * @param data The fetched value to be encoded, can be an array/vector or a single value
     */
    abstract void encodeDataForBuffering(@NotNull final T data);

    /**
     * This method is called for adding the encoded data to the buffer.
     *
     * @return Whether we succeeded in adding the data to the buffer. A false value indicates overflow of underlying
     *         buffer and that we should stop reading more data from the column and return the buffer as-is.
     */
    abstract boolean addEncodedDataToBuffer(@NotNull final EncodedData data);

    /**
     * The total number of encoded bytes present in the buffer. Useful for adding page size constraints.
     */
    abstract int getNumBytesBuffered();

    final public void close() {
        context.close();
        tableRowSetIt.close();
    }
}
