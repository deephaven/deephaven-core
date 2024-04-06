//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.transfer;

import io.deephaven.base.verify.Require;
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
 * fetch the data, encode it and add it to buffer while enforcing page size constraints and handling overflow.
 *
 * @param <COLUMN_TYPE> The type of the data in the column
 * @param <ENCODED_COLUMN_TYPE> The type of the encoded data to be added to the buffer
 * @param <BUFFER_TYPE> The type of the buffer to be written out to the Parquet file
 */
abstract class VariableWidthTransfer<COLUMN_TYPE, ENCODED_COLUMN_TYPE, BUFFER_TYPE>
        implements TransferObject<BUFFER_TYPE> {
    private ObjectChunk<COLUMN_TYPE, Values> chunk;
    private final ColumnSource<?> columnSource;
    private final RowSequence.Iterator tableRowSetIt;
    private final ChunkSource.GetContext context;
    private final int targetPageSizeInBytes;
    private int currentChunkIdx;
    /**
     * The reusable field used to store the output from {@link #encodeDataForBuffering}.
     */
    private final EncodedData<ENCODED_COLUMN_TYPE> encodedData;
    /**
     * Whether {@link #encodedData} stores the encoded value corresponding to {@link #currentChunkIdx}. This is useful
     * to cache the value which took us beyond the page size limit. We cache it to avoid re-encoding.
     */
    @Nullable
    private boolean cached;

    /**
     * The buffer to be written out to the Parquet file. This buffer is reused across pages and is resized if needed.
     */
    BUFFER_TYPE buffer;

    /**
     * This variable is used as:
     * <ul>
     * <li>The target number of elements on one page. It is a soft maximum, in that we will exceed it if a particular
     * row exceeds this target so that we can fit all the elements from a single row on the same page.
     * <li>An upper bound on the number of rows per page.
     * </ul>
     */
    final int targetElementsPerPage;

    VariableWidthTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetElementsPerPage, final int targetPageSizeInBytes, @NotNull final BUFFER_TYPE buffer) {
        this.columnSource = columnSource;
        this.tableRowSetIt = tableRowSet.getRowSequenceIterator();
        this.targetPageSizeInBytes = Require.gtZero(targetPageSizeInBytes, "targetPageSizeInBytes");
        this.targetElementsPerPage = Require.gtZero(targetElementsPerPage, "targetElementsPerPage");
        this.context =
                columnSource.makeGetContext(Math.toIntExact(Math.min(targetElementsPerPage, tableRowSet.size())));
        this.currentChunkIdx = 0;
        this.buffer = buffer;
        this.encodedData = new EncodedData<ENCODED_COLUMN_TYPE>();
        this.cached = false;
    }

    @Override
    public final BUFFER_TYPE getBuffer() {
        return buffer;
    }

    @Override
    final public boolean hasMoreDataToBuffer() {
        // Unread data present either in the table or in the chunk
        return tableRowSetIt.hasMore() || chunk != null;
    }

    /**
     * We pull each row from the column and encode it before adding it to the buffer. This class is used to store the
     * encoded data, the number of values encoded (which can be more than 1 in case of array/vector columns) and the
     * number of bytes encoded.
     */
    static final class EncodedData<E> {
        E encodedValues;
        int numValues;
        int numBytes;

        /**
         * Construct an empty object to be filled later using {@link #fillSingle} or {@link #fillRepeated} methods
         */
        EncodedData() {}

        /**
         * Used for non vector/array types where we have a single value in each row
         */
        void fillSingle(@NotNull final E encodedValues, final int numBytes) {
            fillInternal(encodedValues, numBytes, 1);
        }

        /**
         * Used for vector/array types where we can have a more than one value in each row
         */
        void fillRepeated(@NotNull final E data, final int numBytes, final int numValues) {
            fillInternal(data, numBytes, numValues);
        }

        private void fillInternal(@NotNull final E encodedValues, final int numBytes, final int numValues) {
            this.encodedValues = encodedValues;
            this.numBytes = numBytes;
            this.numValues = numValues;
        }
    }

    /**
     * Helper method which transfers one page size worth of data from column source to buffer. The method assumes we
     * have more data to buffer, so should be called if {@link #hasMoreDataToBuffer()} returns true.
     */
    final void transferOnePageToBufferHelper() {
        OUTER: do {
            if (chunk == null) {
                // Fetch a chunk of data from the table
                final RowSequence rs = tableRowSetIt.getNextRowSequenceWithLength(targetElementsPerPage);
                // noinspection unchecked
                chunk = (ObjectChunk<COLUMN_TYPE, Values>) columnSource.getChunk(context, rs);
                currentChunkIdx = 0;
            }
            final int chunkSize = chunk.size();
            while (currentChunkIdx < chunkSize) {
                final COLUMN_TYPE data = chunk.get(currentChunkIdx);
                if (data == null) {
                    if (!addNullToBuffer()) {
                        // Reattempt adding null to the buffer in the next iteration
                        break OUTER;
                    }
                    currentChunkIdx++;
                    continue;
                }
                if (!cached) {
                    encodeDataForBuffering(data, encodedData);
                }
                if (isBufferEmpty()) {
                    // Always copy the first entry
                    addEncodedDataToBuffer(encodedData, true);
                } else if (getNumBytesBuffered() + encodedData.numBytes > targetPageSizeInBytes ||
                        !addEncodedDataToBuffer(encodedData, false)) {
                    // Reattempt adding the encoded value to the buffer in the next iteration
                    cached = true;
                    break OUTER;
                }
                cached = false;
                currentChunkIdx++;
            }
            if (currentChunkIdx == chunk.size()) {
                chunk = null;
            }
        } while (tableRowSetIt.hasMore());
    }

    /**
     * This method is called when we encounter a null row.
     *
     * @return Whether we succeeded in adding the null row to the buffer. A false value indicates overflow of underlying
     *         buffer and that we should stop reading more data from the column and return the buffer as-is.
     */
    abstract boolean addNullToBuffer();

    /**
     * This method is called when we fetch a non-null row entry from the column and need to encode it before adding it
     * to the buffer. This method assumes the data is non-null. The encoded data is stored in the parameter
     * {@code encodedData}
     *
     * @param data The fetched value to be encoded, can be an array/vector or a single value
     * @param encodedData The object to be filled with the encoded data
     */
    abstract void encodeDataForBuffering(@NotNull final COLUMN_TYPE data,
            @NotNull final EncodedData<ENCODED_COLUMN_TYPE> encodedData);

    /**
     * This method is called for adding the encoded data to the buffer.
     * 
     * @param data The encoded data to be added to the buffer
     * @param force Whether we should force adding the data to the buffer even if it overflows buffer size and requires
     *        resizing
     *
     * @return Whether we succeeded in adding the data to the buffer. A false value indicates overflow of underlying
     *         buffer and that we should stop reading more data from the column and return the buffer as-is.
     */
    abstract boolean addEncodedDataToBuffer(@NotNull final EncodedData<ENCODED_COLUMN_TYPE> data, final boolean force);

    /**
     * The total number of encoded bytes corresponding to non-null values. Useful for adding page size constraints.
     */
    abstract int getNumBytesBuffered();

    /**
     * Whether the buffer is empty, i.e. it contains no null or non-null value.
     */
    abstract boolean isBufferEmpty();

    final public void close() {
        context.close();
        tableRowSetIt.close();
    }
}
