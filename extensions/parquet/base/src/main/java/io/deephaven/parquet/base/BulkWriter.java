//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

/**
 * Provides the option to write values of specific type in bulk The concrete type of the bulkValue object depends on the
 * specific implementation
 */
interface BulkWriter<BUFFER_TYPE> {
    class WriteResult {
        final int valueCount;
        final IntBuffer nullOffsets;

        WriteResult(final int valueCount) {
            this(valueCount, null);
        }

        public WriteResult(final int valueCount, @Nullable final IntBuffer nullOffsets) {
            this.valueCount = valueCount;
            this.nullOffsets = nullOffsets;
        }
    }

    /**
     * Write a buffer's worth of values to the underlying page.
     *
     * @param bulkValues the buffer of values
     * @param rowCount the total number of rows to write.
     */
    void writeBulk(@NotNull BUFFER_TYPE bulkValues, int rowCount, @NotNull Statistics<?> statistics);

    /**
     * Write a buffer's worth of values to the underlying page. This method will find, without writing, {@code null}
     * values and record their offsets in an {@link WriteResult#nullOffsets IntBuffer} in the result. The appropriate
     * definition level will be set for null values.
     *
     * @param bulkValues the values to write
     * @param dlEncoder the encoder for definition levels
     * @param rowCount the number of rows being written
     * @param statistics the {@link Statistics} object to modify.
     * @return a {@link WriteResult} containing the statistics of the result.
     * @throws IOException if there was an error during write.
     */
    @NotNull
    WriteResult writeBulkFilterNulls(@NotNull BUFFER_TYPE bulkValues,
            @NotNull RunLengthBitPackingHybridEncoder dlEncoder,
            final int rowCount,
            @NotNull Statistics<?> statistics) throws IOException;

    /**
     * Write a buffer's worth of packed vector values to the underlying page. This method will set the proper definition
     * level and repetition values in the encoders for {@code null} values.
     *
     * @param bulkValues the packed array values
     * @param vectorSizes a buffer where each element contains the number of elements in each packed vector.
     * @param rlEncoder the repetition level encoder
     * @param dlEncoder the definition level encoder.
     * @param nonNullValueCount the total count of non-null values
     * @return the number of values actually written
     * @throws IOException if writing failed.
     */
    int writeBulkVector(@NotNull final BUFFER_TYPE bulkValues,
            @NotNull final IntBuffer vectorSizes,
            @NotNull final RunLengthBitPackingHybridEncoder rlEncoder,
            @NotNull final RunLengthBitPackingHybridEncoder dlEncoder,
            final int nonNullValueCount,
            @NotNull Statistics<?> statistics) throws IOException;

    /**
     * Write a buffer's worth of packed vector values to the underlying page, skipping null values. This method will
     * find {@code null} values and record their offsets in an {@link WriteResult#nullOffsets IntBuffer} in the result.
     *
     * @param bulkValues the packed vector values to write
     * @param rowCount the number of rows being written.
     * @return a {@link WriteResult} containing the statistics of the result.
     */
    @NotNull
    WriteResult writeBulkVectorFilterNulls(@NotNull BUFFER_TYPE bulkValues,
            final int rowCount,
            @NotNull Statistics<?> statistics);

    /**
     * Clear all internal state.
     */
    void reset();

    /**
     * Get a view of this writer {@link ByteBuffer}.
     *
     * @return a {@link ByteBuffer} containing the written data.
     *
     * @throws IOException if there is an exception reading the data.
     */
    ByteBuffer getByteBufferView() throws IOException;

}
