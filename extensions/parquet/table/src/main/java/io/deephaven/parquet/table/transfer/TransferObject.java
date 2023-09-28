/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.CodecLookup;
import io.deephaven.engine.util.BigDecimalUtils;
import io.deephaven.parquet.table.*;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.codec.ObjectCodec;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

/**
 * Classes that implement this interface are responsible for converting data from individual DH columns into buffers
 * to be written out to the Parquet file.
 *
 * @param <B>
 */
public interface TransferObject<B> extends SafeCloseable {
    static <DATA_TYPE> TransferObject<?> create(
            @NotNull final Map<String, Map<ParquetCacheTags, Object>> computedCache,
            @NotNull final RowSet tableRowSet,
            @NotNull final ColumnSource<DATA_TYPE> columnSource,
            @NotNull final ColumnDefinition<DATA_TYPE> columnDefinition,
            final int maxValuesPerPage,
            @NotNull final Class<DATA_TYPE> columnType,
            @NotNull final ParquetInstructions instructions) {
        if (int.class.equals(columnType)) {
            return IntTransfer.create(columnSource, maxValuesPerPage);
        } else if (long.class.equals(columnType)) {
            return LongTransfer.create(columnSource, maxValuesPerPage);
        } else if (double.class.equals(columnType)) {
            return DoubleTransfer.create(columnSource, maxValuesPerPage);
        } else if (float.class.equals(columnType)) {
            return FloatTransfer.create(columnSource, maxValuesPerPage);
        } else if (Boolean.class.equals(columnType)) {
            return BooleanTransfer.create(columnSource, maxValuesPerPage);
        } else if (short.class.equals(columnType)) {
            return new ShortTransfer(columnSource, maxValuesPerPage);
        } else if (char.class.equals(columnType)) {
            return new CharTransfer(columnSource, maxValuesPerPage);
        } else if (byte.class.equals(columnType)) {
            return new ByteTransfer(columnSource, maxValuesPerPage);
        } else if (String.class.equals(columnType)) {
            return new StringTransfer(columnSource, maxValuesPerPage, instructions.getTargetPageSize());
        }

        // If there's an explicit codec, we should disregard the defaults for these CodecLookup#lookup() will properly
        // select the codec assigned by the instructions so we only need to check and redirect once.
        if (!CodecLookup.explicitCodecPresent(instructions.getCodecName(columnDefinition.getName()))) {
            if (BigDecimal.class.equals(columnType)) {
                // noinspection unchecked
                final ColumnSource<BigDecimal> bigDecimalColumnSource = (ColumnSource<BigDecimal>) columnSource;
                final BigDecimalUtils.PrecisionAndScale precisionAndScale = TypeInfos.getPrecisionAndScale(
                        computedCache, columnDefinition.getName(), tableRowSet, () -> bigDecimalColumnSource);
                final ObjectCodec<BigDecimal> codec = new BigDecimalParquetBytesCodec(
                        precisionAndScale.precision, precisionAndScale.scale, -1);
                return new CodecTransfer<>(bigDecimalColumnSource, codec, maxValuesPerPage,
                        instructions.getTargetPageSize());
            } else if (BigInteger.class.equals(columnType)) {
                return new CodecTransfer<>(columnSource, new BigIntegerParquetBytesCodec(-1),
                        maxValuesPerPage, instructions.getTargetPageSize());
            }
        }

        final ObjectCodec<? super DATA_TYPE> codec = CodecLookup.lookup(columnDefinition, instructions);
        return new CodecTransfer<>(columnSource, codec, maxValuesPerPage, instructions.getTargetPageSize());
    }

    /**
     * Fetch all data corresponding to the provided row sequence.
     */
    void fetchData(@NotNull RowSequence rs);

    /**
     * Transfer all the fetched data into an internal buffer, which can then be accessed using
     * {@link TransferObject#getBuffer()}. This method should only be called after
     * {@link TransferObject#fetchData(RowSequence)}}. This method should be used when writing unpaginated data, and
     * should not be interleaved with calls to {@link TransferObject#transferOnePageToBuffer()}. Note that this
     * method can lead to out-of-memory error for variable-width types (e.g. strings) if the fetched data is too big
     * to fit in the available heap.
     *
     * @return The number of fetched data entries copied into the buffer.
     */
    int transferAllToBuffer();

    /**
     * Transfer one page size worth of fetched data into an internal buffer, which can then be accessed using
     * {@link TransferObject#getBuffer()}. The target page size is passed in the constructor. The method should only
     * be called after {@link TransferObject#fetchData(RowSequence)}}. This method should be used when writing
     * paginated data, and should not be interleaved with calls to {@link TransferObject#transferAllToBuffer()}.
     *
     * @return The number of fetched data entries copied into the buffer. This can be different from the total
     * number of entries fetched in case of variable-width types (e.g. strings) when used with additional
     * page size limits while copying.
     */
    int transferOnePageToBuffer();

    /**
     * Check if there is any fetched data which can be copied into buffer
     */
    boolean hasMoreDataToBuffer();

    /**
     * Get the buffer suitable for writing to a Parquet file
     *
     * @return the buffer
     */
    B getBuffer();
}
