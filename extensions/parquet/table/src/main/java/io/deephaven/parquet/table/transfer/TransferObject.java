//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.CodecLookup;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.util.BigDecimalUtils;
import io.deephaven.parquet.base.BigDecimalParquetBytesCodec;
import io.deephaven.parquet.base.BigIntegerParquetBytesCodec;
import io.deephaven.parquet.table.*;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.codec.ObjectCodec;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.IntBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;

/**
 * Classes that implement this interface are responsible for converting data from individual DH columns into buffers to
 * be written out to the Parquet file.
 *
 * @param <BUFFER_TYPE> The type of the buffer to be written out to the Parquet file
 */
public interface TransferObject<BUFFER_TYPE> extends SafeCloseable {
    static <DATA_TYPE> TransferObject<?> create(
            @NotNull final RowSet tableRowSet,
            @NotNull final ParquetInstructions instructions,
            @NotNull final Map<String, Map<ParquetCacheTags, Object>> computedCache,
            @NotNull final String columnName,
            @NotNull final ColumnSource<DATA_TYPE> columnSource) {
        final Class<DATA_TYPE> columnType = columnSource.getType();
        if (columnType == int.class) {
            return IntTransfer.create(columnSource, tableRowSet, instructions.getTargetPageSize());
        }
        if (columnType == long.class) {
            return LongTransfer.create(columnSource, tableRowSet, instructions.getTargetPageSize());
        }
        if (columnType == Instant.class) {
            // noinspection unchecked
            final ColumnSource<DATA_TYPE> longColumnSource =
                    (ColumnSource<DATA_TYPE>) ReinterpretUtils
                            .instantToLongSource((ColumnSource<Instant>) columnSource);
            return LongTransfer.create(longColumnSource, tableRowSet, instructions.getTargetPageSize());
        }
        if (columnType == double.class) {
            return DoubleTransfer.create(columnSource, tableRowSet, instructions.getTargetPageSize());
        }
        if (columnType == float.class) {
            return FloatTransfer.create(columnSource, tableRowSet, instructions.getTargetPageSize());
        }
        if (columnType == Boolean.class) {
            // noinspection unchecked
            final ColumnSource<DATA_TYPE> byteColumnSource =
                    (ColumnSource<DATA_TYPE>) ReinterpretUtils
                            .booleanToByteSource((ColumnSource<Boolean>) columnSource);
            return BooleanTransfer.create(byteColumnSource, tableRowSet, instructions.getTargetPageSize());
        }
        if (columnType == short.class) {
            return new ShortTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
        }
        if (columnType == char.class) {
            return new CharTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
        }
        if (columnType == byte.class) {
            return new ByteTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
        }
        if (columnType == String.class) {
            return new StringTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
        }
        if (CodecLookup.explicitCodecPresent(instructions.getCodecName(columnName))) {
            final ObjectCodec<? super DATA_TYPE> codec = CodecLookup.lookup(
                    columnType, instructions.getCodecName(columnName), instructions.getCodecArgs(columnName));
            return new CodecTransfer<>(columnSource, codec, tableRowSet, instructions.getTargetPageSize());
        }
        if (columnType == BigDecimal.class) {
            final BigDecimalUtils.PrecisionAndScale precisionAndScale = TypeInfos.getPrecisionAndScale(
                    computedCache, columnName, tableRowSet, () -> columnSource);
            final ObjectCodec<BigDecimal> codec = new BigDecimalParquetBytesCodec(
                    precisionAndScale.precision, precisionAndScale.scale);
            return new CodecTransfer<>(columnSource, codec, tableRowSet, instructions.getTargetPageSize());
        }
        if (columnType == BigInteger.class) {
            return new CodecTransfer<>(columnSource, new BigIntegerParquetBytesCodec(), tableRowSet,
                    instructions.getTargetPageSize());
        }
        if (columnType == LocalDate.class) {
            return new DateTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
        }
        if (columnType == LocalTime.class) {
            return new TimeTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
        }
        if (columnType == LocalDateTime.class) {
            return new LocalDateTimeTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
        }

        @Nullable
        final Class<?> componentType = columnSource.getComponentType();
        if (columnType.isArray()) {
            if (componentType == int.class) {
                return new IntArrayTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == long.class) {
                return new LongArrayTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == double.class) {
                return new DoubleArrayTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == float.class) {
                return new FloatArrayTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == Boolean.class) {
                return new BooleanArrayTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == short.class) {
                return new ShortArrayTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == char.class) {
                return new CharArrayTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == byte.class) {
                return new ByteArrayTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == String.class) {
                return new StringArrayTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == BigDecimal.class) {
                final BigDecimalUtils.PrecisionAndScale precisionAndScale = TypeInfos.getPrecisionAndScale(
                        computedCache, columnName, tableRowSet, () -> columnSource);
                final ObjectCodec<BigDecimal> codec = new BigDecimalParquetBytesCodec(
                        precisionAndScale.precision, precisionAndScale.scale);
                return new CodecArrayTransfer<>(columnSource, codec, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == BigInteger.class) {
                return new CodecArrayTransfer<>(columnSource, new BigIntegerParquetBytesCodec(),
                        tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == Instant.class) {
                return new InstantArrayTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == LocalDate.class) {
                return new DateArrayTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == LocalTime.class) {
                return new TimeArrayTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == LocalDateTime.class) {
                return new LocalDateTimeArrayTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            // TODO(deephaven-core#4612): Handle if explicit codec provided
        }
        if (Vector.class.isAssignableFrom(columnType)) {
            if (componentType == int.class) {
                return new IntVectorTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == long.class) {
                return new LongVectorTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == double.class) {
                return new DoubleVectorTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == float.class) {
                return new FloatVectorTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == Boolean.class) {
                return new BooleanVectorTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == short.class) {
                return new ShortVectorTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == char.class) {
                return new CharVectorTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == byte.class) {
                return new ByteVectorTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == String.class) {
                return new StringVectorTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == BigDecimal.class) {
                final BigDecimalUtils.PrecisionAndScale precisionAndScale = TypeInfos.getPrecisionAndScale(
                        computedCache, columnName, tableRowSet, () -> columnSource);
                final ObjectCodec<BigDecimal> codec = new BigDecimalParquetBytesCodec(
                        precisionAndScale.precision, precisionAndScale.scale);
                return new CodecVectorTransfer<>(columnSource, codec, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == BigInteger.class) {
                return new CodecVectorTransfer<>(columnSource, new BigIntegerParquetBytesCodec(),
                        tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == Instant.class) {
                return new InstantVectorTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == LocalDate.class) {
                return new DateVectorTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == LocalTime.class) {
                return new TimeVectorTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            if (componentType == LocalDateTime.class) {
                return new LocalDateTimeVectorTransfer(columnSource, tableRowSet, instructions.getTargetPageSize());
            }
            // TODO(deephaven-core#4612): Handle if explicit codec provided
        }

        // Go with the default
        final ObjectCodec<? super DATA_TYPE> codec = CodecLookup.getDefaultCodec(columnType);
        return new CodecTransfer<>(columnSource, codec, tableRowSet, instructions.getTargetPageSize());
    }

    static <DATA_TYPE> @NotNull TransferObject<IntBuffer> createDictEncodedStringTransfer(
            @NotNull final RowSet tableRowSet, @NotNull final ColumnSource<DATA_TYPE> columnSource,
            final int targetPageSize, @NotNull final StringDictionary dictionary) {
        @Nullable
        final Class<?> dataType = columnSource.getType();
        @Nullable
        final Class<?> componentType = columnSource.getComponentType();
        if (dataType == String.class) {
            return new DictEncodedStringTransfer(columnSource, tableRowSet, targetPageSize, dictionary);
        }
        if (dataType.isArray() && componentType == String.class) {
            return new DictEncodedStringArrayTransfer(columnSource, tableRowSet, targetPageSize, dictionary);
        }
        if (Vector.class.isAssignableFrom(dataType) && componentType == String.class) {
            return new DictEncodedStringVectorTransfer(columnSource, tableRowSet, targetPageSize, dictionary);
        }
        // Dictionary encoding not supported for other types
        throw new UnsupportedOperationException("Dictionary encoding not supported for type " + dataType.getName());
    }

    /**
     * Transfer one page size worth of fetched data into an internal buffer, which can then be accessed using
     * {@link TransferObject#getBuffer()}. The target page size is passed in the constructor. For dictionary encoded
     * string transfers, this method also updates the dictionary with the strings encountered.
     *
     * @return The number of fetched data entries copied into the buffer. This can be different from the total number of
     *         entries fetched in case of variable-width types (e.g. strings) when used with additional page size limits
     *         while copying.
     */
    int transferOnePageToBuffer();

    /**
     * Check if there is any more data which can be copied into buffer
     */
    boolean hasMoreDataToBuffer();

    /**
     * Get the buffer suitable for writing to a Parquet file
     *
     * @return the buffer
     */
    BUFFER_TYPE getBuffer();

    /**
     * Returns whether we encountered any null value while transferring page data to buffer. This method is only used
     * for dictionary encoded string transfer objects. This method should be called after
     * {@link #transferOnePageToBuffer()} and the state resets everytime we call {@link #transferOnePageToBuffer()}.
     */
    default boolean pageHasNull() {
        throw new UnsupportedOperationException("Only supported for dictionary encoded string transfer objects");
    }

    /**
     * Get the lengths of array/vector elements added to the buffer.
     *
     * @return the buffer with counts
     */
    default IntBuffer getRepeatCount() {
        throw new UnsupportedOperationException("Only supported for array and vector transfer objects");
    }
}
